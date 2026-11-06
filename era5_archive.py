"""ERA5 reanalysis archive — free, CC-BY, via Copernicus CDS API.

Used by the recurring historical backfills (wind regime, Baja history)
under WEATHER_SOURCE=free, replacing the Open-Meteo `archive-api`
(which is itself an OM proxy of ERA5 — going to source removes the
middle-man and the Open-Meteo dependency).

Prereqs: `cdsapi` installed; `~/.cdsapirc` configured with the user's
CDS API key. The CDS queue can take minutes per request; that is fine
for cron backfills (no user is waiting).

Caches per-(lat,lon,year,month,vars) NetCDF to disk so repeated runs
within a backfill window don't re-queue. Monthly chunking keeps each
job well under the new CDS-Beta per-request cost cap (a full-year
3-variable request is rejected with HTTP 403 "cost limits exceeded").
"""
from __future__ import annotations

import calendar
import datetime
import hashlib
import json
import os
import shutil
import tempfile
import time
import zipfile

_CACHE_DIR = os.environ.get(
    "ERA5_CACHE_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)),
                 "data", "era5_cache"))

# Map of CDS request variable name -> NetCDF short name (used for .sel)
_NC_NAME = {
    "10m_u_component_of_wind": "u10",
    "10m_v_component_of_wind": "v10",
    "10m_wind_gust_since_previous_post_processing": "fg10",
    "significant_height_of_combined_wind_waves_and_swell": "swh",
    "mean_wave_period": "mwp",
    "mean_wave_direction": "mwd",
}


def _point_area(lat: float, lon: float) -> list:
    return [lat + 0.5, lon - 0.5, lat - 0.5, lon + 0.5]


def _cache_path(area: list, year: int, month: int,
                variables: tuple, dataset: str) -> str:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    box = ",".join(f"{v:.3f}" for v in area)
    h = hashlib.sha1(
        f"{dataset}|{box}|{year}|"
        f"{','.join(sorted(variables))}".encode()).hexdigest()[:10]
    return os.path.join(_CACHE_DIR,
                        f"era5_{year}_{month:02d}_{h}.nc")


# ── Non-blocking CDS requests ───────────────────────────────────────────────
# Measured 2026-10-26 / 11-02: one CDS wave request queued 2 h, so the weekly
# job (10 requests, one after another) hit its 3 h cap and published nothing.
# CDS also rejects requests beyond ~3 queued per account per dataset, so
# requests stay sequential. With ERA5_QUEUE_WAIT_S set: wait at most that
# long (whole process), keep the request ids in a registry, and collect them
# on the next run. Unset = blocking cdsapi retrieve (local backfills).

class Pending(Exception):
    """CDS has the request queued; collect it on a later run."""


PENDING: set[str] = set()          # cache keys still queued at CDS this run
FAILED: set[str] = set()           # cache keys CDS failed/rejected this run


def _local_registry_load() -> dict:
    p = os.path.join(_CACHE_DIR, "era5_pending.json")
    try:
        with open(p) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _local_registry_save(reg: dict) -> None:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(os.path.join(_CACHE_DIR, "era5_pending.json"), "w") as f:
        json.dump(reg, f, indent=1)


# Callers may swap these (baja_history_fetch stores the registry on its release).
registry_load = _local_registry_load
registry_save = _local_registry_save
_registry = None
_deadline = None


def _nonblocking() -> bool:
    return bool(os.environ.get("ERA5_QUEUE_WAIT_S"))


def _reg() -> dict:
    global _registry
    if _registry is None:
        _registry = registry_load() or {}
    return _registry


def clear_registry() -> None:
    """Forget collected requests — call only after the results are published."""
    reg = _reg()
    if reg:
        reg.clear()
        registry_save(reg)


def _client():
    from cdsapi.api import get_url_key_verify
    from ecmwf.datastores import Client
    url, key, _ = get_url_key_verify(None, None, None)
    return Client(url=url, key=key, progress=False)


def _request(area, year, month, variables) -> dict:
    dlast = calendar.monthrange(year, month)[1]
    return {
        "product_type": "reanalysis",
        "variable":     list(variables),
        "year":         str(year),
        "month":        [f"{month:02d}"],
        "day":          [f"{d:02d}" for d in range(1, dlast + 1)],
        "time":         [f"{h:02d}:00" for h in range(24)],
        "area":         list(area),
        "format":       "netcdf",
    }


def _remote_for(client, dataset, request, key, submit=True):
    """The live CDS request for `key`: the registered one if CDS still has
    it, else a fresh submission (registered right away)."""
    reg = _reg()
    if key in reg:
        try:
            remote = client.get_remote(reg[key]["id"])
            if remote.status not in ("failed", "rejected", "dismissed", "deleted"):
                return remote
            print(f"   [era5] {key}: queued request {remote.status}", flush=True)
        except Exception as e:                            # noqa: BLE001
            print(f"   [era5] {key}: queued request lost ({e})", flush=True)
    if not submit:
        return None
    remote = client.submit(dataset, request)
    reg[key] = {"id": remote.request_id,
                "submitted": datetime.datetime.now(datetime.timezone.utc)
                .strftime("%Y-%m-%dT%H:%M:%SZ")}
    registry_save(reg)
    return remote


def _download_nonblocking(dataset, request, target, key) -> None:
    global _deadline
    if _deadline is None:
        _deadline = time.time() + float(os.environ["ERA5_QUEUE_WAIT_S"])
    # Past the deadline something is still queued: only check requests already
    # submitted, never add another (CDS rejects extras; keep it sequential).
    remote = _remote_for(_client(), dataset, request, key,
                         submit=time.time() < _deadline)
    if remote is None:
        raise Pending(f"{key}: not submitted, waiting on an earlier request")
    sleep = 5.0
    while True:
        try:
            if remote.results_ready:                      # raises on failed/rejected
                break
        except Exception as e:                            # noqa: BLE001
            if "Maximum number queued" in str(e):         # transient; resubmit next run
                _deadline = 0.0                           # submit nothing more this run
                raise Pending(f"{key}: CDS queue full for this account") from e
            raise
        if time.time() >= _deadline:
            raise Pending(f"{key}: CDS {remote.status} since {_reg()[key]['submitted']}")
        time.sleep(sleep)
        sleep = min(sleep * 1.5, 60.0)
    remote.download(target)
    # Registry entry stays until clear_registry(): if another request is
    # still pending, the next run re-downloads this one from CDS.


def _retrieve_month(cdsapi, dataset, area, year, month,
                    variables, path) -> bool:
    """One-month CDS retrieve. Returns True on success.

    CDS-Beta packages mixed-stream variables (e.g. instantaneous u10/v10
    together with post-processed `fg10`) as a zip of per-stream NetCDFs
    regardless of `format=netcdf`. We detect that case, extract, merge
    via xarray, and write a single NetCDF at `path` so the caller's
    open_dataset stays simple."""
    import xarray as xr
    request = _request(area, year, month, variables)
    tmp_path = path + ".tmp"
    try:
        if _nonblocking():
            _download_nonblocking(dataset, request, tmp_path,
                                  os.path.basename(path))
        else:
            cdsapi.Client(quiet=True).retrieve(dataset, request, tmp_path)
    except Pending:
        raise
    except Exception as e:                                # noqa: BLE001
        print(f"⚠️ [era5] CDS retrieve failed ({dataset} {year}-"
              f"{month:02d}): {e}", flush=True)
        try:
            os.remove(tmp_path)
        except OSError:
            pass
        return False

    try:
        if zipfile.is_zipfile(tmp_path):
            with tempfile.TemporaryDirectory() as td:
                with zipfile.ZipFile(tmp_path) as zf:
                    zf.extractall(td)
                ncs = [os.path.join(td, n) for n in os.listdir(td)
                       if n.endswith(".nc")]
                if not ncs:
                    raise RuntimeError("zip had no .nc members")
                parts = [xr.open_dataset(f) for f in ncs]
                try:
                    merged = xr.merge(parts, compat="override")
                    merged.to_netcdf(path)
                finally:
                    for p in parts:
                        p.close()
            os.remove(tmp_path)
        else:
            shutil.move(tmp_path, path)
        return True
    except Exception as e:                                # noqa: BLE001
        print(f"⚠️ [era5] post-process failed ({dataset} {year}-"
              f"{month:02d}): {e}", flush=True)
        for p in (tmp_path, path):
            try:
                os.remove(p)
            except OSError:
                pass
        return False


def fetch_era5_hourly_point(lat: float, lon: float, year: int,
                            variables: list[str], *,
                            dataset: str = "reanalysis-era5-single-levels",
                            months=None, area=None
                            ) -> dict:
    """Return ``{"time": [iso hourly], **{nc_var: [hourly values]}}`` for
    `year` at the ERA5 grid cell nearest (lat, lon).

    `variables` are CDS request names (e.g.
    '10m_u_component_of_wind'); returned keys are the NetCDF short names
    (e.g. 'u10') from `_NC_NAME`. Empty dict on any failure (missing
    deps, CDS error, file invalid) — caller's try/except handles.

    Fetched in monthly chunks (one small CDS job per month). By default
    fetches all 12 months of `year`; pass `months=[4,5]` to only request
    a specific subset — critical for weekly incrementals so we don't
    re-queue 10 months we'll throw away (CDS will throttle you for that).
    Partial coverage is returned if some months fail.

    `area` ([N, W, S, E]) requests one box shared by several points: CDS
    cost is per field, not per grid cell, so nearby points can share one
    cached request instead of queueing one each."""
    try:
        import cdsapi
        import xarray as xr
    except ImportError:
        return {}

    times: list[str] = []
    series: dict[str, list] = {}

    if months is None:
        months = range(1, 13)
    if area is None:
        area = _point_area(lat, lon)

    for month in months:
        path = _cache_path(area, year, month, tuple(variables), dataset)
        if not os.path.exists(path):
            try:
                ok = _retrieve_month(cdsapi, dataset, area, year, month,
                                     variables, path)
            except Pending as e:
                print(f"   [era5] pending — {e}", flush=True)
                PENDING.add(os.path.basename(path))
                continue
            if not ok:
                FAILED.add(os.path.basename(path))
                continue
        try:
            ds = xr.open_dataset(path)
        except Exception as e:                            # noqa: BLE001
            print(f"⚠️ [era5] open_dataset failed ({path}): {e}",
                  flush=True)
            continue
        try:
            # ERA5 NetCDF coord names vary by request and CDS platform
            # version. New CDS-Beta uses 'valid_time'.
            lat_name = "latitude" if "latitude" in ds.coords else "lat"
            lon_name = "longitude" if "longitude" in ds.coords else "lon"
            time_name = ("valid_time" if "valid_time" in ds.coords
                         else "time")
            lon_q = (lon if float(ds[lon_name].max()) <= 180.0
                     else lon % 360.0)
            pt = ds.sel({lat_name: lat, lon_name: lon_q},
                        method="nearest").load()
        finally:
            ds.close()

        times.extend(str(t)[:16].replace(" ", "T")
                     for t in pt[time_name].values)
        for req_name in variables:
            nc = _NC_NAME.get(req_name)
            if nc and nc in pt.data_vars:
                series.setdefault(nc, []).extend(
                    None if v != v else float(v)
                    for v in pt[nc].values)

    if not times:
        return {}
    out: dict[str, list] = {"time": times}
    out.update(series)
    return out
