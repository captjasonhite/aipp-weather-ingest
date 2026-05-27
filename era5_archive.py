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
import hashlib
import os
import shutil
import tempfile
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


def _cache_path(lat: float, lon: float, year: int, month: int,
                variables: tuple, dataset: str) -> str:
    os.makedirs(_CACHE_DIR, exist_ok=True)
    h = hashlib.sha1(
        f"{dataset}|{lat:.3f}|{lon:.3f}|{year}|"
        f"{','.join(sorted(variables))}".encode()).hexdigest()[:10]
    return os.path.join(_CACHE_DIR,
                        f"era5_{year}_{month:02d}_{h}.nc")


def _retrieve_month(cdsapi, dataset, lat, lon, year, month,
                    variables, path) -> bool:
    """One-month CDS retrieve. Returns True on success.

    CDS-Beta packages mixed-stream variables (e.g. instantaneous u10/v10
    together with post-processed `fg10`) as a zip of per-stream NetCDFs
    regardless of `format=netcdf`. We detect that case, extract, merge
    via xarray, and write a single NetCDF at `path` so the caller's
    open_dataset stays simple."""
    import xarray as xr
    area = [lat + 0.5, lon - 0.5, lat - 0.5, lon + 0.5]
    dlast = calendar.monthrange(year, month)[1]
    tmp_path = path + ".tmp"
    try:
        cdsapi.Client(quiet=True).retrieve(
            dataset,
            {
                "product_type": "reanalysis",
                "variable":     list(variables),
                "year":         str(year),
                "month":        [f"{month:02d}"],
                "day":          [f"{d:02d}" for d in range(1, dlast + 1)],
                "time":         [f"{h:02d}:00" for h in range(24)],
                "area":         area,
                "format":       "netcdf",
            },
            tmp_path,
        )
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
                            dataset: str = "reanalysis-era5-single-levels"
                            ) -> dict:
    """Return ``{"time": [iso hourly], **{nc_var: [hourly values]}}`` for
    `year` at the ERA5 grid cell nearest (lat, lon).

    `variables` are CDS request names (e.g.
    '10m_u_component_of_wind'); returned keys are the NetCDF short names
    (e.g. 'u10') from `_NC_NAME`. Empty dict on any failure (missing
    deps, CDS error, file invalid) — caller's try/except handles.

    Fetched in monthly chunks (12 small CDS jobs per year). Partial-year
    coverage is returned if some months fail."""
    try:
        import cdsapi
        import xarray as xr
    except ImportError:
        return {}

    times: list[str] = []
    series: dict[str, list] = {}

    for month in range(1, 13):
        path = _cache_path(lat, lon, year, month,
                           tuple(variables), dataset)
        if not os.path.exists(path):
            if not _retrieve_month(cdsapi, dataset, lat, lon, year, month,
                                   variables, path):
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
