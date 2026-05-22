"""Option C ingest — runs in the PUBLIC aipp-weather-ingest repo's Action.

Pulls one ECMWF IFS 0.25 Open Data cycle (+ CMEMS swell partition + NOAA
OISST), crops to the operating bbox, flattens to a single self-contained
SQLite file, and atomically publishes it as GitHub Release assets (tag
`latest`) with a manifest. No object store / credit card.

The Render app never runs this; it only reads the published .db + manifest
from .../releases/download/latest/.

Env:
  GITHUB_TOKEN, GITHUB_REPOSITORY   -> publish to Releases (auto in Actions;
                                       skipped if unset -> local artifact)
  COPERNICUSMARINE_SERVICE_USERNAME/PASSWORD   -> CMEMS auth
  INGEST_STEPS=0,24,48      -> override step list (local fast test)
  INGEST_SKIP_CMEMS=1 / INGEST_SKIP_OISST=1    -> local test without those
  INGEST_OUT=/path/dir      -> output dir (default: ./_out)
"""
import datetime as dt
import hashlib
import json
import os
import sqlite3

import numpy as np

# Operating bbox — CONFIRMED in OPTION_C_PLAN.md §7 (station-DB scan).
LAT_MIN, LAT_MAX = 8.0, 34.0
LON_MIN, LON_MAX = -120.0, -62.0
GRID_RES = 0.25

# Hi-res wave overlay (Phase 2): CMEMS MFWAM native ~0.083 (8 km) over a
# few complex/enclosed-water boxes where 0.25 under-resolves geography.
# fetch_free uses this when a point is in a box AND step <= WAV_MAX_STEP,
# else it falls back to the 0.25 fc wave. Boxes padded ~0.25.
WAV_RES = 1.0 / 12.0
WAV_MAX_STEP = 120                       # 5-day hi-res wave horizon
WAV_STEP_EVERY = 6                       # 6-hourly overlay steps
WAV_BOXES = [
    (21.25, 32.75, -118.75, -106.25),    # Pacific Baja + Sea of Cortez
    (17.25, 19.25,  -65.75,  -63.75),    # E. Caribbean (BVI / USVI)
]

# MSLP (Phase 2b): synoptic mean-sea-level pressure for H/L pressure
# centers + the NPH 35N transect (lon to -160, OUTSIDE the wave bbox).
# ECMWF Open Data `msl` is global 0.25 @ ~0.5 MB/step; we crop to the
# North Pacific and coarsen (pressure is synoptic — 0.5 is plenty).
MSLP_LAT_MIN, MSLP_LAT_MAX = 0.0, 60.0
MSLP_LON_MIN, MSLP_LON_MAX = -180.0, -90.0   # to -90: covers H/L domain (-95) +margin
MSLP_RES = 0.5
MSLP_STEP_EVERY = 6                      # 6-hourly (pressure evolves slowly)

# ECMWF IFS 0.25 oper steps: 3-hourly to 144h, then 6-hourly to 168h.
_FULL_STEPS = list(range(0, 145, 3)) + [150, 156, 162, 168]
STEPS = ([int(s) for s in os.environ["INGEST_STEPS"].split(",")]
         if os.environ.get("INGEST_STEPS") else _FULL_STEPS)

OUT_DIR = os.environ.get("INGEST_OUT", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "_out"))
SKIP_CMEMS = os.environ.get("INGEST_SKIP_CMEMS", "") == "1"
SKIP_OISST = os.environ.get("INGEST_SKIP_OISST", "") == "1"


def _timed(label, fn):
    """Run fn(), print '[timing] <label> Ns' (flushed) so a stalled phase
    is identifiable straight from the Action log."""
    import time
    s = time.perf_counter()
    r = fn()
    print(f"[timing] {label} {time.perf_counter() - s:.1f}s", flush=True)
    return r


def _snap(a):
    """Round a coord array onto the 0.25 grid (guardrail B: exact keys)."""
    return np.round(np.asarray(a, float) / GRID_RES) * GRID_RES


def _grib_da(path, short):
    import xarray as xr
    ds = xr.open_dataset(
        path, engine="cfgrib",
        backend_kwargs={"filter_by_keys": {"shortName": short},
                        "indexpath": ""})
    da = ds[list(ds.data_vars)[0]]
    lonv = da["longitude"]
    if float(lonv.max()) > 180.0:
        da = da.assign_coords(
            longitude=(((lonv + 180.0) % 360.0) - 180.0)).sortby("longitude")
    return da.sortby("latitude")


_ECMWF_SOURCES = ["aws", "ecmwf", "azure"]


class _RetrieveTimeout(Exception):
    """Raised (via SIGALRM, fired by the progress watchdog) when a
    _retrieve attempt STALLS — the `.part` file stops growing. A slow but
    still-progressing download is never killed; only a true stall / a
    multiurl 429-backoff that isn't recovering triggers a mirror rotation."""


def _on_alarm(signum, frame):
    raise _RetrieveTimeout("download stalled (no progress)")


# Abort an attempt only after this many seconds of NO byte progress —
# distinguishes a genuine stall from a legitimately slow large download.
_STALL_TIMEOUT_S = int(os.environ.get("INGEST_STALL_TIMEOUT", "180"))


def _has_all_params(path, params):
    """True iff the GRIB at `path` contains every requested shortName.
    Cheap probe used only by the opt-in local cache (INGEST_REUSE_ATMOS)."""
    try:
        import xarray as xr
        for sn in params:
            ds = xr.open_dataset(
                path, engine="cfgrib",
                backend_kwargs={"filter_by_keys": {"shortName": sn},
                                "indexpath": ""})
            ok = bool(ds.data_vars)
            ds.close()
            if not ok:
                return False
        return True
    except Exception:                                # noqa: BLE001
        return False


def _retrieve(target, **kw):
    """ECMWF Open Data retrieve with MIRROR ROTATION + backoff (guardrail
    C). The `aws` S3 mirror throttles heavily (sustained 503 SlowDown);
    rotating aws->ecmwf->azure abandons a degraded mirror instead of
    hammering it. 9 attempts, capped 300 s backoff, `.part` cleaned
    between tries.

    OPT-IN local cache: if INGEST_REUSE_ATMOS=1 and `target` already exists
    with all requested params, the download is skipped (production keeps
    this OFF so it never serves stale data)."""
    from ecmwf.opendata import Client
    import signal
    import threading
    import time
    if os.environ.get("INGEST_REUSE_ATMOS") == "1" \
            and os.path.exists(target) \
            and _has_all_params(target, kw.get("param") or []):
        print(f"[ecmwf] REUSED cached {os.path.basename(target)} "
              f"(INGEST_REUSE_ATMOS=1)", flush=True)
        return target

    def _part_size():
        sz = 0
        for p in (target, target + ".part"):
            try:
                sz = max(sz, os.path.getsize(p))
            except OSError:
                pass
        return sz

    last = None
    for attempt in range(9):
        src = _ECMWF_SOURCES[attempt % len(_ECMWF_SOURCES)]
        # Progress watchdog: a daemon thread polls the .part file; if it
        # stops growing for _STALL_TIMEOUT_S (true stall, or a multiurl
        # 429-backoff that isn't recovering), it SIGALRMs the main thread
        # so our outer mirror rotation takes over. A slow-but-progressing
        # download is NEVER interrupted.
        prev = signal.signal(signal.SIGALRM, _on_alarm)
        stop = threading.Event()

        def _watch():
            last_sz, last_grow = -1, time.time()
            while not stop.wait(15):
                sz = _part_size()
                if sz > last_sz:
                    last_sz, last_grow = sz, time.time()
                elif time.time() - last_grow > _STALL_TIMEOUT_S:
                    os.kill(os.getpid(), signal.SIGALRM)
                    return

        wt = threading.Thread(target=_watch, daemon=True)
        wt.start()
        try:
            Client(source=src).retrieve(target=target, **kw)
            return target
        except Exception as e:                       # noqa: BLE001
            last = e
            print(f"[ecmwf] {src} attempt {attempt + 1}/9 failed: "
                  f"{str(e)[:140]}", flush=True)
            for p in (target, target + ".part"):
                try:
                    os.remove(p)
                except OSError:
                    pass
            time.sleep(min(300, 30 * (attempt + 1)))
        finally:
            stop.set()
            signal.signal(signal.SIGALRM, prev)
    raise RuntimeError(f"ECMWF retrieve failed after retries: {last}")


def fetch_ecmwf(work):
    atmos = _retrieve(os.path.join(work, "atmos.grib2"),
                      type="fc", step=STEPS,
                      param=["10u", "10v", "10fg", "msl", "2t"])
    # Waves now come from CMEMS MFWAM (8 km, better coastal physics) — the
    # coarse 0.25 ECMWF wave stream is no longer pulled (saves ~138 MB/run).
    # Authoritative cycle = the base time of the data we ACTUALLY got, not
    # c.latest(): the index can advance to a cycle (e.g. 06z) before all its
    # fields are published, so c.latest() raced ahead of the 00z GRIB really
    # served -> negative steps + a mislabeled cycle. The GRIB is self-
    # describing, so trust it.
    base = np.atleast_1d(_grib_da(atmos, "10u")["time"].values)[0]
    run = (np.datetime64(base, "s").astype("datetime64[s]").tolist()
           .replace(tzinfo=dt.timezone.utc))
    return run, atmos


def _crop(da):
    """Subset a DataArray to the bbox, snap coords, return (lats,lons,cube).
    cube shape: (step?, lat, lon) with snapped coord vectors."""
    da = da.sel(latitude=slice(LAT_MIN, LAT_MAX),
                longitude=slice(LON_MIN, LON_MAX))
    lats = _snap(da["latitude"].values)
    lons = _snap(da["longitude"].values)
    return lats, lons, da


def build_db(run, atmos, db_path):
    import xarray as xr  # noqa: F401  (engine used via _grib_da)

    def series(path, short):
        la, lo, da = _crop(_grib_da(path, short))
        v = np.atleast_1d(da.values)
        if v.ndim == 2:                    # single step -> add step axis
            v = v[None, ...]
        vt = np.atleast_1d(da["valid_time"].values).astype(
            "datetime64[s]")
        return la, lo, vt, v.astype("float32")

    def _steps_of(vts):
        return [int(round((t - np.datetime64(run.replace(tzinfo=None), "s"))
                          / np.timedelta64(1, "h"))) for t in vts]

    def _bystep(vts, cube):
        # ECMWF open data returns DIFFERENT step counts per param (the wave
        # stream / 10fg are shorter than 10u/10v) -> key each field by its
        # OWN step list so a short param can't overrun a shared index.
        return {s: cube[i] for i, s in enumerate(_steps_of(vts))}

    lats, lons, u10_vt, u10 = series(atmos, "10u")
    _, _, v10_vt, v10 = series(atmos, "10v")
    _, _, fg_vt, fg10 = series(atmos, "10fg")
    _, _, t2_vt, t2m = series(atmos, "2t")
    U = _bystep(u10_vt, u10)
    V, FG = _bystep(v10_vt, v10), _bystep(fg_vt, fg10)
    T2 = _bystep(t2_vt, t2m)             # 2 m temp (NPH thermal-low contrast)
    steps = sorted(s for s in U if s >= 0)   # driver=10u; drop pre-base hrs
    vt = u10_vt                  # CMEMS nearest-time aligns to driver steps

    # Waves (total swh/mwp/mwd) + swell partition (sw1_*) both from CMEMS
    # MFWAM 8 km, resampled to the ECMWF grid + SST (OISST), so the app deals
    # with ONE grid.
    sw, cmems_ncf = {}, None
    if not SKIP_CMEMS:
        sw, cmems_ncf = _timed("cmems", lambda: fetch_cmems_on_grid(
            lats, lons, vt, steps))
    sst_grid = None
    if not SKIP_OISST:
        sst_grid = _timed("oisst", lambda: fetch_oisst_on_grid(lats, lons))

    if os.path.exists(db_path):
        os.remove(db_path)
    cx = sqlite3.connect(db_path)
    cx.executescript(
        "PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF;"
        "CREATE TABLE meta(cycle_utc TEXT, grid_res REAL, lat_min REAL,"
        " lat_max REAL, lon_min REAL, lon_max REAL, steps_json TEXT,"
        " created_utc TEXT);"
        "CREATE TABLE fc(lat REAL, lon REAL, step INT, u10 REAL, v10 REAL,"
        " fg10 REAL, swh REAL, mwp REAL, mwd REAL, sw1_h REAL, sw1_t REAL,"
        " sw1_d REAL, t2m REAL, PRIMARY KEY(lat,lon,step));"
        "CREATE TABLE sst(lat REAL, lon REAL, sst REAL,"
        " PRIMARY KEY(lat,lon));"
        "CREATE TABLE wav_meta(res REAL, max_step INT, boxes_json TEXT,"
        " steps_json TEXT);"
        "CREATE TABLE wav(lat REAL, lon REAL, step INT, swh REAL, mwp REAL,"
        " mwd REAL, sw1_h REAL, sw1_t REAL, sw1_d REAL,"
        " PRIMARY KEY(lat,lon,step));"
        "CREATE TABLE mslp_meta(res REAL, lat_min REAL, lat_max REAL,"
        " lon_min REAL, lon_max REAL, steps_json TEXT);"
        "CREATE TABLE mslp(lat REAL, lon REAL, step INT, pa REAL,"
        " PRIMARY KEY(lat,lon,step));")
    cx.execute(
        "INSERT INTO meta VALUES(?,?,?,?,?,?,?,?)",
        (run.strftime("%Y-%m-%dT%H:%M:%SZ"), GRID_RES, LAT_MIN, LAT_MAX,
         LON_MIN, LON_MAX, json.dumps(steps),
         dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))

    rows = []
    for st in steps:
        u, v, fg, t2 = U[st], V.get(st), FG.get(st), T2.get(st)
        sh = sw.get(st) if sw else None       # CMEMS total + SW1 for step
        for iy, la in enumerate(lats):
            for ix, lo in enumerate(lons):
                rows.append((
                    float(la), float(lo), st,
                    float(u[iy, ix]),
                    None if v is None else float(v[iy, ix]),
                    None if fg is None else float(fg[iy, ix]),
                    None if sh is None else float(sh["swh"][iy, ix]),
                    None if sh is None else float(sh["mwp"][iy, ix]),
                    None if sh is None else float(sh["mwd"][iy, ix]),
                    None if sh is None else float(sh["sw1_h"][iy, ix]),
                    None if sh is None else float(sh["sw1_t"][iy, ix]),
                    None if sh is None else float(sh["sw1_d"][iy, ix]),
                    None if t2 is None else float(t2[iy, ix])))
    cx.executemany("INSERT INTO fc VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    if sst_grid is not None:
        cx.executemany(
            "INSERT INTO sst VALUES(?,?,?)",
            [(float(la), float(lo), float(sst_grid[iy, ix]))
             for iy, la in enumerate(lats) for ix, lo in enumerate(lons)
             if sst_grid[iy, ix] == sst_grid[iy, ix]])  # drop NaN (land)
    nwav = 0
    if cmems_ncf:
        nwav = _timed("wav", lambda: build_wav(cx, cmems_ncf, run))
    nmslp = _timed("mslp", lambda: build_mslp(cx, atmos, run))
    cx.commit()
    cx.execute("VACUUM")            # guardrail A: single static .db, no -wal
    cx.close()
    return run, steps, len(rows), nwav, nmslp


def fetch_cmems_on_grid(lats, lons, vt, steps):
    """CMEMS MFWAM total sea state (VHM0/VTM10/VMDR -> swh/mwp/mwd) + primary
    swell partition (VHM0_SW1/VTM01_SW1/VMDR_SW1 -> sw1_*) interpolated onto
    the ECMWF grid, keyed by ECMWF step (valid_time -> nearest CMEMS time).
    VMDR/VMDR_SW1 are CF `sea_surface_wave_from_direction` (same convention
    as ECMWF mwd / wind) — no directional transform applied.

    The bbox+time window is downloaded ONCE to a local NetCDF via
    `copernicusmarine.subset`, then every step is sampled in-memory. This
    replaces the previous lazy-remote `open_dataset` + per-step `.sel`,
    which made one network round-trip per step (57 -> timed out)."""
    import glob
    import xarray as xr
    import copernicusmarine
    t0 = np.atleast_1d(vt).astype("datetime64[s]")
    pad = np.timedelta64(3, "h")
    start = (t0.min() - pad).astype("datetime64[s]").astype(object)
    end = (t0.max() + pad).astype("datetime64[s]").astype(object)
    ncf = os.path.join(OUT_DIR, "cmems_sw1.nc")
    if os.path.exists(ncf):
        os.remove(ncf)
    kw = dict(
        dataset_id="cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
        variables=["VHM0", "VTM10", "VMDR",
                   "VHM0_SW1", "VTM01_SW1", "VMDR_SW1"],
        minimum_longitude=LON_MIN, maximum_longitude=LON_MAX,
        minimum_latitude=LAT_MIN, maximum_latitude=LAT_MAX,
        start_datetime=start, end_datetime=end,
        output_directory=OUT_DIR, output_filename=os.path.basename(ncf),
        username=os.environ["COPERNICUSMARINE_SERVICE_USERNAME"],
        password=os.environ["COPERNICUSMARINE_SERVICE_PASSWORD"])
    try:                                  # kwarg name varies across versions
        copernicusmarine.subset(overwrite=True, **kw)
    except TypeError:
        copernicusmarine.subset(**kw)
    if not os.path.exists(ncf):           # some versions rename the output
        cands = sorted(glob.glob(os.path.join(OUT_DIR, "cmems*.nc")),
                       key=os.path.getmtime)
        if not cands:
            raise RuntimeError("CMEMS subset produced no NetCDF")
        ncf = cands[-1]
    ds = xr.open_dataset(ncf).load()      # fully in-memory -> fast .sel
    yy = xr.DataArray(lats, dims="y")
    xx = xr.DataArray(lons, dims="x")
    out = {}
    for t, st in zip(t0, steps):
        snap = ds.sel(time=np.datetime64(t), method="nearest").sel(
            latitude=yy, longitude=xx, method="nearest")
        out[int(st)] = {"swh": snap["VHM0"].values,
                        "mwp": snap["VTM10"].values,
                        "mwd": snap["VMDR"].values,
                        "sw1_h": snap["VHM0_SW1"].values,
                        "sw1_t": snap["VTM01_SW1"].values,
                        "sw1_d": snap["VMDR_SW1"].values}
    ds.close()
    return out, ncf            # ncf reused for the native hi-res wav overlay


def _wav_snap(a):
    """Snap CMEMS coords onto the 0-based WAV_RES lattice so fetch_free's
    floor()-based corner lookup matches exactly (<= half-cell shift)."""
    return np.round(np.asarray(a, float) / WAV_RES) * WAV_RES


def build_wav(cx, ncf, run):
    """Native ~8 km CMEMS wave nodes inside WAV_BOXES, 6-hourly to
    WAV_MAX_STEP, into the `wav` table. Land/NaN nodes are skipped (sparse;
    fetch_free's bilinear renormalises missing corners)."""
    import xarray as xr
    ds = xr.open_dataset(ncf).load()
    base = np.datetime64(run.replace(tzinfo=None), "s")
    wav_steps = [s for s in (_FULL_STEPS if not os.environ.get("INGEST_STEPS")
                             else STEPS)
                 if 0 <= s <= WAV_MAX_STEP and s % WAV_STEP_EVERY == 0]
    seen = set()
    rows = []
    for st in wav_steps:
        snap = ds.sel(time=base + np.timedelta64(int(st), "h"),
                      method="nearest")
        for la_min, la_max, lo_min, lo_max in WAV_BOXES:
            b = snap.sel(latitude=slice(la_min, la_max),
                         longitude=slice(lo_min, lo_max))
            la = _wav_snap(b["latitude"].values)
            lo = _wav_snap(b["longitude"].values)
            V = {k: np.atleast_2d(b[k].values) for k in
                 ("VHM0", "VTM10", "VMDR",
                  "VHM0_SW1", "VTM01_SW1", "VMDR_SW1")}
            for iy in range(len(la)):
                for ix in range(len(lo)):
                    h = V["VHM0"][iy, ix]
                    if h != h:                       # NaN = land/no data
                        continue
                    key = (round(float(la[iy]), 4),
                           round(float(lo[ix]), 4), st)
                    if key in seen:
                        continue
                    seen.add(key)

                    def g(k):
                        x = V[k][iy, ix]
                        return None if x != x else float(x)
                    rows.append((key[0], key[1], st, float(h), g("VTM10"),
                                 g("VMDR"), g("VHM0_SW1"), g("VTM01_SW1"),
                                 g("VMDR_SW1")))
    ds.close()
    cx.executemany("INSERT OR IGNORE INTO wav VALUES(?,?,?,?,?,?,?,?,?)",
                   rows)
    cx.execute("INSERT INTO wav_meta VALUES(?,?,?,?)",
               (WAV_RES, WAV_MAX_STEP, json.dumps(WAV_BOXES),
                json.dumps(wav_steps)))
    return len(rows)


def build_mslp(cx, atmos, run):
    """Synoptic MSLP (Pa) over the North Pacific, coarsened to MSLP_RES,
    6-hourly — for H/L pressure centers + the NPH 35N transect. Own
    domain/grid (NOT the fc bbox), so it does not use _crop."""
    da = _grib_da(atmos, "msl")
    da = da.sel(latitude=slice(MSLP_LAT_MIN, MSLP_LAT_MAX),
                longitude=slice(MSLP_LON_MIN, MSLP_LON_MAX))
    tlat = np.round(np.arange(MSLP_LAT_MIN, MSLP_LAT_MAX + 1e-6,
                              MSLP_RES), 3)
    tlon = np.round(np.arange(MSLP_LON_MIN, MSLP_LON_MAX + 1e-6,
                              MSLP_RES), 3)
    import xarray as xr
    da = da.sel(latitude=xr.DataArray(tlat, dims="la"),
                longitude=xr.DataArray(tlon, dims="lo"), method="nearest")
    v = np.atleast_1d(da.values)
    if v.ndim == 2:
        v = v[None, ...]
    vt = np.atleast_1d(da["valid_time"].values).astype("datetime64[s]")
    base = np.datetime64(run.replace(tzinfo=None), "s")
    rows, steps = [], []
    for si in range(v.shape[0]):
        st = int(round((vt[si] - base) / np.timedelta64(1, "h")))
        if st < 0 or st % MSLP_STEP_EVERY != 0:
            continue
        steps.append(st)
        for iy in range(len(tlat)):
            for ix in range(len(tlon)):
                rows.append((float(tlat[iy]), float(tlon[ix]), st,
                             float(v[si, iy, ix])))
    cx.executemany("INSERT INTO mslp VALUES(?,?,?,?)", rows)
    cx.execute("INSERT INTO mslp_meta VALUES(?,?,?,?,?,?)",
               (MSLP_RES, MSLP_LAT_MIN, MSLP_LAT_MAX,
                MSLP_LON_MIN, MSLP_LON_MAX, json.dumps(sorted(set(steps)))))
    return len(rows)


def fetch_oisst_on_grid(lats, lons):
    """Latest NOAA OISST daily SST interpolated onto the ECMWF grid."""
    import urllib.request
    import xarray as xr
    base = ("https://noaa-cdr-sea-surface-temp-optimum-interpolation-pds"
            ".s3.amazonaws.com/data/v2.1/avhrr")
    today = dt.date.today()
    for back in range(1, 8):
        d = today - dt.timedelta(days=back)
        for suf in ("_preliminary", ""):
            url = (f"{base}/{d:%Y%m}/oisst-avhrr-v02r01.{d:%Y%m%d}{suf}.nc")
            try:
                tmp = os.path.join(OUT_DIR, "oisst.nc")
                urllib.request.urlretrieve(url, tmp)
                ds = xr.open_dataset(tmp)
                s = ds["sst"].isel(time=0)
                if "zlev" in s.dims:
                    s = s.isel(zlev=0)
                s = s.assign_coords(
                    lon=(((s["lon"] + 180) % 360) - 180)).sortby("lon")
                return s.sel(
                    lat=xr.DataArray(lats, dims="y"),
                    lon=xr.DataArray(lons, dims="x"),
                    method="nearest").values
            except Exception:                         # noqa: BLE001
                continue
    raise RuntimeError("OISST: no recent file found")


_GH = "https://api.github.com"
_UP = "https://uploads.github.com"


def _gh(method, url, token, data=None, ctype="application/json"):
    import urllib.request
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", ctype)
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read()
    return json.loads(body) if body and ctype != "application/octet-stream" \
        and url.startswith(_GH) else body


def publish(db_path, run):
    """Atomic publish to GitHub Releases (tag `latest`): upload the .db
    asset to completion, THEN replace latest.json (the pointer flip).
    App reads .../releases/download/latest/<name>."""
    sha = hashlib.sha256(open(db_path, "rb").read()).hexdigest()
    size = os.path.getsize(db_path)
    db_key = f"ecmwf_{run:%Y%m%d%H}.db"
    manifest = {
        "cycle_utc": run.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_key": db_key, "bytes": size, "sha256": sha,
        "created_utc": dt.datetime.now(dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")}
    with open(os.path.join(OUT_DIR, "latest.json"), "w") as fh:
        json.dump(manifest, fh)

    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")        # "owner/name"
    if not (token and repo):
        print(f"[no GITHUB_TOKEN/REPOSITORY — wrote {OUT_DIR}/latest.json, "
              f"upload skipped]")
        return
    import urllib.error

    # ensure release with tag `latest`
    try:
        rel = _gh("GET", f"{_GH}/repos/{repo}/releases/tags/latest", token)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        rel = _gh("POST", f"{_GH}/repos/{repo}/releases", token,
                  json.dumps({"tag_name": "latest", "name": "latest",
                              "body": "Option C weather data plane — "
                              "auto-published; do not edit."}).encode())
    rid = rel["id"]
    assets = {a["name"]: a["id"] for a in rel.get("assets", [])}

    def put_asset(name, blob, ctype):
        if name in assets:                            # assets are immutable
            _gh("DELETE",
                f"{_GH}/repos/{repo}/releases/assets/{assets[name]}", token)
        _gh("POST",
            f"{_UP}/repos/{repo}/releases/{rid}/assets?name={name}",
            token, blob, ctype)

    # 1. data first (new cycle name → additive, old db still referenced)
    with open(db_path, "rb") as fh:
        put_asset(db_key, fh.read(), "application/octet-stream")
    # 2. flip the pointer
    put_asset("latest.json", json.dumps(manifest).encode(),
              "application/json")
    # retention: keep newest 2 cycle DBs
    rel = _gh("GET", f"{_GH}/repos/{repo}/releases/tags/latest", token)
    dbs = sorted((a for a in rel.get("assets", [])
                  if a["name"].endswith(".db")),
                 key=lambda a: a["name"], reverse=True)
    for a in dbs[2:]:
        _gh("DELETE",
            f"{_GH}/repos/{repo}/releases/assets/{a['id']}", token)
    print(f"[published {db_key} + latest.json to {repo} release `latest`]")


def _preflight():
    """Cheap checks BEFORE the ~272 MB ECMWF download, so bad CMEMS creds
    / missing OISST fail in seconds instead of after a full pull."""
    if not SKIP_CMEMS:
        import copernicusmarine
        copernicusmarine.open_dataset(
            dataset_id="cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
            variables=["VHM0_SW1"],
            minimum_longitude=LON_MIN, maximum_longitude=LON_MIN + 0.5,
            minimum_latitude=LAT_MIN, maximum_latitude=LAT_MIN + 0.5,
            username=os.environ["COPERNICUSMARINE_SERVICE_USERNAME"],
            password=os.environ["COPERNICUSMARINE_SERVICE_PASSWORD"])
        print("[preflight] CMEMS auth OK")
    if not SKIP_OISST:
        import urllib.request
        base = ("https://noaa-cdr-sea-surface-temp-optimum-interpolation-"
                "pds.s3.amazonaws.com/data/v2.1/avhrr")
        ok = False
        for back in range(1, 8):
            d = dt.date.today() - dt.timedelta(days=back)
            for suf in ("_preliminary", ""):
                u = (f"{base}/{d:%Y%m}/oisst-avhrr-v02r01."
                     f"{d:%Y%m%d}{suf}.nc")
                try:
                    rq = urllib.request.Request(u, method="HEAD")
                    with urllib.request.urlopen(rq, timeout=20):
                        ok = True
                        break
                except Exception:                     # noqa: BLE001
                    continue
            if ok:
                break
        if not ok:
            raise RuntimeError("[preflight] no recent OISST file")
        print("[preflight] OISST reachable")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    work = os.path.join(OUT_DIR, "work")
    os.makedirs(work, exist_ok=True)
    _timed("preflight", _preflight)
    run, atmos = _timed("ecmwf", lambda: fetch_ecmwf(work))
    db_path = os.path.join(OUT_DIR, f"ecmwf_{run:%Y%m%d%H}.db")
    run, steps, nrows, nwav, nmslp = _timed(
        "build_db", lambda: build_db(run, atmos, db_path))
    sidecars = [p for p in (db_path + "-wal", db_path + "-shm")
                if os.path.exists(p)]
    print(f"cycle={run:%Y-%m-%dT%H}Z steps={len(steps)} rows={nrows} "
          f"wav_rows={nwav} mslp_rows={nmslp} "
          f"size={os.path.getsize(db_path)/1e6:.1f}MB "
          f"sidecars={sidecars or 'none (OK)'}")
    _timed("publish", lambda: publish(db_path, run))


if __name__ == "__main__":
    main()
