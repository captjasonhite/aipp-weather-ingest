"""Option C ingest — runs in the PUBLIC aipp-weather-ingest repo's Action.

Pulls one ECMWF IFS 0.25 Open Data cycle (+ CMEMS swell partition + NOAA
OISST), crops to the operating bbox, flattens to a single self-contained
SQLite file, and atomically publishes it to Cloudflare R2 with a manifest.

The Render app never runs this; it only reads the published .db + manifest.

Env:
  R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
                                          -> publish to R2 (skipped if unset)
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

# ECMWF IFS 0.25 oper steps: 3-hourly to 144h, then 6-hourly to 168h.
_FULL_STEPS = list(range(0, 145, 3)) + [150, 156, 162, 168]
STEPS = ([int(s) for s in os.environ["INGEST_STEPS"].split(",")]
         if os.environ.get("INGEST_STEPS") else _FULL_STEPS)

OUT_DIR = os.environ.get("INGEST_OUT", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "_out"))
SKIP_CMEMS = os.environ.get("INGEST_SKIP_CMEMS", "") == "1"
SKIP_OISST = os.environ.get("INGEST_SKIP_OISST", "") == "1"


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


def _retrieve(client, target, **kw):
    """ECMWF retrieve with explicit exponential backoff (guardrail C)."""
    import time
    last = None
    for attempt in range(6):
        try:
            client.retrieve(target=target, **kw)
            return target
        except Exception as e:                       # noqa: BLE001
            last = e
            for p in (target, target + ".part"):
                try:
                    os.remove(p)
                except OSError:
                    pass
            time.sleep(min(120, 20 * (attempt + 1)))
    raise RuntimeError(f"ECMWF retrieve failed after retries: {last}")


def fetch_ecmwf(work):
    from ecmwf.opendata import Client
    c = Client(source="aws")
    run = c.latest(type="fc", param="10u").replace(tzinfo=dt.timezone.utc)
    atmos = _retrieve(c, os.path.join(work, "atmos.grib2"),
                      type="fc", step=STEPS, param=["10u", "10v", "10fg"])
    wave = _retrieve(c, os.path.join(work, "wave.grib2"),
                     type="fc", stream="wave", step=STEPS,
                     param=["swh", "mwp", "mwd"])
    return run, atmos, wave


def _crop(da):
    """Subset a DataArray to the bbox, snap coords, return (lats,lons,cube).
    cube shape: (step?, lat, lon) with snapped coord vectors."""
    da = da.sel(latitude=slice(LAT_MIN, LAT_MAX),
                longitude=slice(LON_MIN, LON_MAX))
    lats = _snap(da["latitude"].values)
    lons = _snap(da["longitude"].values)
    return lats, lons, da


def build_db(run, atmos, wave, db_path):
    import xarray as xr  # noqa: F401  (engine used via _grib_da)

    def series(path, short):
        la, lo, da = _crop(_grib_da(path, short))
        v = np.atleast_1d(da.values)
        if v.ndim == 2:                    # single step -> add step axis
            v = v[None, ...]
        vt = np.atleast_1d(da["valid_time"].values).astype(
            "datetime64[s]")
        return la, lo, vt, v.astype("float32")

    lats, lons, vt, u10 = series(atmos, "10u")
    _, _, _, v10 = series(atmos, "10v")
    _, _, _, fg10 = series(atmos, "10fg")
    wla, wlo, _, swh = series(wave, "swh")
    _, _, _, mwp = series(wave, "mwp")
    _, _, _, mwd = series(wave, "mwd")
    nstep = u10.shape[0]
    steps = [int(round((t - np.datetime64(run.replace(tzinfo=None), "s"))
                        / np.timedelta64(1, "h"))) for t in vt]

    # Optional swell partition (CMEMS) + SST (OISST), interpolated onto the
    # ECMWF grid so the app deals with ONE grid.
    sw = {}
    if not SKIP_CMEMS:
        sw = fetch_cmems_on_grid(lats, lons, vt, steps)
    sst_grid = None
    if not SKIP_OISST:
        sst_grid = fetch_oisst_on_grid(lats, lons)

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
        " sw1_d REAL, PRIMARY KEY(lat,lon,step));"
        "CREATE TABLE sst(lat REAL, lon REAL, sst REAL,"
        " PRIMARY KEY(lat,lon));")
    cx.execute(
        "INSERT INTO meta VALUES(?,?,?,?,?,?,?,?)",
        (run.strftime("%Y-%m-%dT%H:%M:%SZ"), GRID_RES, LAT_MIN, LAT_MAX,
         LON_MIN, LON_MAX, json.dumps(steps),
         dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))

    rows = []
    for si in range(nstep):
        st = steps[si]
        sh = sw.get(st) if sw else None
        for iy, la in enumerate(lats):
            for ix, lo in enumerate(lons):
                rows.append((
                    float(la), float(lo), st,
                    float(u10[si, iy, ix]), float(v10[si, iy, ix]),
                    float(fg10[si, iy, ix]), float(swh[si, iy, ix]),
                    float(mwp[si, iy, ix]), float(mwd[si, iy, ix]),
                    None if sh is None else float(sh["h"][iy, ix]),
                    None if sh is None else float(sh["t"][iy, ix]),
                    None if sh is None else float(sh["d"][iy, ix])))
    cx.executemany("INSERT INTO fc VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    if sst_grid is not None:
        cx.executemany(
            "INSERT INTO sst VALUES(?,?,?)",
            [(float(la), float(lo), float(sst_grid[iy, ix]))
             for iy, la in enumerate(lats) for ix, lo in enumerate(lons)
             if sst_grid[iy, ix] == sst_grid[iy, ix]])  # drop NaN (land)
    cx.commit()
    cx.execute("VACUUM")            # guardrail A: single static .db, no -wal
    cx.close()
    return run, steps, len(rows)


def fetch_cmems_on_grid(lats, lons, vt, steps):
    """CMEMS MFWAM SW1 interpolated onto the ECMWF grid, keyed by ECMWF
    step (each step's valid_time -> nearest CMEMS time)."""
    import xarray as xr
    import copernicusmarine
    ds = copernicusmarine.open_dataset(
        dataset_id="cmems_mod_glo_wav_anfc_0.083deg_PT3H-i",
        variables=["VHM0_SW1", "VTM01_SW1", "VMDR_SW1"],
        minimum_longitude=LON_MIN, maximum_longitude=LON_MAX,
        minimum_latitude=LAT_MIN, maximum_latitude=LAT_MAX,
        username=os.environ["COPERNICUSMARINE_SERVICE_USERNAME"],
        password=os.environ["COPERNICUSMARINE_SERVICE_PASSWORD"])
    out = {}
    for t, st in zip(vt, steps):
        snap = ds.sel(time=np.datetime64(t), method="nearest").interp(
            latitude=xr.DataArray(lats, dims="y"),
            longitude=xr.DataArray(lons, dims="x"))
        out[int(st)] = {"h": snap["VHM0_SW1"].values,
                        "t": snap["VTM01_SW1"].values,
                        "d": snap["VMDR_SW1"].values}
    return out


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
                return s.interp(
                    lat=xr.DataArray(lats, dims="y"),
                    lon=xr.DataArray(lons, dims="x")).values
            except Exception:                         # noqa: BLE001
                continue
    raise RuntimeError("OISST: no recent file found")


def publish(db_path, run):
    """Atomic publish: upload .db to completion, THEN flip latest.json."""
    sha = hashlib.sha256(open(db_path, "rb").read()).hexdigest()
    size = os.path.getsize(db_path)
    manifest = {
        "cycle_utc": run.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "db_key": f"ecmwf_{run:%Y%m%d%H}.db", "bytes": size, "sha256": sha,
        "created_utc": dt.datetime.now(dt.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")}
    with open(os.path.join(OUT_DIR, "latest.json"), "w") as fh:
        json.dump(manifest, fh)
    ep = os.environ.get("R2_ENDPOINT")
    if not ep:
        print(f"[no R2_* env — wrote {OUT_DIR}/latest.json, upload skipped]")
        return
    import boto3
    s3 = boto3.client(
        "s3", endpoint_url=ep,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"])
    bucket = os.environ["R2_BUCKET"]
    key = manifest["db_key"]
    s3.upload_file(db_path, bucket, key)              # 1. data first
    s3.put_object(Bucket=bucket, Key="latest.json",
                  Body=json.dumps(manifest).encode(),
                  ContentType="application/json")     # 2. flip pointer
    # keep current + previous cycle only
    objs = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
    dbs = sorted((o["Key"] for o in objs if o["Key"].endswith(".db")),
                 reverse=True)
    for old in dbs[2:]:
        s3.delete_object(Bucket=bucket, Key=old)
    print(f"[published {key} + latest.json]")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    work = os.path.join(OUT_DIR, "work")
    os.makedirs(work, exist_ok=True)
    run, atmos, wave = fetch_ecmwf(work)
    db_path = os.path.join(OUT_DIR, f"ecmwf_{run:%Y%m%d%H}.db")
    run, steps, nrows = build_db(run, atmos, wave, db_path)
    sidecars = [p for p in (db_path + "-wal", db_path + "-shm")
                if os.path.exists(p)]
    print(f"cycle={run:%Y-%m-%dT%H}Z steps={len(steps)} rows={nrows} "
          f"size={os.path.getsize(db_path)/1e6:.1f}MB "
          f"sidecars={sidecars or 'none (OK)'}")
    publish(db_path, run)


if __name__ == "__main__":
    main()
