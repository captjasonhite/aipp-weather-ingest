"""Baja-Bash northbound historical pattern — ERA5 backfill, Option C.

Runs in this PUBLIC repo's weekly Action (see .github/workflows/baja-history.yml).
Pulls hourly ERA5 wind + wave for the 4 Baja stations via CDS, applies the
same red/yellow/green traffic-light logic the Render app uses live (single
source of truth: baja_logic.py — vendored from the forecast repo), and
publishes one `processed_<year>.json` per year as a GitHub Release asset
under tag `baja-history-latest`. The Render app reads those public URLs;
this script never talks to Render or Supabase.

Default run = **incremental update of the current year**:
  1. Download the current `processed_<year>.json` from the release (if any).
  2. Fetch only the days from (last_processed + 1) → today-6 (ERA5 lag).
  3. Merge, upload back to the release.

Backfill a full year (one-off):
  python baja_history_fetch.py --year 2025
"""
import argparse
import datetime
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from collections import defaultdict

# Vendored, pure-stdlib logic — keep in sync with forecast/baja_logic.py
from baja_logic import (
    get_bearing,
    get_apparent_data,
    get_apparent_period,
    get_sea_label,
    _BAJA_STATIONS,
    _traffic_light,
)
import era5_archive

# ── Constants ─────────────────────────────────────────────────────────────────

BOAT_SPEED  = 6.0
CAPE_FACTOR = 1.33
_MS_TO_KT   = 1.9438444924406046

OUT_DIR = os.environ.get(
    "BAJA_HISTORY_OUT",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "_out_baja"))

RELEASE_TAG = "baja-history-latest"
RELEASE_TITLE = "Baja Bash historical (ERA5)"
RELEASE_BODY = ("Option C baja-history data plane — auto-published weekly "
                "from ERA5 via CDS. Do not edit manually.")

_GH = "https://api.github.com"
_UP = "https://uploads.github.com"


def _build_stations():
    stations = []
    n = len(_BAJA_STATIONS)
    for idx, (name, lat, lon, cape) in enumerate(_BAJA_STATIONS):
        if idx < n - 1:
            _, n_lat, n_lon, _ = _BAJA_STATIONS[idx + 1]
            nb = get_bearing(lat, lon, n_lat, n_lon)
        else:
            _, p_lat, p_lon, _ = _BAJA_STATIONS[idx - 1]
            sb = get_bearing(lat, lon, p_lat, p_lon)
            nb = (sb + 180) % 360
        slug = name.lower().replace(" ", "_")
        stations.append({"name": name, "slug": slug, "lat": lat, "lon": lon,
                         "cape": cape, "nb": nb})
    return stations


STATIONS = _build_stations()


# ── GitHub Releases helpers (mirrors ingest.py's pattern) ─────────────────────

def _gh(method, url, token, data=None, ctype="application/json"):
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if data is not None:
        req.add_header("Content-Type", ctype)
    with urllib.request.urlopen(req, timeout=120) as r:
        body = r.read()
    if ctype == "application/octet-stream" or not url.startswith(_GH):
        return body
    return json.loads(body) if body else {}


def _ensure_release(repo, token):
    try:
        return _gh("GET",
                   f"{_GH}/repos/{repo}/releases/tags/{RELEASE_TAG}", token)
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
        return _gh("POST", f"{_GH}/repos/{repo}/releases", token,
                   json.dumps({"tag_name": RELEASE_TAG,
                               "name":    RELEASE_TITLE,
                               "body":    RELEASE_BODY}).encode())


def _put_asset(repo, token, rel, name, blob, ctype="application/json"):
    assets = {a["name"]: a["id"] for a in rel.get("assets", [])}
    if name in assets:                                    # immutable on GH
        _gh("DELETE",
            f"{_GH}/repos/{repo}/releases/assets/{assets[name]}", token)
    _gh("POST",
        f"{_UP}/repos/{repo}/releases/{rel['id']}/assets?name={name}",
        token, blob, ctype)


def _download_existing(repo, year):
    """Fetch the current processed_{year}.json from the release.
    Returns parsed dict, or None if asset doesn't exist."""
    url = (f"https://github.com/{repo}/releases/download/"
           f"{RELEASE_TAG}/processed_{year}.json")
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


# ── ERA5 → OM-shaped hourly dict (LA tz, kn) ──────────────────────────────────

def _utc_to_la(t_iso: str) -> str:
    from zoneinfo import ZoneInfo
    dt = datetime.datetime.fromisoformat(t_iso).replace(
        tzinfo=datetime.timezone.utc)
    return dt.astimezone(ZoneInfo("America/Los_Angeles")).strftime(
        "%Y-%m-%dT%H:00")


def _months_in_year(s, e, year):
    """Months of `year` that overlap the [s, e] date range (inclusive)."""
    lo = s.month if s.year == year else 1
    hi = e.month if e.year == year else 12
    return list(range(lo, hi + 1))


def _era5_wind_range(lat, lon, start_date, end_date):
    import math
    s = datetime.date.fromisoformat(start_date)
    e = datetime.date.fromisoformat(end_date)
    times, spd, dirs, gst = [], [], [], []
    for year in range(s.year, e.year + 1):
        data = era5_archive.fetch_era5_hourly_point(
            lat, lon, year,
            ["10m_u_component_of_wind", "10m_v_component_of_wind",
             "10m_wind_gust_since_previous_post_processing"],
            months=_months_in_year(s, e, year))
        if not data.get("time"):
            continue
        us = data.get("u10", [])
        vs = data.get("v10", [])
        gs = data.get("fg10", [])
        for i, t_utc in enumerate(data["time"]):
            d_utc = t_utc[:10]
            if not (s.isoformat() <= d_utc <= e.isoformat()):
                continue
            u = us[i] if i < len(us) else None
            v = vs[i] if i < len(vs) else None
            g = gs[i] if i < len(gs) else None
            if u is None or v is None:
                continue
            sp = math.hypot(u, v) * _MS_TO_KT
            dr = (270.0 - math.degrees(math.atan2(v, u))) % 360.0
            times.append(_utc_to_la(t_utc))
            spd.append(round(sp, 2))
            dirs.append(round(dr, 1))
            gst.append(None if g is None else round(g * _MS_TO_KT, 2))
    return {"hourly": {"time": times,
                       "wind_speed_10m":     spd,
                       "wind_direction_10m": dirs,
                       "wind_gusts_10m":     gst}}


def _era5_marine_range(lat, lon, start_date, end_date):
    s = datetime.date.fromisoformat(start_date)
    e = datetime.date.fromisoformat(end_date)
    times, h, p, d = [], [], [], []
    for year in range(s.year, e.year + 1):
        data = era5_archive.fetch_era5_hourly_point(
            lat, lon, year,
            ["significant_height_of_combined_wind_waves_and_swell",
             "mean_wave_period", "mean_wave_direction"],
            months=_months_in_year(s, e, year))
        if not data.get("time"):
            continue
        hs = data.get("swh", [])
        ps = data.get("mwp", [])
        ds = data.get("mwd", [])
        for i, t_utc in enumerate(data["time"]):
            d_utc = t_utc[:10]
            if not (s.isoformat() <= d_utc <= e.isoformat()):
                continue
            times.append(_utc_to_la(t_utc))
            h.append(None if i >= len(hs) or hs[i] is None
                     else round(float(hs[i]), 2))
            p.append(None if i >= len(ps) or ps[i] is None
                     else round(float(ps[i]), 2))
            d.append(None if i >= len(ds) or ds[i] is None
                     else round(float(ds[i]), 1))
    return {"hourly": {"time": times,
                       "wave_height":    h,
                       "wave_period":    p,
                       "wave_direction": d}}


# ── Per-hour / per-station / per-day processing ──────────────────────────────

def _hour_color(wind_spd, wind_gust, wind_dir, wave_h, wave_p, wave_dir,
                nb, cape):
    if any(v is None for v in [wind_spd, wind_gust, wind_dir]):
        return None
    w = wind_spd * CAPE_FACTOR if cape else wind_spd
    g = wind_gust * CAPE_FACTOR if cape else wind_gust
    aws,      awa, _   = get_apparent_data(w, wind_dir, BOAT_SPEED, nb)
    aws_gust, _,   _   = get_apparent_data(g, wind_dir, BOAT_SPEED, nb)
    if any(v is None for v in [wave_h, wave_p, wave_dir]):
        label, flags = "GOOD", []
    else:
        app_p = get_apparent_period(wave_p, wave_dir, BOAT_SPEED, nb)
        label, flags = get_sea_label(wave_h, wave_p, app_p, wave_dir, nb,
                                     w, awa, g)
    if cape and "⚠️ CAPE EFFECT" not in flags:
        flags = list(flags) + ["⚠️ CAPE EFFECT"]
    worst = {"app_w": aws, "app_w_gust": aws_gust, "awa": awa,
             "label": label, "flags": flags,
             "w": wind_spd, "g": wind_gust}
    signal = _traffic_light(worst)
    if "🔴" in signal: return "red"
    if "🟡" in signal: return "yellow"
    return "green"


def _process_station(wind_raw, marine_raw, nb, cape):
    wh = wind_raw["hourly"]; mh = marine_raw["hourly"]
    wt, ws, wg, wd = wh["time"], wh["wind_speed_10m"], wh["wind_gusts_10m"], wh["wind_direction_10m"]
    midx = {t: i for i, t in enumerate(mh["time"])}
    h, p, d = mh["wave_height"], mh["wave_period"], mh["wave_direction"]
    daily = defaultdict(list)
    for i, t in enumerate(wt):
        daily[t[:10]].append(i)
    out = {}
    for date in sorted(daily.keys()):
        cols, winds, waves = [], [], []
        for i in daily[date]:
            t  = wt[i]
            mi = midx.get(t)
            hv = h[mi] if mi is not None else None
            pv = p[mi] if mi is not None else None
            dv = d[mi] if mi is not None else None
            c  = _hour_color(ws[i], wg[i], wd[i], hv, pv, dv, nb, cape)
            cols.append(c)
            if ws[i] is not None: winds.append(ws[i])
            if hv is not None:    waves.append(hv)
        best, bstart, cur, cstart = 0, 0, 0, 0
        for idx, c in enumerate(cols):
            if c == "green":
                if cur == 0: cstart = idx
                cur += 1
                if cur > best: best, bstart = cur, cstart
            else:
                cur = 0
        day = "green" if best >= 6 else "yellow" if best >= 3 else "red"
        out[date] = {
            "color": day, "best_window_h": best, "best_start": bstart,
            "green_h":  cols.count("green"),
            "yellow_h": cols.count("yellow"),
            "red_h":    cols.count("red"),
            "avg_wind":   round(sum(winds)/len(winds), 1) if winds else 0,
            "avg_wave_h": round(sum(waves)/len(waves), 2) if waves else 0,
        }
    return out


def _combine(by_station, all_dates):
    slugs = [s["slug"] for s in STATIONS]
    result = {}
    for date in all_dates:
        row = {}
        gcount = 0
        for slug in slugs:
            d = by_station.get(slug, {}).get(date)
            row[slug] = d
            if d and d["color"] == "green":
                gcount += 1
        row["green_count"] = gcount
        row["all_green"]   = gcount == len(STATIONS)
        result[date] = row
    return result


def _end_date_for(year):
    """ERA5 has ~5-day lag — clamp to today-6 for current year."""
    today = datetime.date.today()
    if year < today.year:
        return f"{year}-12-31"
    return (today - datetime.timedelta(days=6)).isoformat()


def _process_range(year, start, end):
    """Fetch + process [start, end] for one year. Returns combined dict."""
    sd = datetime.date.fromisoformat(start)
    ed = datetime.date.fromisoformat(end)
    months = _months_in_year(sd, ed, year)
    n_jobs = len(STATIONS) * 2 * len(months)              # 2 datasets per station
    print(f"\n── Baja Bash {year}  {start} → {end} "
          f"({len(months)} months × {len(STATIONS)} stations × 2 datasets "
          f"= {n_jobs} CDS jobs) ──", flush=True)
    by_station = {}
    for s in STATIONS:
        print(f"  {s['name']} …", flush=True)
        wind   = _era5_wind_range(s["lat"], s["lon"], start, end)
        marine = _era5_marine_range(s["lat"], s["lon"], start, end)
        by_station[s["slug"]] = _process_station(wind, marine, s["nb"], s["cape"])
        n = len(by_station[s["slug"]])
        g = sum(1 for d in by_station[s["slug"]].values() if d["color"] == "green")
        print(f"    {n} days, {g} green", flush=True)
    all_dates = sorted({date for sd in by_station.values() for date in sd})
    return _combine(by_station, all_dates)


# ── Top-level orchestration ───────────────────────────────────────────────────

def run_incremental(year, repo, token):
    """Update the current year incrementally on the release. Returns the
    final dict and a status string."""
    existing = _download_existing(repo, year) if repo else None
    if existing is None and not repo:
        existing = {}                                     # local-test path
    if existing is None:
        print(f"[incremental] no existing release asset for {year} — "
              f"falling back to full-year fetch", flush=True)
        return run_full(year, repo, token)

    last = sorted(existing.keys())[-1] if existing else None
    start = (datetime.date.fromisoformat(last)
             + datetime.timedelta(days=1)).isoformat() if last else f"{year}-01-01"
    end = _end_date_for(year)
    if start > end:
        print(f"[incremental] {year} already current (last={last}, "
              f"end={end}) — nothing to do", flush=True)
        return existing, "skipped"

    new = _process_range(year, start, end)
    merged = dict(existing)
    merged.update(new)
    return merged, f"+{len(new)} days, total={len(merged)}"


def run_full(year, repo, token):
    """Full-year fetch (use for one-off backfills)."""
    end = _end_date_for(year)
    data = _process_range(year, f"{year}-01-01", end)
    return data, f"full {len(data)} days"


def publish(year, data, repo, token):
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, f"processed_{year}.json")
    with open(out_path, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    size = os.path.getsize(out_path)
    sha  = hashlib.sha256(open(out_path, "rb").read()).hexdigest()
    print(f"[saved] {out_path}  ({size} bytes, sha256={sha[:12]}…)",
          flush=True)
    if not (repo and token):
        print("[no GITHUB_TOKEN/REPOSITORY — local-only run]", flush=True)
        return
    rel = _ensure_release(repo, token)
    with open(out_path, "rb") as fh:
        _put_asset(repo, token, rel, f"processed_{year}.json", fh.read())
    print(f"[published processed_{year}.json to {repo} @ {RELEASE_TAG}]",
          flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int,
                        help="Backfill this specific year (full fetch). "
                             "Default: incremental update of current year.")
    args = parser.parse_args()

    repo  = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    year  = args.year or datetime.datetime.utcnow().year
    full  = args.year is not None

    print(f"baja-history run: year={year}, mode="
          f"{'full' if full else 'incremental'}, repo={repo or '(local)'}",
          flush=True)

    if full:
        data, status = run_full(year, repo, token)
    else:
        data, status = run_incremental(year, repo, token)

    print(f"[result] {status}", flush=True)
    if status == "skipped":
        return
    publish(year, data, repo, token)


if __name__ == "__main__":
    main()
