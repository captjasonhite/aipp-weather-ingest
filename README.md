# aipp-weather-ingest (Option C data plane)

Free ECMWF/CMEMS/OISST → cropped SQLite → **GitHub Release assets**. The
Render app reads the published `latest.json` + `ecmwf_<cycle>.db`; it never
runs this. No Cloudflare/R2, no credit card, no object-store account.

See `OPTION_C_PLAN.md` in the app repo for the full design.

## One-time setup

1. **Public GitHub repo** `aipp-weather-ingest` (done). Public → scheduled
   Actions never pause on 60-day dormancy and have unlimited minutes.
   Layout: `ingest.py`, `.github/workflows/ingest.yml`.
2. **Secrets** (Repo → Settings → Secrets and variables → Actions) — only
   two; storage auth is the built-in `GITHUB_TOKEN` (no extra secret):
   - `COPERNICUSMARINE_SERVICE_USERNAME`
   - `COPERNICUSMARINE_SERVICE_PASSWORD`
   The workflow already grants `permissions: contents: write` so the Action
   can publish Release assets with the automatic token.
3. (Belt-and-suspenders) a free external cron (cron-job.org / CF Worker)
   that POSTs a `repository_dispatch` `{"event_type":"ingest"}` so ingestion
   survives even if GitHub ever changes public-repo scheduling.
4. Run once: **Actions → ingest → Run workflow**. Success = a release
   tagged `latest` with assets `latest.json` + `ecmwf_<cycle>.db`.

Published artifacts are world-readable at:

    https://github.com/<owner>/aipp-weather-ingest/releases/download/latest/latest.json
    https://github.com/<owner>/aipp-weather-ingest/releases/download/latest/ecmwf_<cycle>.db

The Render app sets `WEATHER_DATA_BASE` to the
`.../releases/download/latest` base.

## Local test (fast, no token, ECMWF core only)

    INGEST_STEPS=0,24,48 INGEST_SKIP_CMEMS=1 INGEST_SKIP_OISST=1 \
      python ingest.py

Writes `ingest/_out/ecmwf_<cycle>.db` + `latest.json`, prints rows/size,
confirms **no `-wal`/`-shm` sidecar** (guardrail A). Without
`GITHUB_TOKEN`/`GITHUB_REPOSITORY` it skips upload (local artifact only).
The full CMEMS+OISST+Release path runs on the first real Action.

## Cadence
2×/day (00z/12z) — aligns ECMWF wind with CMEMS MFWAM wave cycles,
minimizes Action minutes. Keeps the newest 2 cycle DBs in the release.
