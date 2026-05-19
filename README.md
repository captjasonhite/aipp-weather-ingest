# aipp-weather-ingest (Option C data plane)

Free ECMWF/CMEMS/OISST → cropped SQLite → Cloudflare R2. The Render app
reads the published `latest.json` + `ecmwf_<cycle>.db`; it never runs this.

See `OPTION_C_PLAN.md` in the app repo for the full design.

## One-time setup (needs your accounts — Claude can't provision these)

1. **Create a PUBLIC GitHub repo** `aipp-weather-ingest`. Public →
   scheduled Actions never pause on 60-day dormancy and have unlimited
   minutes. It must contain **no app secrets**.
2. Copy into it: `ingest.py`, and `.github-workflows-ingest.yml` →
   `.github/workflows/ingest.yml`.
3. **Cloudflare R2:** create bucket `aipp-weather`; create an R2 API token
   (Object Read & Write). Note the S3 endpoint URL.
4. **Repo → Settings → Secrets → Actions**, add:
   `R2_ENDPOINT`, `R2_BUCKET=aipp-weather`, `R2_ACCESS_KEY_ID`,
   `R2_SECRET_ACCESS_KEY`, `COPERNICUSMARINE_SERVICE_USERNAME`,
   `COPERNICUSMARINE_SERVICE_PASSWORD`.
5. (Belt-and-suspenders) create a free external cron (cron-job.org or a
   Cloudflare Worker) that POSTs a `repository_dispatch` `{"event_type":
   "ingest"}` to the repo so ingestion survives even if GitHub ever
   changes public-repo scheduling.
6. Run the workflow once via **Actions → ingest → Run workflow** and
   confirm `latest.json` + a `.db` appear in R2.

## Local test (fast, no R2, ECMWF core only)

    INGEST_STEPS=0,24,48 INGEST_SKIP_CMEMS=1 INGEST_SKIP_OISST=1 \
      python ingest.py

Writes `ingest/_out/ecmwf_<cycle>.db` and prints rows/size and confirms
**no `-wal`/`-shm` sidecar** (guardrail A). Full CMEMS+OISST+R2 path is
exercised by the first real Action run (their access is already proven
in Spike B / the OISST resolution).

## Cadence
2×/day (00z/12z) — aligns ECMWF wind with CMEMS MFWAM wave cycles,
minimizes Action minutes. Keeps current + previous cycle in R2.
