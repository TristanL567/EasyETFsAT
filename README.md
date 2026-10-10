# EasyETFsAT

Standalone Python + PostgreSQL backend for Austrian ETF tax reporting based on OeKB public data.

## Quick Start

1. Install dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
2. Start PostgreSQL:
   ```bash
   docker compose up -d
   ```
3. Run schema migration:
   ```bash
   alembic upgrade head
   ```
4. Start API:
   ```bash
   uvicorn fondant.api.main:app --reload
   ```

## PyCharm DB Connection

- Host: `localhost`
- Port: `5432`
- Database: `easyetfsat`
- User: `easyetfsat`
- Password: `easyetfsat`

After connecting, run:

```sql
SELECT 1;
```

Schema verification SQL is available in:

- `scripts/verify_schema.sql`

Naming convention dictionary:

- `docs/db_naming_dictionary.md`
- `docs/db_table_catalog.md`

## Core Endpoint

- `GET /etf/{isin}/tax?year={year}`

## Update-Jobs API (EasyRep)

JSON API for queueing update-data jobs and reading their status, used by EasyRep.
Every request needs the header `X-EasyETFsAT-Token`, compared in constant time with the env
var `EASYETFSAT_API_TOKEN`. If the env var is unset or empty, every route answers `503`. A
missing or wrong token gets `401`. The token is checked before the body, and the web
session cookie does not authenticate these routes.

- `POST /api/update-jobs` queues jobs with `JOBREQUSR = "easyrep"` and starts the same
  background runner as the web form. Before it processes jobs, the runner tops up ECB rates.
  - Body: exactly one of `{"isins": ["IE00BMTX1Y45", ...]}` (1 to 50 strings) or
    `{"scope": "existing"}` (every ISIN in `SOURCERPT`, no limit).
  - Each ISIN is trimmed, upper-cased and checked for format and checksum, like the web form.
    Duplicates are queued once.
  - `202` response:
    ```json
    {"jobs": [{"id": 31, "isin": "IE00BMTX1Y45", "status": "queued"}],
     "rejected": [{"value": "IE00BMTX1Y46", "reason": "invalid_isin"}],
     "skipped": [{"id": 30, "isin": "LU1681044993", "reason": "active_job"}]}
    ```
    `skipped` lists ISINs that already have a `queued` or `running` job, with that job's id.
    A request where every ISIN is rejected still answers `202`, with empty `jobs`.
  - `422` for a body that is not a JSON object, has both or neither key, a `scope` other than
    `"existing"`, an `isins` value that is not a non-empty list of strings, or more than 50 ISINs.
- `GET /api/update-jobs?ids=31,30` returns up to 100 jobs:
  ```json
  {"jobs": [{"id": 31, "isin": "IE00BMTX1Y45", "status": "success",
             "message": "Processed 6 FIN reports; wrote 1; ...", "error": null,
             "created": "2026-10-10T09:00:00Z", "started": "2026-10-10T09:00:01Z",
             "finished": "2026-10-10T09:00:09Z"}],
   "not_found": [30]}
  ```
  `status` is one of `queued|running|success|failed|skipped|cancelled`, `message` is `JOBMSG`,
  `error` is `JOBERR`, and the times are ISO 8601 UTC (`null` until set). Repeated ids are
  returned once. Malformed `ids`, no
  ids, ids outside 1..2147483647, or more than 100 ids get `422`.
- `GET /api/update-jobs/{id}` returns one job object as above, `404` if it does not exist,
  or `422` for an id outside 1..2147483647.

Example:

```bash
curl -X POST "$EASYETFSAT_URL/api/update-jobs" \
  -H "X-EasyETFsAT-Token: $EASYETFSAT_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"isins": ["IE00BMTX1Y45"]}'
```

## FX Pipeline (ECB)

- Backfill historical FX rates (`USD`, `GBP`, `CHF`) into `REFEXC`:
  ```bash
  python - <<'PY'
  import asyncio
  from fondant.ingestion.fx_pipeline import backfill_ecb_rates
  print(asyncio.run(backfill_ecb_rates()))
  PY
  ```
- Fetch latest available ECB rates (t-1 window):
  ```bash
  python - <<'PY'
  import asyncio
  from fondant.ingestion.fx_pipeline import fetch_latest_ecb_rates
  print(asyncio.run(fetch_latest_ecb_rates()))
  PY
  ```
- Top up rates from the latest stored date up to today (dry run unless `--apply`):
  ```bash
  python -m fondant.jobs.refresh_ecb_rates
  python -m fondant.jobs.refresh_ecb_rates --apply
  ```
  Every update-data run (web button and `python -m fondant.jobs.run_update_data_jobs`)
  does the same top-up before it processes queued jobs. A failed top-up is logged
  and does not stop the update.

## Migration Tests

- `tests/test_migrations.py` validates:
  - fresh install migration (`base -> head`)
  - rebuilt source + curated architecture at `20260419_0006`
- SQLite tests run by default.
- PostgreSQL tests use `testcontainers` (`postgres:16`) and auto-skip when Docker is unavailable.

## Seed ISINs

- `IE00BMTX1Y45`
- `LU1681044993`
- `LU0380865021`
- `LU0496786574`
- `LU2009147757`
- `IE000XZSV718`

## ISIN Storage + Incremental Ingestion

- ISIN storage file:
  - `Documentation/isin_storage.csv`

- Fetch only missing ISINs from storage (skips ISINs already in `SOURCERPT`):
  ```bash
  python -m fondant.jobs.fetch_missing_isins --dry-run --show-isins
  python -m fondant.jobs.fetch_missing_isins
  ```

- Add one ISIN and persist it to storage while running:
  ```bash
  python -m fondant.jobs.fetch_missing_isins --isin IE00BMTX1Y45 --persist-input
  ```

- Refresh all existing ISINs already in `SOURCERPT` (checks for OeKB changes):
  ```bash
  python -m fondant.jobs.refresh_existing_isins --dry-run --show-isins
  python -m fondant.jobs.refresh_existing_isins
  ```
