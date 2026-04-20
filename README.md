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
