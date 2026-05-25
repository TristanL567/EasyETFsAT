# Technical Architecture

## Repository Purpose

EasyETFsAT is a standalone Python backend for Austrian ETF tax reporting. It ingests OeKB public ETF tax reports, stores both raw and curated data in PostgreSQL, supports ECB FX-rate ingestion, and exposes a small FastAPI service for querying ETF tax values.

## Stack

- Python `>=3.11`
- FastAPI and Uvicorn for the HTTP API
- SQLAlchemy 2 async ORM
- PostgreSQL 16 for the main runtime database
- Alembic for schema migrations
- httpx for OeKB and ECB HTTP clients
- Pydantic for external payload models and settings
- structlog for structured application logging
- pytest, pytest-asyncio, respx, aiosqlite, and testcontainers for tests
- Docker Compose for local PostgreSQL

## Runtime Configuration

Configuration is loaded from environment variables and `.env` through `fondant.config.Settings`.

Default values:

```text
DATABASE_URL=postgresql+asyncpg://easyetfsat:easyetfsat@localhost:5432/easyetfsat
OEKB_BASE_URL=https://my.oekb.at/fond-info/rest/public
ECB_BASE_URL=https://data-api.ecb.europa.eu/service
LOG_LEVEL=INFO
OEKB_RATE_LIMIT_PER_SECOND=4.0
OEKB_TIMEOUT_SECONDS=30
ECB_RATE_LIMIT_PER_SECOND=4.0
ECB_TIMEOUT_SECONDS=30
```

Alembic uses the same database URL converted to a synchronous driver form.

## Main Components

### API

Entry point:

- `fondant.api.main:create_app`
- Runtime app object: `fondant.api.main:app`

Routes:

- `GET /health`
- `GET /etf/{isin}/tax?year={year}`

The ETF tax endpoint:

1. Normalizes the input ISIN to uppercase.
2. Looks for `TAXRPT` rows matching ISIN and report year.
3. If no exact year rows exist, falls back to rows where `TAXYEA IS NULL`.
4. Loads `TAXDAT` joined to `TAXLIN` and `TAXCAT`.
5. Returns report metadata and a nested `tax_fields` dictionary keyed by tax metric and investor category.

Example response shape:

```json
{
  "isin": "IE00BMTX1Y45",
  "year": 2025,
  "year_fallback_null_used": false,
  "count": 1,
  "reports": [
    {
      "stm_id": 111,
      "versions_nr": 1,
      "status_code": "FIN",
      "waehrung": "EUR",
      "meldg_datum": "2025-05-01",
      "tax_fields": {
        "ag_ertraege": {
          "pv_mit": 1.2
        }
      }
    }
  ]
}
```

### OeKB Client

Implemented in:

- `fondant.oekb.client.OeKBClient`
- `fondant.oekb.models`

The client calls:

- `/steuerMeldung/liste`
- `/steuerMeldung/stmId/{stm_id}/ertrStBeh`

It uses:

- Async httpx client.
- Configurable base URL.
- Configurable request timeout.
- Simple per-client rate limiting.
- German OeKB response headers.

### OeKB Parser

Implemented in:

- `fondant.oekb.parser`

The parser converts flexible nested OeKB payloads into a normalized internal tax matrix. It recursively walks dictionaries and lists, maps known `steuerName` values to metric keys, maps category field names to investor categories, and converts numeric strings to `Decimal`.

Implemented tax metrics:

| Code | Metric key | Meaning |
|---|---|---|
| `K40` | `steuerpflichtige_einkuenfte` | Taxable income |
| `K11` | `ag_ertraege` | Distributed income / AGErtraege |
| `K12` | `korrekturbetrag_saldiert` | Net correction amount |
| `K81` | `kest_total` | Total withholding tax |
| `K82` | `kest_substanzgewinne` | Withholding tax on substance gains |
| `K10` | `substanzgewinne_kestpfl` | Taxable substance gains |
| `K55` | `fondsergebnis_nichtausg` | Undistributed fund result |
| `K61` | `korrekturbetrag_age_ak` | Cost-basis adjustment |
| `K62` | `korrekturbetrag_aussch_ak` | Distribution cost-basis adjustment |
| `K36` | `substanzgew_folgejahre` | Substance gain in following years |
| `K21` | `quellensteuern_einbeh` | Retained withholding taxes |

Implemented investor categories:

| Code | Key | Meaning |
|---|---|---|
| `PVM` | `pv_mit` | Private assets with option |
| `PVO` | `pv_ohne` | Private assets without option |
| `BVM` | `bv_mit` | Business assets with option |
| `BVO` | `bv_ohne` | Business assets without option |
| `BVJ` | `bv_jur` | Business assets for legal entities |
| `STF` | `stiftung` | Foundation |

### Ingestion Pipeline

Implemented in:

- `fondant.ingestion.pipeline`

Primary functions:

- `ingest_isin(isin)`
- `ingest_many(isins)`

Pipeline behavior:

1. Creates an `IMPLOG` run entry.
2. Ensures `TAXLIN` and `TAXCAT` dictionaries exist.
3. Fetches the OeKB report list for an ISIN.
4. Upserts a `SECMDA` security master row.
5. Upserts `SOURCERPT` metadata for every returned report.
6. Fetches report details only for reports where `status_code == "FIN"`.
7. Checks whether the source detail should be persisted:
   - Persist if no raw row exists.
   - Persist if the incoming version is newer.
   - Skip if the incoming version is older.
   - Persist same-version payloads if the payload changed.
   - Skip same-version payloads if unchanged.
8. Writes `SOURCERAW` and `SOURCEAGE`.
9. Curates source rows into `TAXRPT`, `TAXDAT`, `TAXADJ`, `SECDIV`, and `TAXCOR`.
10. Updates `IMPLOG` with success or writes `IMPERR` on failure.

The ingestion process is idempotent for unchanged data and detects changed same-version payloads.

### Curated Data Build

For each persisted final report, `_curate_report`:

- Copies report metadata from `SOURCERPT` to `TAXRPT`.
- Deletes and rebuilds `TAXDAT` and `TAXADJ` for that report.
- Expands source values by metric and investor category into narrow `TAXDAT` rows.
- Writes `TAXADJ` rows for `K61` values using adjustment code `AKC`.
- Writes `SECDIV` distribution events when OeKB indicates a distribution report and provides `zufluss`.
- Links corrections in `TAXCOR` when `korrigierte_stm_id` points to an existing report.

### ISIN Jobs

Implemented in:

- `fondant.jobs.fetch_missing_isins`
- `fondant.jobs.refresh_existing_isins`
- `fondant.jobs.isin_storage`

ISIN storage:

- Default path: `Documentation/isin_storage.csv`
- Expected column: `ISIN`
- ISIN validation is structural: two letters followed by ten alphanumeric characters.

Fetch only missing ISINs:

```bash
python -m fondant.jobs.fetch_missing_isins --dry-run --show-isins
python -m fondant.jobs.fetch_missing_isins
```

Add one ISIN and persist it to storage:

```bash
python -m fondant.jobs.fetch_missing_isins --isin IE00BMTX1Y45 --persist-input
```

Refresh already ingested ISINs:

```bash
python -m fondant.jobs.refresh_existing_isins --dry-run --show-isins
python -m fondant.jobs.refresh_existing_isins
```

### ECB FX Pipeline

Implemented in:

- `fondant.ecb.client`
- `fondant.ingestion.fx_pipeline`

Default currencies:

- `USD`
- `GBP`
- `CHF`

Backfill historical rates:

```bash
python - <<'PY'
import asyncio
from fondant.ingestion.fx_pipeline import backfill_ecb_rates
print(asyncio.run(backfill_ecb_rates()))
PY
```

Fetch latest available rates:

```bash
python - <<'PY'
import asyncio
from fondant.ingestion.fx_pipeline import fetch_latest_ecb_rates
print(asyncio.run(fetch_latest_ecb_rates()))
PY
```

`fetch_latest_ecb_rates` requests a lookback window because ECB rates are not published on every calendar day.

### Database Migrations

Alembic migrations define the database schema. The current head tested by the repository is:

```text
20260419_0011
```

Important final objects:

- Tables: `SECMDA`, `SECDIV`, `SOURCERPT`, `SOURCEAGE`, `SOURCERAW`, `TAXRPT`, `TAXDAT`, `TAXADJ`, `TAXLIN`, `TAXCAT`, `TAXCOR`, `REFCCY`, `REFCTR`, `REFEXC`, `IMPLOG`, `IMPERR`
- Views: `V1_TAXDATPRE`, `V2_TAXDATEUR`

### Tests

The test suite covers:

- OeKB parser mappings.
- OeKB client request shape.
- ECB CSV parsing and FX ingestion.
- Main ingestion behavior and idempotency.
- Same-version changed payload updates.
- Missing-ISIN and refresh job selection logic.
- API response behavior and null-year fallback.
- Alembic fresh-install migrations on SQLite and, when Docker is available, PostgreSQL.

Run tests:

```bash
pytest
```

## Local Runtime Flow

1. Install dependencies.
2. Start PostgreSQL.
3. Run Alembic migrations.
4. Ingest ISINs.
5. Optionally backfill ECB FX rates.
6. Start the API.
7. Query through SQL, API, or downstream tools.

Commands:

```bash
pip install -e ".[dev]"
docker compose up -d
alembic upgrade head
python -m fondant.jobs.fetch_missing_isins --dry-run --show-isins
python -m fondant.jobs.fetch_missing_isins
uvicorn fondant.api.main:app --reload
```

## Technical Limitations

- The OeKB parser maps known tax fields only. New or renamed OeKB fields require parser updates.
- The public API exposes one business endpoint.
- No built-in authentication, authorization, or tenant separation.
- No bundled scheduler; recurring ingestion must be handled externally.
- `V2_TAXDATEUR` uses exact-date FX matching.
- The project is backend-focused and does not include a production UI.
