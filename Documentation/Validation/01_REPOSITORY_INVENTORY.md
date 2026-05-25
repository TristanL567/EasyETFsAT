# EV-001 Repository Inventory

## Scope

This inventory is a source-backed discovery artifact for validation planning. It records what was read in the EasyETFsAT repository and avoids architecture critique, entropy critique, implementation changes, live ingestion, or network-dependent jobs.

## Repository State Evidence

- Current branch/status command read: `git status --short --branch`.
- Observed branch: `main...origin/main`.
- Dirty worktree summary at inventory time:
  - Untracked: `Documentation/DATA_AND_QUERY_GUIDE.md`
  - Untracked: `Documentation/README.md`
  - Untracked: `Documentation/STAKEHOLDER_BRIEF.md`
  - Untracked: `Documentation/TECHNICAL_ARCHITECTURE.md`
  - Untracked: `Documentation/Validation/`
- File inventory command read: `rg --files`.
- No application code, migrations, tests, environment files, or existing documentation outside `Documentation\Validation` were edited for this ticket.

## Major Directories And Responsibilities

| Path | Responsibility shown by source files |
|---|---|
| `fondant\api\` | FastAPI application factory and HTTP routes. Evidence: `fondant\api\main.py`, `fondant\api\routes\health.py`, `fondant\api\routes\etf.py`. |
| `fondant\config.py` | Runtime settings loaded through Pydantic settings, including database URL, OeKB and ECB base URLs, timeouts, rate limits, and log level. |
| `fondant\db\` | SQLAlchemy async session setup, declarative base, and ORM model exports. Evidence: `fondant\db\session.py`, `fondant\db\base.py`, `fondant\db\models\*.py`. |
| `fondant\oekb\` | OeKB HTTP client, Pydantic payload models, and parser from OeKB payloads into internal report and tax matrix values. |
| `fondant\ecb\` | ECB HTTP client and rate point model for CSV reference-rate responses. |
| `fondant\ingestion\` | OeKB ingestion pipeline, source-to-curated persistence, seed ISIN list, and ECB FX ingestion functions. |
| `fondant\jobs\` | CLI job entry points for fetching missing ISINs, refreshing existing ISINs, and maintaining `Documentation\isin_storage.csv`. |
| `alembic\` | Alembic migration environment and versioned database migrations. Current migration chain reaches revision `20260419_0011` per migration files and migration tests. |
| `tests\` | Pytest suite covering API, OeKB client/parser, ECB client/FX pipeline, ingestion, jobs, and migrations. |
| `docs\` | Database naming dictionary and table catalog. Evidence: `docs\db_naming_dictionary.md`, `docs\db_table_catalog.md`. |
| `Documentation\` | Stakeholder, technical, data/query, operational, sample usage, ISIN storage, and validation documentation. |
| `scripts\` | SQL schema verification helper. Evidence: `scripts\verify_schema.sql`. |

## Runtime Stack

Source evidence: `pyproject.toml`, `README.md`, `docker-compose.yml`, `fondant\config.py`, `fondant\api\main.py`.

- Python package: `easyetfsat`, version `0.1.0`, requires Python `>=3.11`.
- Web API: FastAPI app created by `fondant.api.main:create_app`; runtime object is `fondant.api.main:app`.
- ASGI server documented in README: `uvicorn fondant.api.main:app --reload`.
- Database access: SQLAlchemy 2 async engine/session using `AsyncSessionFactory`.
- Settings: `pydantic-settings` with defaults for `database_url`, `oekb_base_url`, `ecb_base_url`, log level, rate limits, and timeouts.
- HTTP clients: `httpx.AsyncClient` used by both OeKB and ECB clients.
- Logging: standard `logging` plus `structlog`.

## Database Stack

Source evidence: `docker-compose.yml`, `alembic.ini`, `alembic\env.py`, `alembic\versions\*.py`, `fondant\db\models\*.py`, `docs\db_table_catalog.md`, `tests\test_migrations.py`.

- Local database service: PostgreSQL 16 via Docker Compose service `postgres`.
- Default database/user/password in Compose: `easyetfsat` / `easyetfsat` / `easyetfsat`.
- Default async SQLAlchemy URL in code: `postgresql+asyncpg://easyetfsat:easyetfsat@localhost:5432/easyetfsat`.
- Alembic uses the configured database URL converted to a synchronous driver form by `Settings.alembic_database_url`.
- ORM model groups:
  - Security: `SECMDA`, `SECDIV`.
  - Source tax/report data: `SOURCERPT`, `SOURCEAGE`, `SOURCERAW`.
  - Curated tax data: `TAXRPT`, `TAXLIN`, `TAXCAT`, `TAXDAT`, `TAXADJ`, `TAXCOR`.
  - Reference: `REFCCY`, `REFCTR`, `REFEXC`.
  - Import operations: `IMPLOG`, `IMPERR`.
- Migration files define a chain from `20260418_0001_initial_schema.py` through `20260419_0011_refine_v1_and_add_v2_taxdateur.py`.
- Database views named in docs and tests: `V1_TAXDATPRE`, `V2_TAXDATEUR`.

## API And Operational Entry Points

Source evidence: `README.md`, `fondant\api\routes\health.py`, `fondant\api\routes\etf.py`, `fondant\jobs\fetch_missing_isins.py`, `fondant\jobs\refresh_existing_isins.py`, `fondant\ingestion\fx_pipeline.py`, `Documentation\AgentInstructions\*.md`.

- API health endpoint: `GET /health`.
- API tax endpoint: `GET /etf/{isin}/tax?year={year}`.
- API endpoint backing tables in code: `TAXRPT`, `TAXDAT`, `TAXLIN`, `TAXCAT`.
- Missing-ISIN ingestion job:
  - `python -m fondant.jobs.fetch_missing_isins --dry-run --show-isins`
  - `python -m fondant.jobs.fetch_missing_isins`
- Existing-ISIN refresh job:
  - `python -m fondant.jobs.refresh_existing_isins --dry-run --show-isins`
  - `python -m fondant.jobs.refresh_existing_isins`
- ISIN storage path used by jobs: `Documentation\isin_storage.csv`.
- ECB FX functions documented in README and implemented in `fondant\ingestion\fx_pipeline.py`:
  - `backfill_ecb_rates`
  - `fetch_latest_ecb_rates`
- Schema verification helper: `scripts\verify_schema.sql`.

## Test Stack And Test Files

Source evidence: `pyproject.toml`, `tests\conftest.py`, `tests\test_*.py`.

- Test runner: pytest with `addopts = "-q"` and `testpaths = ["tests"]`.
- Async test support: `pytest-asyncio` with `asyncio_mode = "auto"`.
- HTTP mocking: `respx` used by OeKB and ECB client tests.
- SQLite test support: `aiosqlite`.
- PostgreSQL migration test support: `testcontainers[postgres]`; Docker-dependent tests skip when Docker is unavailable.
- Test files present:
  - `tests\test_api_etf.py`: ETF API response behavior and null-year fallback.
  - `tests\test_ecb_client.py`: ECB CSV parsing/client request behavior.
  - `tests\test_fx_pipeline.py`: ECB FX ingestion and latest-rate selection.
  - `tests\test_ingestion.py`: OeKB ingestion behavior, idempotency, and same-version payload updates.
  - `tests\test_jobs_isin_workflows.py`: missing-ISIN and refresh job selection/storage workflows.
  - `tests\test_migrations.py`: fresh migration install checks for SQLite and PostgreSQL when Docker is available.
  - `tests\test_oekb_client.py`: OeKB client request shape and detail fetch behavior.
  - `tests\test_oekb_parser.py`: parser mapping for selected tax metrics and categories.
  - `tests\conftest.py`: resets cached settings around tests.

## Documentation And Agent Instructions

Source evidence: `README.md`, `docs\*.md`, `Documentation\*.md`, `Documentation\AgentInstructions\*.md`, `Documentation\Validation\REPO_VALIDATION_EPIC.md`, `Documentation\Validation\EV-001_MASTER_DISPATCH.md`.

- Root README: quick start, DB connection, core endpoint, FX commands, migration tests, seed ISINs, and ISIN job commands.
- `docs\db_naming_dictionary.md`: physical naming rules, prefixes, tokens, table set, and legacy mapping.
- `docs\db_table_catalog.md`: table/view catalog with purposes, grain, keys, and fields.
- `Documentation\README.md`: recommended documentation reading order and one-sentence project summary.
- `Documentation\TECHNICAL_ARCHITECTURE.md`: technical description of stack, configuration, components, jobs, migrations, tests, runtime flow, and limitations.
- `Documentation\DATA_AND_QUERY_GUIDE.md`: data layers, table summaries, SQL examples, API query examples, ingestion commands, and data-consumer notes.
- `Documentation\STAKEHOLDER_BRIEF.md`: stakeholder-facing summary, use cases, value, gaps, and positioning.
- `Documentation\AgentInstructions\FETCH_ONLY_MISSING_ISINS.md`: operational steps for missing-ISIN ingestion.
- `Documentation\AgentInstructions\REFRESH_EXISTING_ISINS.md`: operational steps for refreshing existing ISINs.
- `Documentation\Validation\REPO_VALIDATION_EPIC.md`: validation epic and ticket backlog; EV-001 is the only ticket executed here.
- `Documentation\Validation\EV-001_MASTER_DISPATCH.md`: dispatch prompt and expected EV-001 worker output.

## Source Files Read As Evidence

- Repository and project metadata: `README.md`, `pyproject.toml`, `docker-compose.yml`, `alembic.ini`.
- Database and migrations: `alembic\env.py`, `alembic\versions\*.py`, `fondant\db\base.py`, `fondant\db\session.py`, `fondant\db\models\*.py`.
- API: `fondant\api\main.py`, `fondant\api\routes\health.py`, `fondant\api\routes\etf.py`.
- OeKB and ECB integrations: `fondant\oekb\client.py`, `fondant\oekb\models.py`, `fondant\oekb\parser.py`, `fondant\ecb\client.py`, `fondant\ecb\models.py`.
- Ingestion and jobs: `fondant\ingestion\pipeline.py`, `fondant\ingestion\fx_pipeline.py`, `fondant\ingestion\seed.py`, `fondant\jobs\fetch_missing_isins.py`, `fondant\jobs\refresh_existing_isins.py`, `fondant\jobs\isin_storage.py`.
- Tests: `tests\conftest.py`, `tests\test_*.py`.
- Documentation and operations: `docs\*.md`, `Documentation\README.md`, `Documentation\TECHNICAL_ARCHITECTURE.md`, `Documentation\DATA_AND_QUERY_GUIDE.md`, `Documentation\STAKEHOLDER_BRIEF.md`, `Documentation\AgentInstructions\*.md`, `scripts\verify_schema.sql`.
- Validation ticket context: `Documentation\Validation\REPO_VALIDATION_EPIC.md`, `Documentation\Validation\EV-001_MASTER_DISPATCH.md`.

## Validation Planning Focus Areas

These are factual areas future validation tickets can inspect further without changing EV-001 scope:

- API behavior and contract around `/health` and `/etf/{isin}/tax`.
- OeKB list/detail client and parser mapping from source payloads to internal tax metrics.
- Ingestion flow from OeKB reports to source tables and curated tables.
- ECB FX ingestion and `REFEXC`-backed view usage.
- Alembic migration chain and database object expectations.
- Test coverage evidence by behavior area.
- Operational runbooks for missing and existing ISIN ingestion.

## Manual Verification Required

Per the ticket envelope, a human should review this file for factual completeness before EV-001 is accepted.
