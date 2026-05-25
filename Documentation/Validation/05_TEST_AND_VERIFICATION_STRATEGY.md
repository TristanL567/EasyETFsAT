# EV-005 Test And Verification Strategy

## Scope

This report validates what the current EasyETFsAT test suite covers, what it does not cover, and which verification commands should be trusted for future tickets. It is documentation-only. No tests, application code, migrations, configuration, environment files, or existing documentation were changed.

Source evidence was read from `tests\test_*.py`, `tests\conftest.py`, `pyproject.toml`, `README.md`, `Documentation\*.md`, and prior validation artifacts under `Documentation\Validation`.

## Verification Command Results

| Command | Result | Notes |
|---|---|---|
| `git status --short --branch` | Passed before edits | Repository was on `development`. Existing unrelated untracked docs were present: `Documentation/DATA_AND_QUERY_GUIDE.md`, `Documentation/README.md`, `Documentation/STAKEHOLDER_BRIEF.md`, and `Documentation/TECHNICAL_ARCHITECTURE.md`. |
| `py -3.10 -m pytest --collect-only` | Passed | Collected 17 tests across 8 `tests\test_*.py` files. A local `RequestsDependencyWarning` about `urllib3` and `chardet`/`charset_normalizer` compatibility was emitted during collection. |
| `py -3.10 -m ruff check .` | Passed | Reported `All checks passed!`. |

## Test Stack

| Area | Evidence | Meaning for future tickets |
|---|---|---|
| Test runner | `pyproject.toml` configures `pytest` with `testpaths = ["tests"]` and quiet output. | Use `py -3.10 -m pytest ...` locally; prior validation notes indicate bare `pytest` may not be on PATH in this environment. |
| Async tests | `pytest-asyncio` is configured with `asyncio_mode = "auto"`. | Async API, client, ingestion, job, and FX tests can run without per-test event-loop setup. |
| HTTP mocks | `respx` is used by OeKB and ECB client tests. | Client tests do not make live OeKB or ECB calls. |
| Database tests | Most behavioral tests use in-memory SQLite with `aiosqlite` and SQLAlchemy `StaticPool`. | Fast local tests protect repository logic, but not every PostgreSQL-specific behavior. |
| PostgreSQL migration tests | `tests\test_migrations.py` uses `testcontainers[postgres]` and Docker when available. | Production-like migration validation is slower and may skip when Docker or testcontainers are unavailable. |
| Settings isolation | `tests\conftest.py` clears cached settings before and after each test. | Environment overrides in tests are less likely to leak across files. |

## Coverage Map By Test File

| Test file | Tests collected | Behavior covered | Trust level |
|---|---:|---|---|
| `tests\test_api_etf.py` | 3 | FastAPI `/etf/{isin}/tax` success response, 404 for missing year, uppercase ISIN normalization through route use, tax field grouping, and null-year fallback flag. Uses an in-memory SQLite database and ASGI transport. | Good for current happy-path and fallback behavior; narrow for public API contract maturity. |
| `tests\test_ecb_client.py` | 1 | ECB client request path for daily EXR CSV, sorted currency key behavior, query parameters, and CSV parsing into `ECBRatePoint` objects. Uses `respx`; no live ECB network call. | Good for request/parse shape; does not validate live ECB schema drift or error handling. |
| `tests\test_fx_pipeline.py` | 2 | ECB FX backfill upsert into `REFEXC` and latest-rate selection per currency over a lookback window. Uses a fake ECB client and in-memory SQLite. | Good for pipeline write logic; does not validate exact-date FX view fallback behavior. |
| `tests\test_ingestion.py` | 2 | OeKB ingestion idempotency, import log counts, source table writes, curated table writes, dictionary seeding, non-FIN metadata handling, and same-version changed-payload updates. Uses a fake OeKB client and in-memory SQLite. | Strong local characterization of core ingestion behavior; does not exercise live OeKB, pagination, PostgreSQL-specific DDL, or full parser breadth. |
| `tests\test_jobs_isin_workflows.py` | 3 | Missing-ISIN dry-run candidate selection, `--persist-input` storage update, refresh-existing filtering, missing requested ISIN reporting, and console summary fragments. Uses monkeypatched dependencies and in-memory SQLite where needed. | Good for selection and dry-run behavior; limited for operational job failure modes and real CLI invocation. |
| `tests\test_migrations.py` | 2 | Alembic fresh install to revision `20260419_0011`, expected source/curated/reference/import tables, obsolete table absence, view presence, key columns, and SQLite plus Docker-backed PostgreSQL when available. | Strongest schema check; PostgreSQL path is conditional and may skip without Docker. |
| `tests\test_oekb_client.py` | 2 | OeKB list/detail request URLs, required headers, list query parameters, and response parsing for list/detail payloads. Uses `respx`; no live OeKB network call. | Good for current request shape; does not prove live endpoint availability, auth changes, throttling behavior, or pagination coverage. |
| `tests\test_oekb_parser.py` | 2 | Selected `SOURCEAGE` mappings for `bvJurPerson4`, `stiftung4`, K40, K62, and taxable-income K-code values with decimal conversion. | Useful mapping smoke coverage; incomplete for unmapped fields, malformed values, unexpected nested shapes, and parser diagnostics. |

## Behavior Coverage By System Area

| System area | Current protection | Main gaps |
|---|---|---|
| Parser | Selected tax-name/category mappings are asserted in `tests\test_oekb_parser.py`. Ingestion tests also prove selected parsed values flow into source and curated tables. | No negative tests for unknown `steuerName`, unknown category suffixes, malformed numeric values, or complete OeKB tax matrix coverage. |
| OeKB client | Request headers, list parameters, detail URL, and basic response parsing are mocked. | No live integration checks, retry/error/rate-limit tests, timeout behavior, endpoint schema drift checks, or report-list pagination tests. |
| Ingestion | Idempotency, changed same-version payloads, source writes, curated writes, dictionary seeding, and import logs are covered. | No PostgreSQL ingestion run, no live OeKB ingestion, no systemic failure classification, no correction-chain or distribution-event focused tests, and no multi-page list behavior. |
| Operational jobs | Dry-run selection, storage persistence, and refresh filtering are covered at function level. | No subprocess CLI smoke tests, no live job runs, no failure summary tests, no scheduler integration, no monitoring/alerting checks, and no external scheduler contract. |
| API | Current tax route success, 404, and null-year fallback are covered. | No `/health` test, OpenAPI snapshot/contract test, auth/security test, invalid ISIN contract test, decimal precision decision test, multi-report ordering test, or backward compatibility contract. |
| FX and ECB | ECB CSV parsing, FX backfill upsert, and latest-per-currency selection are covered with mocks/fakes. | No live ECB check, no malformed CSV/error tests, no weekend/holiday exact-date fallback tests, and no view-level test proving non-EUR values become `NULL` when exact-date FX is missing. |
| Migrations/schema | Fresh install is checked on SQLite and PostgreSQL when Docker/testcontainers are available. Views and important columns are asserted. | PostgreSQL validation can skip locally; no downgrade tests; no migration-from-existing-data scenario; limited view semantic assertions beyond column presence. |
| Auth/security | Stakeholder and architecture docs state no auth/authorization layer exists. | No tests because there is no implemented auth/security boundary. Future auth work must add tests before trusting API exposure. |
| Scheduler absence | Stakeholder and architecture docs state no bundled scheduler exists; jobs are manual or externally scheduled. | No scheduler tests because no scheduler exists. Operational scheduling should be treated as future infrastructure/product work, not current backend behavior. |

## Fast Local Checks

Use these commands for quick feedback when Docker, live services, and local PostgreSQL are not required:

| Change type | Recommended command |
|---|---|
| Confirm branch and dirty state | `git status --short --branch` |
| Test discovery and import sanity | `py -3.10 -m pytest --collect-only` |
| Lint all tracked Python code and docs-adjacent imports | `py -3.10 -m ruff check .` |
| API route behavior | `py -3.10 -m pytest tests/test_api_etf.py` |
| OeKB parser mappings | `py -3.10 -m pytest tests/test_oekb_parser.py` |
| OeKB client request shape | `py -3.10 -m pytest tests/test_oekb_client.py` |
| OeKB ingestion behavior | `py -3.10 -m pytest tests/test_ingestion.py` |
| ECB client and FX pipeline | `py -3.10 -m pytest tests/test_ecb_client.py tests/test_fx_pipeline.py` |
| ISIN job selection and dry-run behavior | `py -3.10 -m pytest tests/test_jobs_isin_workflows.py` |

## Slower Or Container Checks

| Check | Command | When to trust it |
|---|---|---|
| Full local test suite | `py -3.10 -m pytest` | Good pre-handoff command for broad backend changes. It may include Docker-gated PostgreSQL migration coverage only if Docker/testcontainers can run. |
| Migration-specific checks | `py -3.10 -m pytest tests/test_migrations.py` | Required for schema, model, Alembic, or view work. Treat a PostgreSQL skip as residual risk, not as full production-like validation. |
| Docker availability probe | `docker info` | Useful before relying on PostgreSQL migration tests. Do not treat it as a product test; it only confirms the local container runtime is reachable. |

## Skipped Or Unavailable Checks And Residual Risk

- No live OeKB or ECB calls were run for EV-005. This is intentional per ticket scope. Residual risk: external endpoint availability, schema drift, throttling, and production response edge cases are not validated by the local suite.
- `py -3.10 -m pytest --collect-only` does not execute tests. It proves collection/import viability for 17 tests, not behavior correctness.
- Docker-backed PostgreSQL migration execution was not run by EV-005 because the ticket required collection and lint checks only. Residual risk remains until `py -3.10 -m pytest tests/test_migrations.py` executes in an environment where Docker/testcontainers are available and the PostgreSQL test does not skip.
- Auth/security cannot be validated by current tests because the project currently documents no authentication or authorization layer.
- Scheduler behavior cannot be validated by current tests because the project currently documents no bundled scheduler; ingestion is manual or externally scheduled.
- API contract maturity remains limited because tests assert selected response examples rather than a durable OpenAPI or consumer compatibility contract.
- FX fallback behavior remains a known gap: current docs describe exact-date FX matching for `V2_TAXDATEUR`, while tests cover ingestion of latest rates but not view-level missing-rate behavior or nearest-prior fallback semantics.

## Future Test Tickets

| Draft ticket | Goal | Suggested verification |
|---|---|---|
| EV-FUT-TST-001 Parser Negative Coverage | Add tests for unknown OeKB tax fields, unknown category suffixes, malformed numeric values, and parser diagnostics or explicitly documented silent-drop behavior. | `py -3.10 -m pytest tests/test_oekb_parser.py tests/test_ingestion.py`; `py -3.10 -m ruff check fondant/oekb tests/test_oekb_parser.py` |
| EV-FUT-TST-002 API Contract Hardening | Add tests for `/health`, invalid ISIN handling, OpenAPI/response contract stability, decimal precision expectations, and multi-report ordering. | `py -3.10 -m pytest tests/test_api_etf.py`; `py -3.10 -m ruff check fondant/api tests/test_api_etf.py` |
| EV-FUT-TST-003 Operational Job Failure Coverage | Add tests for CLI-level invocation, mixed success/failure summaries, all-failure batch behavior, and exit-code expectations. | `py -3.10 -m pytest tests/test_jobs_isin_workflows.py tests/test_ingestion.py`; `py -3.10 -m ruff check fondant/jobs fondant/ingestion tests` |
| EV-FUT-TST-004 OeKB Pagination And Error Handling | Characterize report-list pagination and HTTP error/timeout handling with mocked responses before any live ingestion. | `py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py`; `py -3.10 -m ruff check fondant/oekb fondant/ingestion tests` |
| EV-FUT-TST-005 FX View Semantics | Add migration/view tests for exact-date FX matching, missing FX rows, EUR rate `1`, and any future nearest-prior fallback decision. | `py -3.10 -m pytest tests/test_migrations.py tests/test_fx_pipeline.py`; require non-skipped PostgreSQL migration validation when view SQL changes. |
| EV-FUT-TST-006 Auth And Scheduler Decision Tests | If auth or a scheduler is introduced, add tests that define the boundary before exposing or automating it. | Auth: API tests plus security-specific checks. Scheduler: job invocation tests plus scheduler adapter tests. |

## Recommended Verification Policy For Future Tickets

1. Start every ticket with `git status --short --branch` and preserve unrelated dirty worktree entries.
2. Run `py -3.10 -m pytest --collect-only` when the ticket touches imports, tests, docs that reference commands, or dependency assumptions.
3. Run the narrowest relevant test file first, based on the coverage map above.
4. Add `py -3.10 -m ruff check .` for any code or test change; for documentation-only validation tickets, it is still a useful repository health check when requested.
5. Run `py -3.10 -m pytest` before handoff for cross-module backend changes.
6. Require `py -3.10 -m pytest tests/test_migrations.py` for migration, ORM model, table/view, or FX-view work. Record whether the PostgreSQL test ran or skipped.
7. Do not treat mocked OeKB/ECB tests as live integration certification. Live network checks should remain separate, explicit, and operator-approved.

## Source Evidence Index

- Test files: `tests\test_api_etf.py`, `tests\test_ecb_client.py`, `tests\test_fx_pipeline.py`, `tests\test_ingestion.py`, `tests\test_jobs_isin_workflows.py`, `tests\test_migrations.py`, `tests\test_oekb_client.py`, `tests\test_oekb_parser.py`, `tests\conftest.py`.
- Project config: `pyproject.toml`.
- Operational docs: `README.md`, `Documentation\TECHNICAL_ARCHITECTURE.md`, `Documentation\DATA_AND_QUERY_GUIDE.md`, `Documentation\STAKEHOLDER_BRIEF.md`, `Documentation\AgentInstructions\FETCH_ONLY_MISSING_ISINS.md`, `Documentation\AgentInstructions\REFRESH_EXISTING_ISINS.md`.
- Prior validation: `Documentation\Validation\01_REPOSITORY_INVENTORY.md`, `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md`, `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md`, `Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md`.

## Scope Validation Notes

- Ticket executed: EV-005 only.
- Produced artifact: `Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md`.
- No tests, application code, migrations, environment files, existing documentation outside this new validation artifact, staging, commits, pushes, PRs, live network calls, or EV-006 work were performed.
- Manual verification is not required by the EV-005 ticket envelope.
