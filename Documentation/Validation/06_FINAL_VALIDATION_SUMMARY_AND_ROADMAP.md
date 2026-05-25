# EV-006 Final Validation Summary And Follow-Up Roadmap

## Purpose

This document closes the repository validation epic by consolidating EV-001
through EV-005 into one decision artifact. It summarizes what EasyETFsAT does,
how the main components relate, what validation artifacts were created, which
risks remain, and which small follow-up tickets should be considered next.

This is a planning and summary artifact only. No application code, migrations,
tests, environment files, existing project documentation, external tickets,
staging, commits, pushes, or pull requests were changed or created.

## Source Validation Artifacts

| Ticket | Artifact | Validation status |
|---|---|---|
| EV-001 | `Documentation\Validation\01_REPOSITORY_INVENTORY.md` | Accepted. Repository layout, runtime stack, database stack, test stack, docs, and operational entry points were inventoried. |
| EV-002 | `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md` | Accepted. Architecture, APIs, ingestion, FX, jobs, database relationships, migrations, and tests were mapped. |
| EV-003 | `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md` | Accepted. Maintainability and entropy risks were ranked with evidence and draft follow-up tickets. |
| EV-004 | `Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md` | Accepted. Repository reading order, authoritative docs, common change areas, and safe validation commands were documented. |
| EV-005 | `Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md` | Accepted. Test coverage, trusted commands, skipped checks, and targeted future test tickets were documented. |

## What The Repository Does

EasyETFsAT is a Python/FastAPI service and data pipeline for Austrian ETF tax
data. It ingests OeKB tax-report data by ISIN, preserves source payloads and
source-shaped parsed values, curates those values into reporting tables, exposes
tax data through an HTTP API, and supports ECB FX-rate ingestion for EUR
conversion views.

The repository also includes operational jobs for missing-ISIN ingestion and
existing-ISIN refreshes, Alembic migrations for the database schema and views,
and a pytest suite covering API behavior, OeKB and ECB clients, parser mapping,
ingestion, operational job selection, FX ingestion, and migration installation.

## Main Component Relationships

| Area | Current role |
|---|---|
| API | `fondant\api` exposes `GET /health` and `GET /etf/{isin}/tax`. The tax route reads curated tax tables directly and does not call OeKB, ECB, ingestion jobs, or database views at request time. |
| OeKB integration | `fondant\oekb` fetches list/detail payloads and maps known OeKB tax fields and categories into internal source values. |
| Ingestion | `fondant\ingestion\pipeline.py` orchestrates OeKB list/detail retrieval, import logging, source persistence, dictionary seeding, source-to-curated curation, correction links, and distribution events. |
| ECB FX | `fondant\ecb` and `fondant\ingestion\fx_pipeline.py` fetch and upsert ECB reference rates into `REFEXC`. |
| Jobs | `fondant\jobs` selects ISINs from storage or existing source records, supports dry-run modes, and calls `ingest_many`. |
| Database | SQLAlchemy models and Alembic migrations define source tables, curated tax tables, reference tables, import observability tables, and projection views. |
| Tests | `tests\test_*.py` provides fast local characterization for core behavior, with PostgreSQL migration validation conditional on Docker/testcontainers availability. |

## Current Validation Posture

The repository has a clear implementation shape and enough local tests to make
small targeted changes reviewable. The strongest covered areas are basic API
response behavior, OeKB/ECB request shape, selected parser mappings, ingestion
idempotency and curation flow, ISIN job selection, FX upserts, and fresh
migration installation.

The main validation weakness is not absence of tests, but uneven coverage at
the system trust boundaries: unknown OeKB payload shapes, report-list
pagination, API contract maturity, FX view semantics, operational failure
diagnostics, and PostgreSQL-specific migration behavior when container tests
skip.

## Prioritization Method

Follow-up work is ranked by:

| Factor | Meaning |
|---|---|
| Risk reduction | How much the ticket reduces correctness, schema/API, operational, or maintenance risk. |
| Stakeholder value | How directly the ticket improves operator confidence, data-consumer trust, or future developer speed. |
| Implementation complexity | How small and independently reviewable the ticket is. Lower complexity is better. |

Recommended order favors high risk reduction with low or moderate complexity.
Broad rewrites are intentionally avoided.

## Prioritized Roadmap

| Rank | Category | Draft ticket | Risk reduction | Stakeholder value | Complexity | Why this comes next |
|---:|---|---|---|---|---|---|
| 1 | Test-only | EV-FUP-TST-001 Parser Negative Coverage | High | High | Low | The parser is the source-to-curated trust boundary, and unknown fields/categories or malformed values can be silently dropped. Characterization tests can be added before changing behavior. |
| 2 | Backend | EV-FUP-BE-001 Parser Diagnostics | High | High | Medium | After tests characterize current drops, make unmapped tax fields, unknown categories, and invalid numeric values observable without changing schema. |
| 3 | Test-only | EV-FUP-TST-002 OeKB Pagination Characterization | High | Medium | Low | Ingestion currently appears to read one OeKB report-list page. A mocked test can confirm the risk before implementation. |
| 4 | Backend | EV-FUP-BE-002 OeKB Report-List Pagination | High | Medium | Medium | If pagination risk is confirmed, retrieve all expected report-list pages while preserving no-live-ingestion boundaries in tests. |
| 5 | Test-only | EV-FUP-TST-003 API Contract Hardening | Medium | High | Low | The public tax API needs stronger tests for `/health`, invalid ISINs, multi-report ordering, null dates, OpenAPI/response shape, and numeric precision expectations. |
| 6 | Product/API | EV-FUP-API-001 Decimal And API Contract Decision | Medium | High | Low | Decide whether tax amounts should remain JSON floats or use decimal-safe strings before any response-shape change. |
| 7 | Product/API | EV-FUP-API-002 FX Conversion Semantics Decision | Medium | High | Low | Non-EUR EUR-converted view values currently depend on exact-date FX rows. A product/data decision should precede view or API changes. |
| 8 | Database | EV-FUP-DB-001 FX View Semantics Tests | Medium | Medium | Medium | Add migration/view tests for exact-date FX behavior, EUR rate `1`, missing FX rows, and any approved fallback semantics. |
| 9 | Operations | EV-FUP-OPS-001 Ingestion Batch Failure Summary | Medium | Medium | Medium | Operators need clearer distinction between isolated ISIN failures and systemic ingestion failures. |
| 10 | Test-only | EV-FUP-TST-004 Operational Job Failure Coverage | Medium | Medium | Low | Add tests for mixed/all-failure job summaries and exit-code expectations before changing operational output. |
| 11 | Backend | EV-FUP-BE-003 Tax Metric Registry Consistency Check | Medium | Medium | Medium | Reduce duplicated tax metric/category knowledge by adding a code-level registry or consistency check while preserving schema and API output. |
| 12 | Documentation-only | EV-FUP-DOC-001 PostgreSQL Migration Gate Guidance | Medium | Medium | Low | Document when Docker-backed PostgreSQL migration checks are required and how to record skips as residual risk. |
| 13 | Database | EV-FUP-DB-002 Required PostgreSQL Migration CI Gate | Medium | Medium | Medium | Promote production-like migration validation from optional local coverage to a reliable CI/release gate if project infrastructure allows. |
| 14 | Documentation-only | EV-FUP-DOC-002 Foundation Category Alias Decision Note | Low | Medium | Low | Record whether `STF` and `STI` aliases should remain for compatibility before any naming cleanup is attempted. |
| 15 | Operations | EV-FUP-OPS-002 Shared Job CLI Utilities | Low | Low | Low | If job behavior is touched later, extract only small shared ISIN sanitization or summary helpers with tests. Do not do this as a standalone broad cleanup. |

## Documentation-Only Follow-Ups

### EV-FUP-DOC-001 PostgreSQL Migration Gate Guidance

Goal: Document when PostgreSQL-backed migration verification is required and how
validators should record Docker/testcontainers skips as residual risk.

Allowed areas:

- `Documentation\Validation\`

Non-goals:

- CI configuration.
- Migration edits.
- Test edits.

Verification:

- `git status --short --branch`
- `rg --files Documentation\Validation`
- Human review for clarity.

### EV-FUP-DOC-002 Foundation Category Alias Decision Note

Goal: Record the compatibility decision for `stiftung`, `STF`, and `STI` naming
before any future cleanup.

Allowed areas:

- `Documentation\Validation\`

Non-goals:

- Renaming database columns.
- Editing migrations, views, parser maps, ingestion dictionaries, or public API
  fields.

Verification:

- Human review against EV-003 terminology evidence.

## Test-Only Follow-Ups

### EV-FUP-TST-001 Parser Negative Coverage

Goal: Add parser tests for unknown OeKB tax fields, unknown category suffixes,
malformed numeric values, and current silent-drop behavior or future diagnostics.

Allowed areas:

- `tests\`
- `Documentation\Validation\`

Non-goals:

- Parser behavior changes.
- Schema changes.
- Live OeKB calls.

Verification:

- `py -3.10 -m pytest tests/test_oekb_parser.py`
- `py -3.10 -m ruff check tests/test_oekb_parser.py`

### EV-FUP-TST-002 OeKB Pagination Characterization

Goal: Add mocked or fake-client tests that prove whether ingestion processes all
expected OeKB report-list pages.

Allowed areas:

- `tests\`
- `Documentation\Validation\`

Non-goals:

- OeKB client implementation changes.
- Ingestion implementation changes.
- Live OeKB calls.

Verification:

- `py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py`
- `py -3.10 -m ruff check tests`

### EV-FUP-TST-003 API Contract Hardening

Goal: Add focused API tests for `/health`, invalid ISIN handling, multiple
reports, null or tied report dates, OpenAPI/response shape, and decimal
precision expectations.

Allowed areas:

- `tests\`
- `Documentation\Validation\`

Non-goals:

- API response redesign.
- New endpoints.
- Schema changes.

Verification:

- `py -3.10 -m pytest tests/test_api_etf.py`
- `py -3.10 -m ruff check tests/test_api_etf.py`

### EV-FUP-TST-004 Operational Job Failure Coverage

Goal: Add tests for mixed success/failure, all-failure batch summaries, and
expected job exit-code behavior.

Allowed areas:

- `tests\`
- `Documentation\Validation\`

Non-goals:

- Job output changes.
- Ingestion behavior changes.
- Scheduler work.

Verification:

- `py -3.10 -m pytest tests/test_jobs_isin_workflows.py tests/test_ingestion.py`
- `py -3.10 -m ruff check tests`

## Backend Follow-Ups

### EV-FUP-BE-001 Parser Diagnostics

Goal: Make unmapped OeKB tax fields, unknown categories, and invalid numeric
values observable in parser or ingestion diagnostics while preserving existing
schema unless a later ticket explicitly changes it.

Allowed areas:

- `fondant\oekb\`
- `fondant\ingestion\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Database schema changes.
- Tax column renames.
- Live OeKB ingestion.
- Broad parser rewrite.

Verification:

- `py -3.10 -m pytest tests/test_oekb_parser.py tests/test_ingestion.py`
- `py -3.10 -m ruff check fondant/oekb fondant/ingestion tests`

### EV-FUP-BE-002 OeKB Report-List Pagination

Goal: Implement complete mocked report-list pagination for ingestion if
EV-FUP-TST-002 confirms current one-page behavior is insufficient.

Allowed areas:

- `fondant\oekb\`
- `fondant\ingestion\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Parser changes.
- Schema changes.
- Live OeKB calls.
- Scheduler changes.

Verification:

- `py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py`
- `py -3.10 -m ruff check fondant/oekb fondant/ingestion tests`

### EV-FUP-BE-003 Tax Metric Registry Consistency Check

Goal: Add a code-level consistency check or registry so parser metric/category
definitions and ingestion dictionary rows cannot drift silently.

Allowed areas:

- `fondant\oekb\`
- `fondant\ingestion\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Renaming database columns.
- Editing Alembic migrations.
- Changing view output names.
- Changing API response field names.

Verification:

- `py -3.10 -m pytest tests/test_oekb_parser.py tests/test_ingestion.py`
- `py -3.10 -m ruff check fondant tests`

## Database Follow-Ups

### EV-FUP-DB-001 FX View Semantics Tests

Goal: Add migration/view tests for current exact-date FX matching, EUR rate `1`,
missing FX rows, and any approved nearest-prior fallback behavior.

Allowed areas:

- `tests\`
- `Documentation\Validation\`
- `alembic\` only if a later implementation ticket explicitly owns view changes.

Non-goals:

- Immediate view rewrite without product/API decision.
- Changing `REFEXC` data.
- Live ECB calls.

Verification:

- `py -3.10 -m pytest tests/test_migrations.py tests/test_fx_pipeline.py`
- Record whether Docker-backed PostgreSQL migration validation ran or skipped.

### EV-FUP-DB-002 Required PostgreSQL Migration CI Gate

Goal: Make PostgreSQL-backed migration validation a required CI or release gate,
or document why the project accepts local optional coverage.

Allowed areas:

- To be defined by master based on CI ownership.
- `Documentation\Validation\` for decision notes.

Non-goals:

- Migration behavior changes.
- Schema redesign.

Verification:

- CI or release-check evidence once infrastructure scope is assigned.

## Operations Follow-Ups

### EV-FUP-OPS-001 Ingestion Batch Failure Summary

Goal: Improve batch-level operator output so isolated ISIN failures and systemic
failures are distinguishable.

Allowed areas:

- `fondant\ingestion\`
- `fondant\jobs\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Database schema changes.
- Parser semantic changes.
- Live ingestion.
- Scheduler or alerting infrastructure.

Verification:

- `py -3.10 -m pytest tests/test_ingestion.py tests/test_jobs_isin_workflows.py`
- `py -3.10 -m ruff check fondant/ingestion fondant/jobs tests`

### EV-FUP-OPS-002 Shared Job CLI Utilities

Goal: If job behavior is already being changed, extract only small shared
helpers for ISIN sanitization or result summarization with tests.

Allowed areas:

- `fondant\jobs\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Standalone broad cleanup.
- New scheduler.
- Live ingestion.
- Job behavior redesign.

Verification:

- `py -3.10 -m pytest tests/test_jobs_isin_workflows.py`
- `py -3.10 -m ruff check fondant/jobs tests/test_jobs_isin_workflows.py`

## Product/API Follow-Ups

### EV-FUP-API-001 Decimal And API Contract Decision

Goal: Decide whether public API tax amounts should remain JSON floats or move to
decimal-safe strings before changing API behavior.

Allowed areas:

- `Documentation\Validation\`
- `tests\` only for a later implementation or contract-hardening ticket.
- `fondant\api\` only for a later implementation ticket.

Non-goals:

- Immediate API response change.
- New endpoints.
- Schema changes.

Verification:

- Human decision record.
- Future implementation ticket should run `py -3.10 -m pytest tests/test_api_etf.py`.

### EV-FUP-API-002 FX Conversion Semantics Decision

Goal: Decide whether EUR conversion should remain exact-date only, use
nearest-prior FX fallback, or expose clearer missing-FX diagnostics.

Allowed areas:

- `Documentation\Validation\`
- Future implementation areas to be defined after decision.

Non-goals:

- Immediate migration or view changes.
- Changing existing data.
- Live ECB ingestion.

Verification:

- Human decision record.
- Future implementation ticket should include migration/view tests and FX tests.

## Residual Risks

- Unknown OeKB tax fields, unexpected category aliases, and malformed numeric
  values can still be dropped or ignored without enough operator-visible
  diagnostics.
- OeKB report-list pagination remains unresolved until characterized against
  expected payload behavior.
- API tests remain narrow for consumer contract stability, decimal precision,
  invalid ISIN behavior, and multi-report ordering.
- Non-EUR EUR-converted view values depend on exact-date FX rows and can return
  null when ECB observations are absent for the report date.
- Batch ingestion can isolate per-ISIN failures, but operators need better
  summary diagnostics to distinguish isolated and systemic failures quickly.
- PostgreSQL migration validation can skip when Docker/testcontainers are
  unavailable; SQLite coverage does not fully replace production-like DDL and
  view validation.
- Tax metric/category definitions remain distributed across parser maps,
  ingestion dictionaries, ORM source columns, migration view SQL, docs, and
  tests.
- The project currently documents no auth layer and no bundled scheduler; any
  future exposure or automation should introduce explicit tests and operations
  boundaries.

## Open Questions For The Operator

- Does OeKB guarantee that one report-list page is sufficient for the expected
  ETF history, or should pagination be required before production trust?
- Should public API numeric tax amounts remain JSON floats, or should the API
  preserve decimal precision through strings?
- Should EUR conversion use exact-date FX only, nearest-prior fallback, or
  explicit missing-FX diagnostics?
- Should PostgreSQL migration tests become a required CI/release gate?
- Do external consumers depend on `STI` view columns for `stiftung`, requiring
  alias preservation?
- Should ingestion failure reporting remain console-oriented, or should future
  operations work introduce structured summaries or alerts?

## Recommended Next Ticket

Start with EV-FUP-TST-001 Parser Negative Coverage. It is small, test-only, and
protects the highest-risk trust boundary before behavior changes. If the
operator wants immediate stakeholder-facing value instead, choose
EV-FUP-TST-003 API Contract Hardening or EV-FUP-API-002 FX Conversion Semantics
Decision.

## Manual Verification Required

Per EV-006, a human should review this final summary and select the next
implementation, test, documentation, operations, database, or product/API
ticket.
