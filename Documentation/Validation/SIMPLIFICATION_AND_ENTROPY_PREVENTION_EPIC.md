# Simplification And Entropy Prevention Epic

## Master Agent Instructions

You are the AEGIS master agent for the EasyETFsAT simplification effort.

Target repository:

```text
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT
```

Required branch:

```text
development
```

Store planning, validation, and decision artifacts under:

```text
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation
```

Work ticket-first. Dispatch exactly one ticket at a time, preserve the
`master -> worker -> validator -> master` loop, and commit each accepted result
to `development` using this commit format:

```text
Simplification Epic <ticket-id>: <short description>
```

The goal is not a broad rewrite. The goal is to remove avoidable ambiguity,
make the current behavior easier to trust, and prevent future entropy by
putting tests, decisions, and small abstractions at the repo's highest-risk
boundaries.

## Source Evidence

This epic is based on:

- `Documentation\Validation\01_REPOSITORY_INVENTORY.md`
- `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md`
- `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md`
- `Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md`
- `Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md`
- `Documentation\Validation\06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md`
- Live PostgreSQL inspection of Docker container `easyetfsat-postgres`

Live database evidence recorded on 2026-05-25:

- PostgreSQL version: `16.13`.
- Alembic head: `20260419_0011`.
- Schema objects: 17 tables and 2 reporting views.
- Column count across tables and views: 270.
- Widest table: `SOURCEAGE` with 73 source-shaped columns.
- Reporting views: `V1_TAXDATPRE` and `V2_TAXDATEUR`.
- Exact populated row counts:

| Relation | Rows |
|---|---:|
| `IMPERR` | 12 |
| `IMPLOG` | 84 |
| `REFCCY` | 0 |
| `REFCTR` | 0 |
| `REFEXC` | 12513 |
| `SECDIV` | 0 |
| `SECMDA` | 6 |
| `SOURCEAGE` | 38 |
| `SOURCERAW` | 38 |
| `SOURCERPT` | 38 |
| `TAXADJ` | 196 |
| `TAXCAT` | 6 |
| `TAXCOR` | 0 |
| `TAXDAT` | 1774 |
| `TAXLIN` | 11 |
| `TAXRPT` | 38 |
| `V1_TAXDATPRE` | 38 |
| `V2_TAXDATEUR` | 38 |

The live schema confirms that the repo is already coherent, but the maintenance
risk is concentrated in a few places: source-to-curated tax mapping, duplicated
tax metric/category knowledge, exact-date FX semantics, optional PostgreSQL
verification, and operational diagnostics.

## Entropy Prevention Principles

- Add characterization tests before changing behavior at ingestion, parser,
  API, and database boundaries.
- Prefer one authoritative registry or decision note where repeated tax codes,
  category aliases, and API field meanings currently drift across parser,
  migrations, views, tests, and docs.
- Keep source-shaped persistence separate from curated reporting behavior.
  `SOURCEAGE` can stay wide if its role is explicit and tested.
- Treat database views as product contracts, not incidental SQL. View semantics
  need tests and documented decisions.
- Keep operational jobs small and observable. Operators should know which ISINs
  failed and whether a failure is isolated or systemic.
- Avoid cosmetic refactors that rename many schema/API fields without a
  compatibility decision.

## Epic Acceptance Criteria

The epic is complete when:

- Every ticket below has a worker result, validator approval, and a commit on
  `development`, or a human-approved override.
- Parser, ingestion, API, FX, and migration trust boundaries have focused tests
  or explicit decision documents.
- Tax line/category knowledge has one documented source of truth or an automated
  consistency gate.
- PostgreSQL migration/view verification is no longer an optional mystery; the
  project has a clear local or CI gate.
- The repository is easier to navigate because the simplifications reduce
  duplicate knowledge and unclear ownership rather than adding process overhead.

## Ticket Backlog

### SE-001: Characterize Parser Drops And Bad Values

```yaml
ticket_id: SE-001
goal: Add tests that document how the OeKB parser handles unknown tax fields, unknown category suffixes, malformed numeric values, and currently dropped data.
dependencies: []
business_context: Parser behavior is the first trust boundary between OeKB payloads and curated tax data.
entropy_prevented: Silent drift in external payload shape becomes visible before it contaminates curated tables or API output.
allowed_areas:
  - tests\
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - .env
requirements:
  - Add focused parser characterization tests.
  - Use static fixtures or inline payloads only; do not call OeKB.
  - Document any currently silent drops as observed behavior, not as approved product behavior.
acceptance_criteria:
  - Tests cover unknown line code, unknown category code, malformed numeric value, and missing expected value.
  - Tests are deterministic and do not require Docker or network access.
verification_commands:
  - py -3.10 -m pytest tests/test_oekb_parser.py
  - py -3.10 -m ruff check tests/test_oekb_parser.py
```

### SE-002: Add Parser Diagnostics Without Schema Churn

```yaml
ticket_id: SE-002
goal: Make parser and ingestion diagnostics expose unmapped fields, unknown categories, and invalid numeric values without changing database schema.
dependencies:
  - SE-001
business_context: Operators and validators need to know when incoming OeKB data no longer matches the implementation's assumptions.
entropy_prevented: New external fields are surfaced at the boundary instead of becoming hidden code comments, scattered TODOs, or silent data loss.
allowed_areas:
  - fondant\oekb\
  - fondant\ingestion\
  - tests\
  - Documentation\Validation\
must_not_touch:
  - alembic\
  - public API response shape unless explicitly required by tests
  - live ingestion credentials
requirements:
  - Preserve existing persisted schema and current curated output.
  - Add structured diagnostics that tests can assert.
  - Keep diagnostics local to parser/ingestion; do not create a broad logging framework.
acceptance_criteria:
  - Unknown fields/categories and invalid numeric values are observable in tests.
  - Existing ingestion tests still pass.
verification_commands:
  - py -3.10 -m pytest tests/test_oekb_parser.py tests/test_ingestion.py
  - py -3.10 -m ruff check fondant/oekb fondant/ingestion tests
```

### SE-003: Create A Tax Code And Category Registry

```yaml
ticket_id: SE-003
goal: Introduce one authoritative code-level registry for tax line codes, tax category codes, API aliases, and view aliases, then add consistency tests against parser output and seeded dictionaries.
dependencies:
  - SE-001
business_context: Tax code meaning currently spans parser mappings, seed dictionaries, migrations/views, tests, and documentation.
entropy_prevented: Future changes to `K61`, `K62`, `K40`, `STF`, `STI`, or public field aliases happen in one reviewed place.
allowed_areas:
  - fondant\
  - tests\
  - Documentation\Validation\
must_not_touch:
  - alembic\ unless a later ticket explicitly approves migration changes
  - API field names without a compatibility decision
requirements:
  - Keep the abstraction small and data-oriented.
  - Include all active live `TAXLIN` codes: `K40`, `K11`, `K12`, `K81`, `K82`, `K10`, `K55`, `K61`, `K62`, `K36`, `K21`.
  - Include all live `TAXCAT` codes: `PVM`, `PVO`, `BVM`, `BVO`, `BVJ`, `STF`.
  - Capture the current `STF` source category versus `STI` view/API alias decision explicitly.
acceptance_criteria:
  - Parser, seed dictionaries, and documented aliases can be checked from one registry.
  - Tests fail if a tax code/category is added in one layer but not reflected in the registry.
verification_commands:
  - py -3.10 -m pytest tests
  - py -3.10 -m ruff check fondant tests
```

### SE-004: Decide Foundation Category Alias Compatibility

```yaml
ticket_id: SE-004
goal: Produce a short compatibility decision for `STF`, `stiftung`, and `STI` before any naming cleanup.
dependencies:
  - SE-003
business_context: The database dictionary uses `STF`, while reporting/view aliases expose `STI`-style fields. Renaming casually would risk breaking consumers.
entropy_prevented: Naming drift is documented as either intentional compatibility or a future migration target.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
requirements:
  - Explain current source name, dictionary code, view/API alias, and compatibility risk.
  - Recommend keep, alias, or migrate.
acceptance_criteria:
  - Future agents can tell whether `STI` is a bug, compatibility alias, or planned migration.
verification_commands:
  - git status --short --branch
  - rg -n "STF|STI|stiftung" Documentation\Validation
```

### SE-005: Characterize OeKB Report-List Pagination

```yaml
ticket_id: SE-005
goal: Add mocked tests that prove whether OeKB report-list pagination processes all expected pages.
dependencies: []
business_context: Missing report-list pages would create incomplete source and tax history.
entropy_prevented: Pagination assumptions become executable tests instead of tribal knowledge.
allowed_areas:
  - tests\
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - live OeKB calls
requirements:
  - Use mocked HTTP responses or fake clients.
  - Confirm the current behavior before changing implementation.
acceptance_criteria:
  - Tests demonstrate current one-page or all-page behavior.
  - No network access is required.
verification_commands:
  - py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py
  - py -3.10 -m ruff check tests
```

### SE-006: Implement Complete OeKB Pagination If Needed

```yaml
ticket_id: SE-006
goal: Implement complete OeKB report-list pagination only if SE-005 confirms incomplete current behavior.
dependencies:
  - SE-005
business_context: Ingestion should not silently miss reports when the upstream list spans multiple pages.
entropy_prevented: External API traversal is handled in one tested client path instead of ad hoc job logic.
allowed_areas:
  - fondant\oekb\
  - fondant\ingestion\
  - tests\
  - Documentation\Validation\
must_not_touch:
  - live OeKB credentials
  - database schema
requirements:
  - Preserve no-live-network tests.
  - Keep pagination logic in the OeKB client or a narrow ingestion boundary.
acceptance_criteria:
  - Mocked multi-page ingestion retrieves all expected reports.
  - Existing ingestion idempotency remains intact.
verification_commands:
  - py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py
  - py -3.10 -m ruff check fondant/oekb fondant/ingestion tests
```

### SE-007: Harden The Public API Contract

```yaml
ticket_id: SE-007
goal: Add tests and a short contract note for `/health` and `GET /etf/{isin}/tax`.
dependencies:
  - SE-003
business_context: API consumers need stable response shape, ordering, null handling, and numeric semantics.
entropy_prevented: Public behavior stops being inferred from implementation details and becomes a reviewed contract.
allowed_areas:
  - tests\
  - Documentation\Validation\
must_not_touch:
  - fondant\ unless tests reveal a confirmed bug and the human approves a follow-up implementation ticket
  - alembic\
requirements:
  - Cover invalid ISINs, no-data ISINs, multiple reports, tied or null dates, OpenAPI shape, and numeric precision expectations.
  - Document whether JSON numbers are accepted or decimal-safe strings are required later.
acceptance_criteria:
  - Contract tests pass against current behavior or document a blocked follow-up.
  - The contract note is short and implementation-faithful.
verification_commands:
  - py -3.10 -m pytest tests/test_api_etf.py
  - py -3.10 -m ruff check tests/test_api_etf.py
```

### SE-008: Decide FX Conversion Semantics

```yaml
ticket_id: SE-008
goal: Decide and document how non-EUR tax values should convert when exact-date ECB FX is missing.
dependencies: []
business_context: `V2_TAXDATEUR` currently uses EUR rate `1` and exact-date joins to `REFEXC`; missing or zero FX rows produce null converted values.
entropy_prevented: FX behavior becomes an explicit data/product rule before SQL or API code changes.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - alembic\
  - fondant\
  - tests\
requirements:
  - Compare exact-date only, previous-business-day fallback, nearest prior available rate, and explicit-null options.
  - Recommend one default and state consumer impact.
acceptance_criteria:
  - The decision note names approved behavior and rejected alternatives.
verification_commands:
  - git status --short --branch
  - rg -n "FX|REFEXC|V2_TAXDATEUR" Documentation\Validation
```

### SE-009: Add PostgreSQL View Semantics Tests

```yaml
ticket_id: SE-009
goal: Add database-backed tests for `V1_TAXDATPRE` and `V2_TAXDATEUR` view semantics.
dependencies:
  - SE-008
business_context: The reporting views are product contracts over tax and FX data.
entropy_prevented: Future migration changes cannot accidentally alter EUR conversion, category pivoting, or missing-rate behavior.
allowed_areas:
  - tests\
  - Documentation\Validation\
must_not_touch:
  - alembic\ unless SE-008 explicitly requires implementation changes in a later ticket
  - fondant\
requirements:
  - Use PostgreSQL-backed tests, not SQLite substitutes, for view behavior.
  - Cover EUR rate `1`, exact-date non-EUR rate, missing FX rate, zero FX rate, and key category pivots.
acceptance_criteria:
  - Tests pass when Docker/testcontainers are available.
  - Skips clearly state missing PostgreSQL dependency when not available.
verification_commands:
  - py -3.10 -m pytest tests
  - py -3.10 -m ruff check tests
```

### SE-010: Make PostgreSQL Migration Verification A Standard Gate

```yaml
ticket_id: SE-010
goal: Create a documented and repeatable PostgreSQL migration verification gate for local validation and, if available, CI.
dependencies:
  - SE-009
business_context: SQLite-style checks cannot validate PostgreSQL views, quoted identifiers, numeric behavior, and constraints with enough confidence.
entropy_prevented: Schema verification no longer depends on whether a validator remembers to run Docker manually.
allowed_areas:
  - Documentation\Validation\
  - tests\
  - pyproject.toml
  - CI configuration only if present and approved by the human
must_not_touch:
  - production credentials
  - database volumes
  - migrations unless a test exposes a confirmed issue
requirements:
  - Document the exact local commands for Docker-backed migration validation.
  - Prefer a test marker or script that makes the gate easy to run.
  - Do not require live OeKB or ECB access.
acceptance_criteria:
  - A validator can run one documented command to verify migrations/views against PostgreSQL.
  - CI feasibility is recorded even if CI implementation is deferred.
verification_commands:
  - docker compose ps
  - py -3.10 -m alembic current
  - py -3.10 -m pytest tests
```

### SE-011: Improve Ingestion Batch Failure Summaries

```yaml
ticket_id: SE-011
goal: Make ingestion job summaries distinguish isolated ISIN failures from systemic failures.
dependencies:
  - SE-002
business_context: Operators need to know whether to retry a few ISINs, inspect upstream availability, or stop the batch.
entropy_prevented: Operational failure interpretation is centralized in tested job behavior instead of scattered console reading.
allowed_areas:
  - fondant\jobs\
  - fondant\ingestion\
  - tests\
  - Documentation\Validation\
must_not_touch:
  - scheduler infrastructure
  - live ingestion credentials
requirements:
  - Add tests for all-success, mixed-failure, and all-failure runs.
  - Preserve dry-run behavior.
  - Keep output concise and machine-readable enough for future automation.
acceptance_criteria:
  - Job tests assert summary counts and failure categories.
  - Existing refresh and missing-ISIN flows still pass.
verification_commands:
  - py -3.10 -m pytest tests/test_jobs_isin_workflows.py tests/test_ingestion.py
  - py -3.10 -m ruff check fondant/jobs fondant/ingestion tests
```

### SE-012: Split Source, Curated, And API Documentation For Maintainers

```yaml
ticket_id: SE-012
goal: Refine documentation so maintainers can quickly distinguish source-shaped storage, curated tax tables, reporting views, and API output.
dependencies:
  - SE-003
  - SE-008
business_context: The repository is easier to extend when maintainers know which layer owns each concept.
entropy_prevented: Future changes are routed to the right layer instead of mixing parser, schema, view, and API concerns.
allowed_areas:
  - Documentation\
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
requirements:
  - Keep docs concise and link to authoritative files instead of duplicating large tables.
  - Add a "where to change what" section for tax codes, category aliases, FX semantics, API output, and ingestion jobs.
acceptance_criteria:
  - A new maintainer can identify the correct layer for common changes without reading all migrations first.
verification_commands:
  - rg --files Documentation
  - rg -n "source-shaped|curated|V2_TAXDATEUR|STF|STI|tax code" Documentation
```

## Recommended Execution Order

1. `SE-001` parser characterization.
2. `SE-002` parser diagnostics.
3. `SE-003` tax registry and consistency tests.
4. `SE-004` foundation alias decision.
5. `SE-005` OeKB pagination characterization.
6. `SE-006` OeKB pagination implementation if required.
7. `SE-007` API contract hardening.
8. `SE-008` FX conversion decision.
9. `SE-009` PostgreSQL view semantics tests.
10. `SE-010` PostgreSQL migration verification gate.
11. `SE-011` ingestion failure summaries.
12. `SE-012` maintainer documentation refinement.

This order deliberately starts with tests and decisions before implementation.
That keeps simplification work small, auditable, and less likely to create new
entropy while trying to remove old entropy.
