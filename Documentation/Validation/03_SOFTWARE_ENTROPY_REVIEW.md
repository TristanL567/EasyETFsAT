# EV-003 Software Entropy And Maintainability Review

## Scope

This report identifies maintainability risks, unclear boundaries, readability problems, test gaps, and documentation drift in the current EasyETFsAT implementation. It is diagnostic only. No application code, migrations, tests, environment files, or existing documentation were changed.

Source evidence was read from `fondant`, `alembic`, `tests`, `docs`, `Documentation`, `pyproject.toml`, and the prior EV-001/EV-002 validation artifacts.

## Severity Scale

| Severity | Meaning |
|---|---|
| High | Material correctness, operational, or schema/API risk likely to affect future changes or production trust. |
| Medium | Clear maintainability, coupling, readability, or verification weakness that can create future defects. |
| Low | Localized readability, terminology, or documentation issue with limited current blast radius. |

## Ranked Risk Register

| Rank | Severity | Type | Status | Finding |
|---:|---|---|---|---|
| 1 | High | correctness risk, schema/API risk, test gap | Confirmed risk surface | Parser unmapped or malformed OeKB tax data is silently ignored, and the tests cover only selected known mappings. |
| 2 | High | coupling risk, readability risk | Confirmed maintainability risk | OeKB tax metric/category knowledge is duplicated across parser maps, ingestion dictionaries, ORM wide columns, migrations/views, docs, and tests. |
| 3 | High | operational risk, test gap | Plausible production risk | OeKB list pagination is exposed in the client but ingestion reads only one list page per ISIN. |
| 4 | Medium | operational risk, schema/API risk | Confirmed behavior with business impact | EUR conversion view requires exact-date FX rows, so non-EUR values become null when ECB has no observation for the report date. |
| 5 | Medium | operational risk, correctness risk | Plausible production risk | Ingestion catches all exceptions and returns per-ISIN `FAILED`, which keeps batch jobs running but can hide systemic failures behind aggregate job output. |
| 6 | Medium | coupling risk, readability risk | Confirmed maintainability risk | `fondant.ingestion.pipeline` owns too many responsibilities: client orchestration, import logging, source persistence, dictionary seeding, curation, correction links, and distribution events. |
| 7 | Medium | test gap, schema/API risk | Confirmed test gap | API tests cover the current response shape but not OpenAPI contract stability, multi-report ordering semantics, decimal precision expectations, or malformed ISIN behavior. |
| 8 | Medium | test gap, operational risk | Confirmed test gap | PostgreSQL migration validation is optional and skipped when Docker or testcontainers are unavailable. |
| 9 | Low | documentation drift, readability risk | Confirmed terminology drift | Foundation category is represented as `STF` in source tables/dictionaries and `STI` in view output columns, with docs documenting the alias but code relying on both spellings. |
| 10 | Low | readability risk, operational risk | Confirmed local issue | Job modules duplicate CLI sanitization, candidate selection, result summarization, and print-based reporting patterns. |

## Detailed Findings

### 1. Silent Parser Drops For Unknown OeKB Payload Shapes

Severity: High  
Types: correctness risk, schema/API risk, test gap  
Status: Confirmed risk surface

Evidence:

- `fondant\oekb\parser.py:50` defines `TAX_FIELD_MAP`, and `fondant\oekb\parser.py:177` only maps a `steuerName` when it appears in that map.
- `fondant\oekb\parser.py:165` recursively walks arbitrary nested payloads, while `fondant\oekb\parser.py:202` returns `None` for categories not in `CATEGORY_KEY_MAP`.
- `fondant\oekb\parser.py:205` through `fondant\oekb\parser.py:219` converts supported values to `Decimal` but returns `None` for invalid numeric strings without surfacing parser diagnostics.
- `tests\test_oekb_parser.py:9` and `tests\test_oekb_parser.py:41` cover selected category suffixes and K40/K62 mappings, not unmapped tax fields, malformed values, unexpected category aliases, or "dropped field" observability.

Why it matters:

The parser is the source-to-curated trust boundary. Unknown OeKB field names or category names can result in missing curated tax values without an import error. That is a correctness risk because downstream API and views can look clean while source payload content was skipped.

Recommended action:

Add parser diagnostics and tests that make unmapped `steuerName`, unknown category aliases, and invalid numeric values observable. This can be done without changing schema first by reporting parse warnings into an in-memory parse result or import error detail.

### 2. Tax Metric And Category Knowledge Is Duplicated Across Layers

Severity: High  
Types: coupling risk, readability risk  
Status: Confirmed maintainability risk

Evidence:

- Parser metric/category maps live in `fondant\oekb\parser.py:10`, `fondant\oekb\parser.py:27`, `fondant\oekb\parser.py:41`, and `fondant\oekb\parser.py:50`.
- Ingestion has separate line/category dictionaries in `fondant\ingestion\pipeline.py:41` and `fondant\ingestion\pipeline.py:55`.
- ORM `SOURCEAGE` repeats every metric/category field as explicit mapped columns from `fondant\db\models\tax.py:57` through `fondant\db\models\tax.py:151`.
- Migration view SQL repeats selected K-code/category projections in `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:17` and `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:111`.
- Tests assert selected duplicated names in `tests\test_migrations.py:87` through `tests\test_migrations.py:112`.

Module boundary review:

- Affected modules: `fondant.oekb.parser`, `fondant.ingestion.pipeline`, `fondant.db.models.tax`, Alembic view migrations, docs, and tests.
- Public/internal interfaces at risk: parser output keys, `SOURCEAGE` attributes, `TAXLIN`/`TAXCAT` dictionaries, `TAXDAT` curation, and reporting view column names.
- Coupling risk: a new or corrected tax metric requires coordinated edits across code, schema, views, and tests. Callers and maintainers must know internal naming conventions across layers.
- Deep/shallow assessment: current behavior is shallow across several modules; the maps are simple individually, but the real invariant is distributed.
- Recommendation: defer schema changes, but create a narrow mapping-registry ticket that first centralizes code-level tax metric/category definitions and generates or validates the existing dictionaries from one source of truth.
- Scope guard: no code, moves, renames, or refactor implementation were produced here.

### 3. OeKB Ingestion Does Not Appear To Follow List Pagination

Severity: High  
Types: operational risk, test gap  
Status: Plausible production risk

Evidence:

- `OeKBClient.get_report_list` accepts `offset` and `limit` in `fondant\oekb\client.py:63` through `fondant\oekb\client.py:80`, with default `limit=50`.
- `fondant\ingestion\pipeline.py:89` calls `client.get_report_list(isin)` once and does not loop over pages.
- `tests\test_oekb_client.py:10` validates request parameters, while `tests\test_ingestion.py:38` uses a fake client returning a fixed local list; no test covers more than one OeKB page.

Why it matters:

If an ISIN has more report-list entries than the default page size or if OeKB expects pagination to retrieve all results, ingestion can miss reports while still returning success. The exact production impact depends on OeKB list cardinality per ISIN, so this is a plausible risk rather than a confirmed defect.

Recommended action:

Add a small discovery/test ticket to define expected pagination behavior, then implement complete page retrieval if confirmed.

### 4. Exact-Date FX Conversion Produces Null EUR Values For Missing ECB Dates

Severity: Medium  
Types: operational risk, schema/API risk  
Status: Confirmed behavior with business impact

Evidence:

- `fondant\ingestion\fx_pipeline.py:53` through `fondant\ingestion\fx_pipeline.py:79` fetches a recent lookback window and keeps the latest available observation per currency for ingestion.
- `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:145` joins `REFEXC` by exact `REFCCY` and `REFDAT`, and `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:156` through `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:173` return `NULL` when `FXRAT` is null or zero.
- EV-002 documents the same exact-date dependency in `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md:157`.
- `Documentation\STAKEHOLDER_BRIEF.md:115` already warns that non-EUR converted values return `NULL` when the report date has no ECB observation.

Why it matters:

The behavior is documented, but it remains operationally risky because analysts may interpret nulls as missing tax amounts rather than missing FX observations.

Recommended action:

Keep the current schema unchanged until product intent is clear. Add explicit validation/tests and a future product/API decision ticket for nearest-prior FX fallback or clearer null diagnostics.

### 5. Batch Ingestion Error Handling Can Hide Systemic Failures

Severity: Medium  
Types: operational risk, correctness risk  
Status: Plausible production risk

Evidence:

- `fondant\ingestion\pipeline.py:171` catches `Exception` around the full ISIN ingest, records `IMPERR`, marks `IMPLOG` failed, commits, and returns `IngestionResult(status="FAILED")`.
- `fondant\ingestion\pipeline.py:214` through `fondant\ingestion\pipeline.py:218` processes ISINs sequentially and keeps going.
- `fondant\jobs\fetch_missing_isins.py:71` through `fondant\jobs\fetch_missing_isins.py:85` and `fondant\jobs\refresh_existing_isins.py:50` through `fondant\jobs\refresh_existing_isins.py:66` summarize failures with console output and return exit code `2` only when any result is failed.

Why it matters:

Per-ISIN isolation is useful, but a database misconfiguration, OeKB outage, parser regression, or systemic migration problem can produce many individual failures with limited triage detail at the job boundary. The operator must inspect logs and `IMPERR` after the fact.

Recommended action:

Add operational failure classification and batch-level summary detail, such as counting exception types and exposing a systemic-failure threshold. Keep this as a future operations ticket.

### 6. Ingestion Pipeline Is A Large Multi-Owner Module

Severity: Medium  
Types: coupling risk, readability risk  
Status: Confirmed maintainability risk

Evidence:

- `fondant\ingestion\pipeline.py:75` starts the main `ingest_isin` orchestration.
- The same file owns import logging at `fondant\ingestion\pipeline.py:222`, generic upsert mechanics at `fondant\ingestion\pipeline.py:237`, security master upsert at `fondant\ingestion\pipeline.py:293`, source persistence decisions at `fondant\ingestion\pipeline.py:319`, dictionary seeding at `fondant\ingestion\pipeline.py:345`, and curation at `fondant\ingestion\pipeline.py:378`.
- `_curate_report` deletes and rewrites curated points in `fondant\ingestion\pipeline.py:419` and `fondant\ingestion\pipeline.py:420`, then owns TAXDAT, TAXADJ, SECDIV, and TAXCOR side effects through the remainder of the function.

Module boundary review:

- Affected modules: `fondant.ingestion.pipeline`, `fondant.oekb.parser`, `fondant.db.models`, job modules, and API consumers of curated data.
- Public/internal interfaces at risk: `ingest_isin`, `ingest_many`, `IngestionResult`, curated table contents, `IMPLOG`/`IMPERR`.
- Coupling risk: changing curation rules requires understanding error handling, source version logic, dictionary seeding, and operational logging in one file.
- Deep/shallow assessment: this is a deep module from the caller perspective, but internally it has too many invariants in one file. The public interface is simple; the implementation needs internal seams only if they reduce actual testing and ownership cost.
- Recommendation: defer broad refactor. First add characterization tests around curation side effects; then split only if tests show a stable curation boundary.
- Scope guard: no code, moves, renames, or refactor implementation were produced here.

### 7. API Contract Tests Are Narrow

Severity: Medium  
Types: test gap, schema/API risk  
Status: Confirmed test gap

Evidence:

- The API route returns `float(taxdat.amount)` at `fondant\api\routes\etf.py:57`, which defines a decimal precision behavior not asserted beyond simple values.
- `tests\test_api_etf.py:117`, `tests\test_api_etf.py:130`, and `tests\test_api_etf.py:136` cover success, 404, and null-year fallback only.
- `fondant\api\routes\etf.py:15` exposes a public route, but tests do not cover generated OpenAPI shape, invalid ISIN string behavior, multiple reports with tied/null dates, or response sorting semantics beyond a single report.

Why it matters:

The route is the public consumer surface. Narrow tests make future curation, schema, or API response changes easier to break accidentally.

Recommended action:

Add API contract tests before changing response behavior. Decide whether numeric values should remain JSON floats or preserve decimal string precision.

### 8. PostgreSQL Migration Validation Is Conditional

Severity: Medium  
Types: test gap, operational risk  
Status: Confirmed test gap

Evidence:

- `tests\test_migrations.py:130` through `tests\test_migrations.py:140` skips PostgreSQL migration tests when Docker is unavailable.
- `tests\test_migrations.py:144` through `tests\test_migrations.py:153` skips when testcontainers cannot start PostgreSQL.
- `tests\test_migrations.py:156` always tests SQLite fresh install, while `tests\test_migrations.py:164` tests PostgreSQL only when the fixture is available.

Why it matters:

SQLite coverage is valuable for quick validation, but production uses PostgreSQL. PostgreSQL-specific DDL, JSONB, constraints, indexes, and view behavior can drift if local or CI environments skip the container test.

Recommended action:

Make PostgreSQL migration verification a required CI gate or add a documented release checklist that records when it was skipped.

### 9. Foundation Category Uses `STF` And `STI` In Different Layers

Severity: Low  
Types: documentation drift, readability risk  
Status: Confirmed terminology drift

Evidence:

- Code dictionary uses category code `STF` for `stiftung` at `fondant\ingestion\pipeline.py:61`.
- Parser maps `stiftung` to `STF` through `fondant\oekb\parser.py:47`.
- Source table fields use `STF`, such as `fondant\db\models\tax.py:81` and `fondant\db\models\tax.py:151`.
- View output columns use `STI`, such as `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:46`, `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:70`, and `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:94`.
- `docs\db_naming_dictionary.md:62` and `docs\db_naming_dictionary.md:63` document the alias, so this is not undocumented. The drift remains a readability trap for maintainers and analysts.

Ubiquitous language map:

| Term | Current meaning | Evidence | Aliases or conflicts | Open ambiguity |
|---|---|---|---|---|
| `stiftung` | Investor category for foundations. | `fondant\oekb\parser.py:23`, `fondant\ingestion\pipeline.py:61` | `STF` in source/model/category dictionaries; `STI` in views. | Whether future public/reporting columns should continue using `STI` for compatibility. |
| `STF` | Physical/code category abbreviation for `stiftung`. | `docs\db_naming_dictionary.md:62`, `fondant\db\models\tax.py:81` | Conflicts visually with `STI` view alias. | None for current code; documented as source-table abbreviation. |
| `STI` | View-output alias for `stiftung`. | `docs\db_naming_dictionary.md:63`, `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:46` | Not used by parser or dictionary rows. | Whether external consumers rely on it. |

Usage guidance:

Use `stiftung` in prose and code-level business language. Treat `STF` and `STI` as physical-layer aliases until a compatibility ticket decides otherwise.

### 10. Job Modules Duplicate Operational Plumbing

Severity: Low  
Types: readability risk, operational risk  
Status: Confirmed local issue

Evidence:

- `fondant\jobs\fetch_missing_isins.py:49` and `fondant\jobs\refresh_existing_isins.py:30` both sanitize ISIN inputs.
- `fondant\jobs\fetch_missing_isins.py:71` and `fondant\jobs\refresh_existing_isins.py:50` both summarize `IngestionResult` lists with print statements.
- `fondant\jobs\fetch_missing_isins.py:87` and `fondant\jobs\refresh_existing_isins.py:68` both implement candidate selection, `--limit`, `--dry-run`, and `--show-isins` flow.

Why it matters:

The duplication is small today, but operational behavior can drift as job features are added. Because these are CLI jobs rather than library APIs, the risk is readability and consistency rather than immediate correctness.

Recommended action:

Defer until another job change is needed. If touched, extract only a small shared summarizer/input sanitizer with tests.

## Quick Wins

| Candidate | Why it is small | Evidence | Verification idea |
|---|---|---|---|
| Add parser negative-case tests for unknown tax field/category and invalid numeric values. | Test-only; no schema change required if current silent behavior is explicitly characterized first. | `fondant\oekb\parser.py:177`, `fondant\oekb\parser.py:202`, `fondant\oekb\parser.py:205`; `tests\test_oekb_parser.py:9`. | `pytest tests/test_oekb_parser.py` and `ruff check tests/test_oekb_parser.py`. |
| Add API tests for invalid ISIN, multi-report ordering, and decimal precision expectations. | Test-only and scoped to public route behavior. | `fondant\api\routes\etf.py:15`, `fondant\api\routes\etf.py:57`; `tests\test_api_etf.py:117`. | `pytest tests/test_api_etf.py`. |
| Document PostgreSQL migration-test skip risk in validation/CI notes. | Documentation-only; no code or migration change. | `tests\test_migrations.py:130`, `tests\test_migrations.py:144`, `tests\test_migrations.py:164`. | Human review plus `pytest --collect-only`. |
| Add a focused OeKB pagination characterization test using a fake client or mocked client calls. | Test-first discovery can confirm expected behavior before implementation. | `fondant\oekb\client.py:63`, `fondant\ingestion\pipeline.py:89`. | New test should fail or document non-support before implementation. |

## Larger Architectural Work

| Candidate | Why it is larger | Evidence | Suggested sequencing |
|---|---|---|---|
| Centralize tax metric/category definitions. | Touches parser, ingestion dictionaries, model/view expectations, tests, and possibly migrations if taken too far. | `fondant\oekb\parser.py:27`, `fondant\ingestion\pipeline.py:41`, `fondant\db\models\tax.py:57`, `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:17`. | Start with code-level registry plus consistency tests. Do not rename columns or change views in the first ticket. |
| Split curation responsibilities after characterization tests. | Current pipeline hides many side effects behind one public function; refactoring without tests could change data semantics. | `fondant\ingestion\pipeline.py:378`, `fondant\ingestion\pipeline.py:419`, `fondant\ingestion\pipeline.py:462`, `fondant\ingestion\pipeline.py:480`. | First add tests for TAXDAT/TAXADJ/SECDIV/TAXCOR side effects, then extract a curation helper if justified. |
| Decide FX fallback semantics. | Requires product/data-consumer decision, database view or API behavior change, and tests. | `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py:145`, `Documentation\STAKEHOLDER_BRIEF.md:115`. | Run a design clarification ticket before implementation; decide exact-date, nearest-prior, or explicit null diagnostics. |
| Improve ingestion observability for systemic failures. | Changes operational output and possibly import logging semantics. | `fondant\ingestion\pipeline.py:171`, `fondant\jobs\fetch_missing_isins.py:71`, `fondant\jobs\refresh_existing_isins.py:50`. | Add tests around batch summaries first; avoid changing job exit codes without operator approval. |

## Proposed Future AEGIS Tickets

### Draft Ticket: EV-FUT-001 Parser Diagnostics And Coverage

Goal: Make OeKB parser drops and malformed values observable, then add focused parser tests.

Allowed areas:

- `fondant\oekb\`
- `fondant\ingestion\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Database schema changes.
- Renaming tax columns.
- Live OeKB ingestion.
- Broad parser rewrite.

Verification ideas:

- `pytest tests/test_oekb_parser.py tests/test_ingestion.py`
- `ruff check fondant/oekb fondant/ingestion tests/test_oekb_parser.py tests/test_ingestion.py`
- Characterize unmapped tax field, unknown category, invalid numeric value, and successful known mapping behavior.

### Draft Ticket: EV-FUT-002 Tax Metric Registry Consistency Check

Goal: Reduce duplicated tax metric/category definitions by introducing a code-level consistency check or registry while preserving existing schema and API output.

Allowed areas:

- `fondant\oekb\`
- `fondant\ingestion\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Renaming database columns.
- Editing Alembic migrations.
- Changing `V1_TAXDATPRE` or `V2_TAXDATEUR`.
- Changing API response field names.

Verification ideas:

- `pytest tests/test_oekb_parser.py tests/test_ingestion.py`
- `ruff check fondant tests`
- Add a test that all parser metric/category codes have matching ingestion dictionary rows.

### Draft Ticket: EV-FUT-003 OeKB Report-List Pagination Validation

Goal: Confirm and, if needed, implement complete OeKB report-list pagination for ingestion.

Allowed areas:

- `fondant\oekb\`
- `fondant\ingestion\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Live OeKB ingestion without explicit approval.
- Parser or schema changes.
- Job scheduling changes.

Verification ideas:

- `pytest tests/test_oekb_client.py tests/test_ingestion.py`
- `ruff check fondant/oekb fondant/ingestion tests/test_oekb_client.py tests/test_ingestion.py`
- Use mocked paged responses or fake client behavior to prove all expected reports are processed.

### Draft Ticket: EV-FUT-004 API Contract Hardening Tests

Goal: Add focused tests for `/etf/{isin}/tax` contract stability, including numeric representation and multi-report ordering.

Allowed areas:

- `tests\`
- `fondant\api\` only if tests reveal a confirmed bug and ticket is expanded by master.
- `Documentation\Validation\`

Non-goals:

- New API endpoints.
- Response redesign.
- Database schema changes.
- Live ingestion.

Verification ideas:

- `pytest tests/test_api_etf.py`
- `ruff check tests/test_api_etf.py`
- Cover invalid ISIN string behavior, multiple reports, null `meldg_datum`, and decimal precision expectation.

### Draft Ticket: EV-FUT-005 FX Conversion Semantics Decision

Goal: Decide and document whether EUR conversion should remain exact-date only or use nearest-prior FX fallback.

Allowed areas:

- `Documentation\Validation\`
- `Documentation\` only if a later documentation ticket explicitly allows it.
- `tests\` and `alembic\` only in a later implementation ticket after the decision.

Non-goals:

- Immediate migration or view changes.
- Changing existing `REFEXC` data.
- Live ECB ingestion.

Verification ideas:

- Human decision record for exact-date vs fallback.
- Future implementation verification would include migration/view tests and FX pipeline tests.

### Draft Ticket: EV-FUT-006 Ingestion Batch Observability

Goal: Improve batch-level reporting so operators can distinguish isolated ISIN failures from systemic failures.

Allowed areas:

- `fondant\ingestion\`
- `fondant\jobs\`
- `tests\`
- `Documentation\Validation\`

Non-goals:

- Changing database schema.
- Changing OeKB parser semantics.
- Live ingestion.
- Scheduler or infrastructure work.

Verification ideas:

- `pytest tests/test_ingestion.py tests/test_jobs_isin_workflows.py`
- `ruff check fondant/ingestion fondant/jobs tests/test_ingestion.py tests/test_jobs_isin_workflows.py`
- Test mixed success/failure and all-failure batch summaries.

## Open Questions

- Does OeKB guarantee that a single `limit=50` page is sufficient for the expected ETF report-list history, or must ingestion support pagination?
- Should `/etf/{isin}/tax` preserve tax amounts as decimal strings instead of JSON floats for consumer-facing precision?
- Do any external consumers already depend on `STI` view columns for `stiftung`, making alias cleanup a compatibility concern?
- Should non-EUR EUR conversion use exact report-date FX only, nearest-prior FX, or explicit "missing FX" diagnostics?
- Should PostgreSQL migration tests be a required CI/release gate, or is local Docker-optional validation acceptable?

## Scope Validation Notes

- Ticket executed: EV-003 only.
- Produced artifact: `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md`.
- Code, migrations, tests, environment files, and existing documentation were read only.
- No fixes, formatting sweeps, dependency upgrades, database column renames, live ingestion, staging, commits, pushes, PRs, or EV-004 work were performed.

## Manual Verification Required

A human should review this severity ranking and choose which follow-up tickets matter.
