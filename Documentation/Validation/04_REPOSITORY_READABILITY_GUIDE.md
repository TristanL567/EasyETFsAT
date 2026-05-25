# EV-004 Repository Readability Guide

## Purpose

Use this guide as the validation-oriented entry point for EasyETFsAT. It points
maintainers and agents to the right files, likely change areas, and safest
checks without replacing the project README, technical architecture, data guide,
or stakeholder documentation.

Source validation artifacts:

- `Documentation\Validation\01_REPOSITORY_INVENTORY.md`
- `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md`
- `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md`
- `Documentation\Validation\EV-001_VALIDATION.md`
- `Documentation\Validation\EV-002_VALIDATION.md`
- `Documentation\Validation\EV-003_VALIDATION.md`

## Recommended Reading Order

1. `Documentation\README.md` for the project documentation map.
2. `README.md` for quick start, local runtime, API, migration, and job commands.
3. `Documentation\Validation\01_REPOSITORY_INVENTORY.md` for repository layout,
   runtime stack, database stack, test stack, and operational entry points.
4. `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md` for
   module ownership, API routes, ingestion flows, FX flow, jobs, migrations, and
   test relationships.
5. `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md` for ranked
   maintainability risks, known test gaps, and proposed follow-up tickets.
6. `Documentation\TECHNICAL_ARCHITECTURE.md` when implementation-level detail is
   needed.
7. `Documentation\DATA_AND_QUERY_GUIDE.md` when database tables, views, SQL
   examples, or data-consumer behavior matter.
8. `Documentation\STAKEHOLDER_BRIEF.md` when explaining project value or limits
   to non-implementation stakeholders.

## Authoritative Docs And Runbooks

| Need | Start here |
|---|---|
| Local setup, API examples, migration tests, FX notes, ISIN jobs | `README.md` |
| Documentation overview | `Documentation\README.md` |
| Technical component description | `Documentation\TECHNICAL_ARCHITECTURE.md` |
| Data layers, tables, views, and query examples | `Documentation\DATA_AND_QUERY_GUIDE.md` |
| Stakeholder-facing summary and limitations | `Documentation\STAKEHOLDER_BRIEF.md` |
| Missing-ISIN operational runbook | `Documentation\AgentInstructions\FETCH_ONLY_MISSING_ISINS.md` |
| Existing-ISIN refresh runbook | `Documentation\AgentInstructions\REFRESH_EXISTING_ISINS.md` |
| Database naming and table catalog | `docs\db_naming_dictionary.md`, `docs\db_table_catalog.md` |
| Validation baseline | `Documentation\Validation\01_REPOSITORY_INVENTORY.md` |
| Architecture and API map | `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md` |
| Entropy and follow-up risks | `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md` |

## Where To Change What

| Change type | Primary area | Read before changing | Safest validation commands |
|---|---|---|---|
| API route behavior or response shape | `fondant\api\` | Architecture map API section; entropy finding 7 | `py -3.10 -m pytest tests/test_api_etf.py`; `py -3.10 -m ruff check fondant/api tests/test_api_etf.py` |
| OeKB ingestion orchestration or curation | `fondant\ingestion\pipeline.py` | Architecture map OeKB data flow; entropy findings 2, 5, 6 | `py -3.10 -m pytest tests/test_ingestion.py`; `py -3.10 -m ruff check fondant/ingestion tests/test_ingestion.py` |
| OeKB parser mappings or payload interpretation | `fondant\oekb\parser.py`, `fondant\oekb\models.py` | Entropy findings 1, 2, 9 | `py -3.10 -m pytest tests/test_oekb_parser.py`; `py -3.10 -m ruff check fondant/oekb tests/test_oekb_parser.py` |
| OeKB client request behavior | `fondant\oekb\client.py` | Architecture map public/internal interfaces; entropy finding 3 | `py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py`; `py -3.10 -m ruff check fondant/oekb fondant/ingestion tests/test_oekb_client.py tests/test_ingestion.py` |
| ECB FX client or pipeline | `fondant\ecb\`, `fondant\ingestion\fx_pipeline.py` | Architecture map ECB FX flow; entropy finding 4 | `py -3.10 -m pytest tests/test_ecb_client.py tests/test_fx_pipeline.py`; `py -3.10 -m ruff check fondant/ecb fondant/ingestion tests/test_ecb_client.py tests/test_fx_pipeline.py` |
| ISIN job selection, dry-run, or reporting | `fondant\jobs\` | Architecture map job flows; entropy finding 10 | `py -3.10 -m pytest tests/test_jobs_isin_workflows.py`; `py -3.10 -m ruff check fondant/jobs tests/test_jobs_isin_workflows.py` |
| Database schema or views | `alembic\versions\`, `fondant\db\models\` | Architecture map database relationship and migration sections; data guide; entropy findings 2, 4, 8, 9 | `py -3.10 -m pytest tests/test_migrations.py`; run Docker-backed PostgreSQL checks when available |
| Database naming or table documentation | `docs\db_naming_dictionary.md`, `docs\db_table_catalog.md` | Inventory documentation section; entropy finding 9 | Human doc review plus targeted tests for any related code behavior |
| Validation or onboarding docs | `Documentation\Validation\` | This guide and prior validation artifacts | `git status --short --branch`; `rg --files Documentation\Validation` |
| Test behavior or coverage | `tests\` | Architecture map test coverage map; entropy quick wins | Target the relevant `tests/test_*.py`; use `py -3.10 -m pytest --collect-only` for collection-only checks |

## Validation Commands By Work Type

Use the narrowest command that covers the changed behavior first, then widen only
when the change crosses module boundaries.

| Work type | Suggested command |
|---|---|
| Confirm branch and unrelated worktree state | `git status --short --branch` |
| List validation artifacts | `rg --files Documentation\Validation` |
| Fast test discovery | `py -3.10 -m pytest --collect-only` |
| Lint all currently checked code | `py -3.10 -m ruff check .` |
| API-only change | `py -3.10 -m pytest tests/test_api_etf.py` |
| Parser-only change | `py -3.10 -m pytest tests/test_oekb_parser.py` |
| OeKB client or ingestion change | `py -3.10 -m pytest tests/test_oekb_client.py tests/test_ingestion.py` |
| FX change | `py -3.10 -m pytest tests/test_ecb_client.py tests/test_fx_pipeline.py` |
| Job change | `py -3.10 -m pytest tests/test_jobs_isin_workflows.py` |
| Migration or schema change | `py -3.10 -m pytest tests/test_migrations.py` |

Validation note from EV-003: bare `pytest` and `ruff` were not discoverable on
PATH in this environment. The `py -3.10 -m ...` invocations passed during
EV-003 validation and are the safer local form unless the environment changes.

## Common Navigation Rules

- Start with `fondant\api\` for HTTP behavior; the tax API reads curated tax
  tables and does not call OeKB, ECB, ingestion jobs, or views at request time.
- Start with `fondant\oekb\` for external OeKB request and parser behavior.
- Start with `fondant\ingestion\pipeline.py` for source persistence, curation,
  import logging, and ingestion side effects.
- Start with `fondant\ecb\` and `fondant\ingestion\fx_pipeline.py` for ECB
  reference-rate loading.
- Start with `fondant\jobs\` for CLI selection, dry-run behavior, and
  `Documentation\isin_storage.csv` workflows.
- Start with `alembic\versions\` and `fondant\db\models\` for schema and view
  changes, then cross-check `docs\db_table_catalog.md`.
- Start with the matching `tests\test_*.py` file before changing behavior.

## Do Not Touch Casually

- `.env`, `.venv\`, `.git\`, credentials, local machine configuration, or
  generated cache directories.
- Alembic migrations and database views unless the ticket explicitly owns schema
  behavior and includes migration validation.
- Parser tax mappings, ingestion dictionaries, `SOURCEAGE` columns, and view
  projections as isolated edits; EV-003 identifies these as coupled across
  several layers.
- FX conversion semantics in `V2_TAXDATEUR` without a product/data decision on
  exact-date versus fallback behavior.
- Existing top-level or product documentation when a validation ticket is scoped
  only to `Documentation\Validation`.
- Live OeKB or ECB ingestion jobs without explicit operator approval.

## Manual Verification Required

A human should review this guide for whether it improves repository navigation
without duplicating the authoritative docs named above.
