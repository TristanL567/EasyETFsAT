# Maintainer Layer Guide

Use this guide to route common changes to the right layer before editing code,
migrations, jobs, or public output. It summarizes existing decisions and links
to the authoritative files instead of repeating the full tax and schema tables.

## Layer Boundaries

| Layer | Owns | Does not own | Authoritative references |
|---|---|---|---|
| Source-shaped OeKB storage | Raw OeKB detail JSON, OeKB report metadata, and parsed wide source values in `SOURCERAW`, `SOURCERPT`, and `SOURCEAGE`. | Public API shape, reporting-view compatibility aliases, or curated tax semantics beyond preserving what arrived from OeKB. | [Data and Query Guide](DATA_AND_QUERY_GUIDE.md), [Architecture relationships](Validation/02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md), `fondant/oekb/parser.py`, `fondant/ingestion/pipeline.py` |
| Curated tax rows | Normalized report headers, dictionaries, tax facts, adjustments, distributions, and corrections in `TAXRPT`, `TAXLIN`, `TAXCAT`, `TAXDAT`, `TAXADJ`, `SECDIV`, and `TAXCOR`. | Source archive layout, EUR reporting-view conversion rules, or HTTP response formatting. | [SE-003 tax registry note](Validation/SE-003_TAX_CODE_REGISTRY.md), `fondant/tax_registry.py`, `fondant/ingestion/pipeline.py` |
| Reporting views | Spreadsheet/BI projections such as `V1_TAXDATPRE` and `V2_TAXDATEUR`, including selected pivot columns and view aliases. | Parser aliases, dictionary seed values, or API route behavior unless a compatibility decision says the public contract also changes. | [Data and Query Guide](DATA_AND_QUERY_GUIDE.md), [SE-004 alias decision](Validation/SE-004_FOUNDATION_CATEGORY_ALIAS_DECISION.md), [SE-008 FX decision](Validation/SE-008_FX_CONVERSION_SEMANTICS_DECISION.md), Alembic view migrations |
| API output | HTTP behavior for `/health` and `GET /etf/{isin}/tax`, including response shape, ordering expectations, null handling, and numeric serialization. | OeKB ingestion, ECB fetching, view SQL, or database migrations at request time. | [SE-007 public API contract](Validation/SE-007_PUBLIC_API_CONTRACT.md), `fondant/api/routes/etf.py`, `tests/test_api_etf.py` |
| Ingestion and jobs | OeKB fetching, source persistence, source-to-curated rebuilds, import logs/errors, ISIN selection, dry runs, and batch summaries. | Public API shape and reporting-view SQL. | [Technical Architecture](TECHNICAL_ARCHITECTURE.md), [Fetch missing ISINs](AgentInstructions/FETCH_ONLY_MISSING_ISINS.md), [Refresh existing ISINs](AgentInstructions/REFRESH_EXISTING_ISINS.md), `fondant/ingestion/pipeline.py`, `fondant/jobs/` |

## Where to change what

| Change | Start here | Keep separate from |
|---|---|---|
| Tax codes, metric keys, source tax names, or dictionary rows | `fondant/tax_registry.py`; use [SE-003](Validation/SE-003_TAX_CODE_REGISTRY.md) for the registry contract and run the registry tests. | Alembic view aliases and API compatibility changes unless the ticket explicitly owns them. |
| Category aliases | `fondant/tax_registry.py` for parser/source/API category keys. Preserve the documented `STF` / `stiftung` / `STI` split from [SE-004](Validation/SE-004_FOUNDATION_CATEGORY_ALIAS_DECISION.md). | Casual renames of `STI` view columns; those require a migration and consumer-impact ticket. |
| FX semantics | [SE-008](Validation/SE-008_FX_CONVERSION_SEMANTICS_DECISION.md) is the current decision record. It approves nearest-prior available ECB rates as the future default, while the current `V2_TAXDATEUR` implementation still uses exact-date `REFEXC` matching. | Parser changes, tax-code registry changes, or API numeric serialization unless a follow-up ticket owns those surfaces. |
| API output | [SE-007](Validation/SE-007_PUBLIC_API_CONTRACT.md), `fondant/api/routes/etf.py`, and `tests/test_api_etf.py`. | Ingestion jobs and reporting views; the API reads curated tax tables and does not call OeKB, ECB, or `V2_TAXDATEUR` at request time. |
| Ingestion jobs | `fondant/jobs/`, `fondant/ingestion/pipeline.py`, and the agent runbooks under `Documentation/AgentInstructions/`. | Public API response changes and schema/view migrations unless the ticket explicitly expands to those layers. |

## Compatibility Facts To Preserve

- `STF` is the canonical source/category code used by parser output, seeded
  dictionaries, and source-shaped columns.
- `stiftung` is the business/API category key for the foundation category.
- `STI` is the existing reporting-view compatibility alias for foundation
  columns in `V1_TAXDATPRE` and `V2_TAXDATEUR`; it is not a typo to clean up
  casually.
- `V2_TAXDATEUR` currently uses EUR rate `1`, exact-date `REFEXC` joins, and
  null output for missing or zero non-EUR FX rates. The approved future FX
  semantic is nearest prior available ECB rate on or before `TAXMDT`, but that
  behavior still needs a separate implementation ticket and PostgreSQL-backed
  tests.

## Practical Routing Rule

If a change describes what arrived from OeKB, start at the source layer. If it
describes what tax fact the system trusts and stores, start at the curated
layer. If it describes spreadsheet-shaped SQL columns, start at reporting
views. If it describes JSON returned to callers, start at the API contract. If
it describes fetching, retrying, dry runs, or batch failure interpretation,
start at ingestion and jobs.
