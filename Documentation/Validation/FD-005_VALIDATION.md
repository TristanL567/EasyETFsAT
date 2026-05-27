# FD-005 Validation

## AEGIS Cross-Reference

Before validation, the read-only AEGIS reference at
`C:\Users\Tristan Leiter\Documents\aegis-core` was applied from the previously
loaded master, ticket-planner-worker, code-validator, ticket contract, swarm
contract, ticket-scope-validation, operating-discipline, and shared
orchestration instructions.

Exact AEGIS role/contract files named `Master-Planner`, `Master-Agent`, or
`Master-Validator` were not found in prior cross-reference. The canonical
equivalents used for validation were `master`, `ticket-planner-worker`, and
`code-validator`.

## Scope Decision

Accepted.

FD-005 stayed documentation-only under `Documentation\Validation\`. It did not
edit application code, migrations, tests, API behavior, schema, database data,
credentials, Docker configuration, or live OeKB/ECB behavior.

## Findings

- `Documentation\Validation\FD-005_REPORTING_AND_API_FIELD_LINEAGE.md`
  documents the final reporting and API surfaces required by the ticket:
  `V1_TAXDATPRE`, `V2_TAXDATEUR`, `GET /health`, and
  `GET /etf/{isin}/tax`.
- The document correctly distinguishes reporting views from the public API:
  views are SQL reporting projections, while the API route reads curated
  `TAXRPT`, `TAXDAT`, `TAXLIN`, and `TAXCAT` rows directly.
- Source inspection confirmed `GET /health` returns the constant
  `{"status": "ok"}`.
- Source inspection confirmed `GET /etf/{isin}/tax`:
  - uppercases the path ISIN;
  - queries exact-year `TAXRPT` rows, then falls back to null-year rows;
  - orders by `TAXRPT.meldg_datum.desc()`;
  - joins `TAXDAT`, `TAXLIN`, and `TAXCAT`;
  - serializes tax amounts as floats;
  - does not call OeKB, ECB, ingestion jobs, or `V2_TAXDATEUR`.
- Migration and PostgreSQL metadata inspection confirmed the documented view
  shapes:
  - `V1_TAXDATPRE`: 22 columns.
  - `V2_TAXDATEUR`: 24 columns, including `TAXMDT` and `FXRAT`.
- The document accurately records the `STF` / `stiftung` / `STI` boundary:
  `STF` is the source/category code, `stiftung` is the semantic/API category
  key, and `STI` is the reporting-view compatibility alias.
- Null and zero behavior is clearly separated across `V1_TAXDATPRE`,
  `V2_TAXDATEUR`, and `GET /etf/{isin}/tax`.
- Unrelated untracked epic files were preserved and not edited.

## Verification

```text
git status --short --branch
rg -n "V1_TAXDATPRE|V2_TAXDATEUR|FXRAT|GET /etf|STI|API" Documentation\Validation\FD-005_REPORTING_AND_API_FIELD_LINEAGE.md
git diff --check -- Documentation\Validation\FD-005_REPORTING_AND_API_FIELD_LINEAGE.md
```

Additional validator checks:

```text
Source inspection:
fondant\api\routes\health.py
fondant\api\routes\etf.py

rg -n "V1_TAXDATPRE|V2_TAXDATEUR|FXRAT|REFEXC|TAXCAT|TAXLIN|K61STI|K62STI|K40STI" alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py tests\test_tax_views_postgres.py tests\test_api_etf.py Documentation\Validation\SE-007_PUBLIC_API_CONTRACT.md

SQLAlchemy/PostgreSQL metadata inspection:
V1_TAXDATPRE 22 columns
V2_TAXDATEUR 24 columns
```

## Human Readability

```yaml
concise: true
unnecessary_elements_removed: true
abstraction_added: false
abstraction_rationale: null
```

## Result

```yaml
status: accepted
changed_files:
  - Documentation/Validation/FD-005_REPORTING_AND_API_FIELD_LINEAGE.md
  - Documentation/Validation/FD-005_VALIDATION.md
layer_touched: documentation
layer_separation_preserved: true
diff_summary: Added and validated the final reporting-view and API field
  lineage documentation, including API-vs-view differences, STI alias behavior,
  and absent-fact versus FX-conversion nulls.
```
