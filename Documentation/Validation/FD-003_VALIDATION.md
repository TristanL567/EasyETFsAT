# FD-003 Validation

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

FD-003 stayed documentation-only under `Documentation\Validation\`. It did not
edit application code, migrations, tests, schema, database data, credentials,
Docker configuration, or live ECB behavior.

## Findings

- `Documentation\Validation\FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md` covers
  `REFEXC`, `REFCCY`, `REFCTR`, `TAXLIN`, and `TAXCAT`.
- The artifact clearly separates downloaded ECB observations in `REFEXC` from
  static/reference dictionary tables.
- The artifact explains the current `V2_TAXDATEUR` exact-date FX behavior:
  `REFEXC.REFCCY = FNDCCY`, `REFEXC.REFDAT = TAXMDT`, `EUR` uses rate `1`,
  and missing or zero FX rates produce null converted values.
- The artifact correctly ties `TAXLIN` and `TAXCAT` to
  `fondant\tax_registry.py`, including the `STF` / `stiftung` / `STI` alias
  boundary.
- Local SELECT-only PostgreSQL validation confirmed:
  - `REFEXC`: 12,513 rows.
  - `REFCCY`: 0 rows.
  - `REFCTR`: 0 rows.
  - `TAXLIN`: 11 rows.
  - `TAXCAT`: 6 rows.
  - `REFEXC` contains `CHF`, `GBP`, and `USD`, each with 4,171 observations
    spanning `2010-01-04` through `2026-04-17`.
- The open question about `REFCCY` and `REFCTR` seed authority is justified by
  the zero local row counts and lack of an active application seed path in the
  reviewed source.
- Unrelated untracked epic files were preserved and not edited.

## Verification

```text
git status --short --branch
rg -n "REFEXC|REFCCY|REFCTR|TAXLIN|TAXCAT|FXRAT|ECB" Documentation\Validation\FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md
git diff --check -- Documentation\Validation\FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md
```

Additional validator checks:

```text
SQLAlchemy/PostgreSQL read-only inspection:
REFEXC=12513
REFCCY=0
REFCTR=0
TAXLIN=11
TAXCAT=6
REFEXC_CCY=CHF|2010-01-04|2026-04-17|4171
REFEXC_CCY=GBP|2010-01-04|2026-04-17|4171
REFEXC_CCY=USD|2010-01-04|2026-04-17|4171

rg -n "backfill_ecb_rates|fetch_latest_ecb_rates|ECBRatePoint|REFEXC|REFCCY|REFCTR|_ensure_tax_dictionaries|LINE_DICTIONARY|CATEGORY_DICTIONARY|FXRAT|V2_TAXDATEUR" fondant alembic tests Documentation\Validation\SE-008_FX_CONVERSION_SEMANTICS_DECISION.md
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
  - Documentation/Validation/FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md
  - Documentation/Validation/FD-003_VALIDATION.md
layer_touched: documentation
layer_separation_preserved: true
diff_summary: Added and validated the reference and FX field catalog for
  ECB observations, reference dictionaries, tax dictionaries, and current
  V2_TAXDATEUR exact-date FX behavior.
```
