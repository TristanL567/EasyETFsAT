# FD-002 Validation

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

FD-002 stayed documentation-only under `Documentation\Validation\`. It did not
edit application code, migrations, tests, schema, database data, credentials,
Docker configuration, or live OeKB behavior.

## Findings

- `Documentation\Validation\FD-002_OEKB_SOURCE_FIELD_CATALOG.md` documents the
  consumed OeKB source fields for `SOURCERPT`, `SOURCERAW`, and `SOURCEAGE`.
- The artifact distinguishes report-list metadata, raw report-detail payload
  storage, parsed tax matrix values, parser diagnostics, and raw-only ignored
  structural fields.
- The artifact references existing documentation and repo source instead of
  duplicating broader architecture explanations.
- The raw-payload claims were verified against local PostgreSQL using
  SELECT-only SQLAlchemy queries:
  - `SOURCERAW` rows: 38.
  - rows with top-level `list`: 38.
  - rows with nested `expandedRows`: 38.
  - rows containing
    `StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2`: 0.
- Repo source confirms the K82 source name is parser-supported in
  `fondant\tax_registry.py`, while parser diagnostics for unknown tax fields
  and invalid numeric values are covered in parser code and tests.
- Unrelated untracked epic files were preserved and not edited.

## Verification

```text
git status --short --branch
rg -n "OeKB|SOURCERAW|SOURCERPT|SOURCEAGE|StB_|stmId" Documentation\Validation\FD-002_OEKB_SOURCE_FIELD_CATALOG.md
git diff --check -- Documentation\Validation\FD-002_OEKB_SOURCE_FIELD_CATALOG.md
```

Additional validator checks:

```text
SQLAlchemy/PostgreSQL read-only inspection:
sourceraw_rows=38
top_level_list_rows=38
rows_with_expandedRows=38
k82_source_name_rows=0

rg -n "StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2|TAX_FIELD_MAP|CATEGORY_KEY_MAP|unknown_tax_field|invalid_numeric_value" fondant\tax_registry.py fondant\oekb\parser.py tests\test_oekb_parser.py
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
  - Documentation/Validation/FD-002_OEKB_SOURCE_FIELD_CATALOG.md
  - Documentation/Validation/FD-002_VALIDATION.md
layer_touched: documentation
layer_separation_preserved: true
diff_summary: Added and validated the OeKB source field catalog for report
  metadata, raw payload capture, parsed tax matrix fields, parser diagnostics,
  and source-backed open questions.
```
