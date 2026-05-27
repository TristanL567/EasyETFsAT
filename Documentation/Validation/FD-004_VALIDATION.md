# FD-004 Validation

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

Accepted with a validator documentation clarification.

FD-004 stayed documentation-only under `Documentation\Validation\`. It did not
edit application code, migrations, tests, schema, database data, credentials,
Docker configuration, or live OeKB/ECB behavior.

During validation, the case-study wording was clarified to distinguish global
field availability from the specific `LU0380865021` / `611854` report. The
clarification is documentation-only and within FD-004 scope.

## Findings

- `Documentation\Validation\FD-004_SOURCE_TO_CURATED_LINEAGE.md` explains the
  source-to-curated trust boundary from `SOURCERPT`, `SOURCERAW`, and
  `SOURCEAGE` into `TAXRPT`, `TAXDAT`, `TAXADJ`, `SECDIV`, and `TAXCOR`.
- The document correctly describes registry-driven lineage through
  `fondant\tax_registry.py`, parser metric/category keys, source matrix
  columns, `TAXLIN`, `TAXCAT`, and `TAXDAT`.
- Null versus explicit zero behavior is correctly separated:
  - missing source values remain null in `SOURCEAGE` and create no `TAXDAT`
    row;
  - explicit zero values are retained as curated facts;
  - final view nulls can represent absent facts, while FD-003 covers nulls
    caused by missing or zero FX.
- Read-only PostgreSQL validation confirmed the aggregate case-study counts:
  - `SOURCEAGE`: 38 rows, 6 `K61BVJ` present, 6 `K62BVJ` present, 0
    `K61BVJ` zeros, 5 `K62BVJ` zeros.
  - `TAXDAT`: 1,774 rows, 6 `K61/BVJ` rows, 6 `K62/BVJ` rows, 0
    `K61/BVJ` zeros, 5 `K62/BVJ` zeros.
  - `V1_TAXDATPRE`: 38 rows, same present/zero counts.
  - `V2_TAXDATEUR`: 38 rows, same present/zero counts.
- The specific `LU0380865021` / `611854` report was also checked:
  - `SOURCEAGE.SRCK61BVJ` is null.
  - `SOURCEAGE.SRCK62BVJ` is null.
  - there are no matching `TAXDAT` rows for `K61 / BVJ` or `K62 / BVJ`.
  - therefore, for that report, the absence predates reporting views and is
    not an FX or view-pivot issue.
- Unrelated untracked epic files were preserved and not edited.

## Verification

```text
git status --short --branch
rg -n "TAXRPT|TAXDAT|TAXADJ|SECDIV|TAXCOR|K61BVJ|K62BVJ|null|zero|LU0380865021" Documentation\Validation\FD-004_SOURCE_TO_CURATED_LINEAGE.md
git diff --check -- Documentation\Validation\FD-004_SOURCE_TO_CURATED_LINEAGE.md
```

Additional validator check:

```text
SQLAlchemy/PostgreSQL read-only inspection:
sourceage_counts=(38, 6, 6, 0, 5)
taxdat_counts=(1774, 6, 6, 0, 5)
v1_counts=(38, 6, 6, 0, 5)
v2_counts=(38, 6, 6, 0, 5)
specific_sourceage=('LU0380865021', 611854, None, None)
specific_taxdat=[]
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
  - Documentation/Validation/FD-004_SOURCE_TO_CURATED_LINEAGE.md
  - Documentation/Validation/FD-004_VALIDATION.md
layer_touched: documentation
layer_separation_preserved: true
diff_summary: Added and validated the source-to-curated lineage documentation,
  including null-versus-zero semantics and the K61BVJ/K62BVJ case study.
```
