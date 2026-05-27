# SE-012 Validation

## AEGIS Cross-Reference

Before validation, the read-only AEGIS reference at
`C:\Users\Tristan Leiter\Documents\aegis-core` was applied from the previously
loaded master, ticket-planner-worker, code-validator, ticket contract, swarm
contract, ticket-scope-validation, operating-discipline, and shared
orchestration instructions.

Exact AEGIS role/contract files named `Master-Planner`, `Master-Agent`, or
`Master-Validator` were not found. The canonical equivalents used for this
validation were `master`, `ticket-planner-worker`, and `code-validator`.

## Scope Decision

Accepted.

SE-012 was documentation-only and stayed inside the allowed `Documentation\`
area. It did not edit `fondant\`, `alembic\`, `tests\`, schema, runtime
credentials, Docker configuration, or application behavior.

## Findings

- `Documentation\MAINTAINER_LAYER_GUIDE.md` separates source-shaped OeKB
  storage, curated tax rows, reporting views, API output, and ingestion/job
  behavior.
- The guide includes a concise "Where to change what" section for tax codes,
  category aliases, FX semantics, API output, and ingestion jobs.
- The guide links to existing authoritative files and decision records instead
  of duplicating large registry or schema tables.
- The guide preserves the documented `STF` / `stiftung` / `STI` compatibility
  decision and the current-versus-future FX semantics from SE-008.
- `Documentation\README.md` adds the guide to the reading order.
- The unrelated untracked
  `Documentation\Validation\DATABASE_LINEAGE_AND_DATA_READINESS_EPIC.md` was
  preserved and was not included in this ticket.

## Verification

```text
git status --short --branch
rg --files Documentation
rg -n "source-shaped|curated|V2_TAXDATEUR|STF|STI|tax code|where to change" Documentation
```

All SE-012 verification commands passed.

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
  - Documentation/MAINTAINER_LAYER_GUIDE.md
  - Documentation/README.md
  - Documentation/Validation/SE-012_VALIDATION.md
layer_touched: documentation
layer_separation_preserved: true
diff_summary: Added a concise maintainer guide and README entry that route
  common future changes to the correct source, curated, reporting-view, API,
  or ingestion/job layer.
```
