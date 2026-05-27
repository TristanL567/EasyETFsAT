# Field Lineage Documentation Epic

## Intention

The goal is to make every available database field understandable and traceable.
For each field, maintainers should be able to answer:

- What does this field represent?
- What value does it hold, including null versus zero semantics?
- Where does the value come from: OeKB report list, OeKB report detail, ECB FX
  data, static seed dictionaries, application metadata, or derived SQL?
- How does the value move through the pipeline from downloaded source data to
  source-shaped tables, curated facts, reporting views, and API output?
- Which code, migration, test, or documentation file is authoritative for the
  field?

This is documentation and lineage work first. It should not change behavior,
schema, data, migrations, or API contracts unless a later ticket proves a
separate implementation change is needed and the human approves it.

## AEGIS Cross-Reference Requirement

Before planning or execution, every ticket in this epic must cross-reference
`C:\Users\Tristan Leiter\Documents\aegis-core` as read-only reference material.
The agent must load and follow all AEGIS role, contract, skill, and runbook
instructions relevant to master, ticket planning, ticket execution, validator
blocking rules, ticket scope, handoff reporting, and validation.

Do not edit `aegis-core`.

If a specifically named AEGIS skill or role contract cannot be found, the agent
must state that explicitly in its completion report before proceeding with
assumptions.

## Master Agent Instructions

You are the AEGIS master agent for the EasyETFsAT field-lineage documentation
effort.

Target repository:

```text
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT
```

Required branch:

```text
development
```

Planning, validation, and decision artifacts belong under:

```text
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation
```

Field documentation artifacts should be concise and maintainer-facing. Prefer
tables, explicit source references, and runnable SELECT-only SQL checks over
long prose.

Dispatch exactly one ticket at a time. Preserve the
`master -> worker -> validator -> master` loop. Do not start the next ticket
until the previous worker result has been validated and committed.

Commit accepted results to `development` using this format:

```text
Field Lineage Epic <ticket-id>: <short description>
```

## Source Evidence

This epic is based on:

- `fondant\db\models\*.py`
- `fondant\ingestion\pipeline.py`
- `fondant\oekb\parser.py`
- `fondant\tax_registry.py`
- `fondant\api\routes\*.py`
- `alembic\versions\*.py`
- `tests\`
- `Documentation\DATA_AND_QUERY_GUIDE.md`
- `Documentation\TECHNICAL_ARCHITECTURE.md`
- `Documentation\MAINTAINER_LAYER_GUIDE.md`
- All existing documentation under `Documentation\`, including stakeholder
  docs, agent runbooks, validation notes, and prior epic artifacts. Each ticket
  must check whether relevant documentation already exists before creating new
  content.
- Existing validation notes under `Documentation\Validation\`
- The local PostgreSQL database when available, using SELECT-only queries

The existing untracked
`Documentation\Validation\DATABASE_LINEAGE_AND_DATA_READINESS_EPIC.md` must be
preserved and not edited unless a future ticket explicitly owns it.

## Output Standard

The final documentation set should include two primary maintainer-facing
outputs:

- One comprehensive field table that lists every available database field with
  description, value meaning, source, transformation path, final-output usage,
  null/zero semantics where known, and authoritative references.
- One concise summary markdown that explains how to use the field table and
  points to the existing detailed documentation, validation notes, runbooks,
  and lineage artifacts.

Intermediate ticket artifacts may split inventory, source inputs,
transformations, and final outputs to keep review manageable, but the epic must
converge into those two primary outputs.

Avoid pretending unknown business meaning is known. Mark unclear semantics as
open questions with evidence.

## Epic Acceptance Criteria

The epic is complete when:

- Every table and view field in the current database schema is listed with a
  concise meaning, source layer, and transformation status.
- The final field dictionary is one searchable table rather than several
  competing partial catalogs.
- The final summary markdown points maintainers to the detailed field table and
  to existing documentation under `Documentation\`.
- Downloaded OeKB and ECB source fields used by the pipeline are documented.
- Final reporting/API values can be traced back to source or curated fields.
- Null versus explicit zero semantics are documented for tax matrix values.
- Known gaps are separated from confirmed behavior.
- Validators can rerun documented SELECT-only SQL checks against the local
  PostgreSQL database.

## Ticket Backlog

### FD-001: Inventory All Database Tables, Views, And Columns

```yaml
ticket_id: FD-001
goal: Create the authoritative schema inventory for all current database tables, views, and columns.
dependencies: []
business_context: Field-level lineage cannot be complete until the actual database surface is enumerated.
entropy_prevented: Future field documentation is anchored to the live schema instead of partial memory or selected examples.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
  - database schema
  - database data
  - credentials
requirements:
  - Review existing documentation under `Documentation\` before writing; reuse
    or reference prior explanations instead of duplicating them.
  - Use repo models, migrations, and SELECT-only PostgreSQL metadata queries when available.
  - List every table and view with column names, nullable status when available, type when available, and layer classification.
  - Classify each object as source, raw archive, curated tax, reference, operational log, reporting view, or metadata.
  - Include exact SQL metadata queries used.
  - Do not infer deep business meaning yet; this ticket is inventory-first.
acceptance_criteria:
  - Every current table and view column is represented once.
  - The inventory identifies which later ticket should own detailed semantics for each object group.
verification_commands:
  - git status --short --branch
  - rg -n "SOURCERPT|SOURCEAGE|SOURCERAW|TAXRPT|TAXDAT|REFEXC|IMPLOG|V1_TAXDATPRE|V2_TAXDATEUR" Documentation\Validation\FD-001_DATABASE_FIELD_INVENTORY.md
```

### FD-002: Document Downloaded OeKB Source Fields

```yaml
ticket_id: FD-002
goal: Document OeKB report-list and report-detail fields consumed by the pipeline.
dependencies:
  - FD-001
business_context: Maintainers need to know which database fields are direct source captures versus transformed curated facts.
entropy_prevented: Parser and ingestion changes can be reviewed against a source-field catalog instead of ad hoc payload reading.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
  - live OeKB calls
  - credentials
requirements:
  - Review existing documentation under `Documentation\` before writing; reuse
    or reference prior explanations instead of duplicating them.
  - Use stored `SOURCERAW` payloads, parser code, ingestion code, tests, and existing docs.
  - Identify report metadata fields, raw detail payload fields, and parsed tax matrix source names.
  - Mark whether each field is persisted raw-only, copied to source tables, transformed to curated tables, or ignored.
acceptance_criteria:
  - OeKB fields used by `SOURCERPT`, `SOURCERAW`, and `SOURCEAGE` are documented with source and destination.
verification_commands:
  - git status --short --branch
  - rg -n "OeKB|SOURCERAW|SOURCERPT|SOURCEAGE|StB_|stmId" Documentation\Validation\FD-002_OEKB_SOURCE_FIELD_CATALOG.md
```

### FD-003: Document ECB And Reference Data Fields

```yaml
ticket_id: FD-003
goal: Document ECB FX and reference/dictionary fields.
dependencies:
  - FD-001
business_context: FX and dictionary values influence final reporting values but come from different source classes than OeKB tax payloads.
entropy_prevented: Reference data, static dictionaries, and downloaded FX observations are not confused with OeKB tax facts.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
  - live ECB calls
  - database data
requirements:
  - Review existing documentation under `Documentation\` before writing; reuse
    or reference prior explanations instead of duplicating them.
  - Cover `REFEXC`, `REFCCY`, `REFCTR`, `TAXLIN`, and `TAXCAT`.
  - Explain static seed dictionaries versus downloaded ECB observations.
  - Explain how `REFEXC` participates in `V2_TAXDATEUR`.
acceptance_criteria:
  - Reference fields can be distinguished from source tax facts and derived reporting outputs.
verification_commands:
  - git status --short --branch
  - rg -n "REFEXC|REFCCY|REFCTR|TAXLIN|TAXCAT|FXRAT|ECB" Documentation\Validation\FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md
```

### FD-004: Document Source-To-Curated Tax Transformation

```yaml
ticket_id: FD-004
goal: Document how source-shaped OeKB values become curated tax/report rows.
dependencies:
  - FD-002
  - FD-003
business_context: The most important trust boundary is the source-to-curated transformation.
entropy_prevented: Missing, null, zero, and dropped values can be interpreted by lineage instead of guessing from final views.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
  - database data
requirements:
  - Review existing documentation under `Documentation\` before writing; reuse
    or reference prior explanations instead of duplicating them.
  - Cover `TAXRPT`, `TAXDAT`, `TAXADJ`, `SECDIV`, and `TAXCOR`.
  - Explain how `fondant\tax_registry.py` drives tax line/category meanings.
  - Explain null versus explicit zero behavior for tax matrix values.
  - Include the `K61BVJ` / `K62BVJ` example as a lineage case study if available.
acceptance_criteria:
  - A maintainer can trace a selected `TAXDAT` value back to source fields and understand why absent source values stay absent.
verification_commands:
  - git status --short --branch
  - rg -n "TAXRPT|TAXDAT|TAXADJ|SECDIV|TAXCOR|K61BVJ|K62BVJ|null|zero" Documentation\Validation\FD-004_SOURCE_TO_CURATED_LINEAGE.md
```

### FD-005: Document Reporting Views And API Output Lineage

```yaml
ticket_id: FD-005
goal: Document how curated values become reporting view columns and public API output fields.
dependencies:
  - FD-004
business_context: Downstream consumers use views and API responses, so final values need traceable provenance.
entropy_prevented: View/API field semantics stop being inferred from SQL aliases and route code.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
  - API behavior
  - database schema
requirements:
  - Review existing documentation under `Documentation\` before writing; reuse
    or reference prior explanations instead of duplicating them.
  - Cover `V1_TAXDATPRE`, `V2_TAXDATEUR`, `/health`, and `GET /etf/{isin}/tax`.
  - Explain direct pivots, FX conversion, exact-date FX behavior, and API table reads.
  - Document `STF` to `STI` alias behavior.
acceptance_criteria:
  - Every final reporting/API field has a documented source or derivation path.
verification_commands:
  - git status --short --branch
  - rg -n "V1_TAXDATPRE|V2_TAXDATEUR|FXRAT|GET /etf|STI|API" Documentation\Validation\FD-005_REPORTING_AND_API_FIELD_LINEAGE.md
```

### FD-006: Create Final Field Table And Summary Guide

```yaml
ticket_id: FD-006
goal: Consolidate the prior field catalogs into one comprehensive field table and one summary guide.
dependencies:
  - FD-001
  - FD-002
  - FD-003
  - FD-004
  - FD-005
business_context: The final output should be easy to use without reading every validation note.
entropy_prevented: Field knowledge stays searchable and centralized.
allowed_areas:
  - Documentation\
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
requirements:
  - Review all existing documentation under `Documentation\` before writing; preserve useful prior explanations by linking to them.
  - Create one comprehensive field table under `Documentation\` or `Documentation\Validation\`.
  - Each field-table row should include object, field, type, nullable status, description, value meaning, source, transformation path, final-output usage, null/zero semantics, detailed-owner artifact, and authoritative references.
  - Create one concise summary markdown under `Documentation\` that explains the purpose of the field table and points to existing detailed docs and validation artifacts.
  - Include a short "how to trace a field" workflow.
acceptance_criteria:
  - A new maintainer can find a field in one table, understand what it means, and follow its lineage to final output.
  - The summary markdown points to the field table and to existing detailed documentation rather than replacing it.
verification_commands:
  - git status --short --branch
  - rg -n "field dictionary|field table|lineage|source|curated|reporting|API|transformation" Documentation
```

## Recommended Execution Order

1. `FD-001` schema inventory.
2. `FD-002` OeKB source field catalog.
3. `FD-003` ECB/reference field catalog.
4. `FD-004` source-to-curated transformation lineage.
5. `FD-005` reporting/API output lineage.
6. `FD-006` final field table and summary guide.

This order starts with factual inventory, then documents source inputs, then
transformation and final outputs. That keeps business interpretation tied to
evidence instead of letting documentation drift into speculation.
