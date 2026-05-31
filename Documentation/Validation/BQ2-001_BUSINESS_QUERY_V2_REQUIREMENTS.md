# BQ2-001 BusinessQuery V2 Requirements And Data Design

Ticket: BQ2-001

Scope: documentation only. This ticket does not change application code,
tests, migrations, schema, Render configuration, or environment files.

## Objective

Define BusinessQuery V2 requirements for dual-currency result rows,
selected-year availability messaging, saved-query management, and navigation
before implementation work begins.

## Source Context

| Source | Relevant current state |
|---|---|
| `Documentation/Validation/BQ-001_SAVED_QUERY_REQUIREMENTS.md` | Defines saved query, legal subcategory, tax year, `All available years`, result, and CSV requirements for the first BusinessQuery implementation. |
| `Documentation/Validation/BQ-008_BUSINESS_QUERY_FINAL_HANDOFF.md` | Confirms saved queries, subcategories, tax-year selection, CSV export, and current known limitations. |
| `fondant/business_query.py` | Validates `BusinessQueryInput`, supports ISIN input, legal entity type, legal subcategory, amount multiplier, tax year filtering, and reads from `V2_TAXDATEUR`. |
| `fondant/api/routes/web.py` | Defines the authenticated BusinessQuery form, saved-query save/load routes, current CSV export, and one BusinessQuery navigation section. |
| `fondant/api/templates/app.html` | Renders current BusinessQuery form, results, save/load controls, CSV export, and documentation text. |
| `fondant/db/models/business_query.py` | Defines `BQSAVED` saved-query persistence scoped by authenticated username. |
| `alembic/versions/20260419_0011_refine_v1_and_add_v2_taxdateur.py` | Defines `V1_TAXDATPRE` and `V2_TAXDATEUR`, including current EUR conversion through `REFEXC`. |
| `alembic/versions/20260530_0014_add_saved_business_query_schema.py` | Creates the `BQSAVED` table, owner index, and per-user saved-query name uniqueness. |

## Current State

BusinessQuery supports saved queries. Saved queries are stored in `BQSAVED`
and are scoped to the authenticated username through `BQSUSR`.

BusinessQuery supports structured input for:

- ISIN list.
- Legal entity type.
- Legal entity subcategory.
- Tax year selection, including `All available years`.
- Amount multiplier.

The current result flow is based on `V2_TAXDATEUR`. `V2_TAXDATEUR` exposes the
report identity columns, fund/home currency as `FNDCCY`, FX metadata as
`FXRAT` and `TAXMDT`, and EUR-converted tax amount columns for the supported
tax fields and legal-entity suffixes.

Current result rows expose:

- ISIN.
- Tax year.
- OeKB report id.
- Fund currency.
- Report date.
- FX rate.
- Legal entity category.
- Tax field code and label.
- EUR base value.
- Amount multiplier.
- EUR calculated value.

Current amount multiplication is service-side: calculated value is the selected
base value multiplied by the submitted amount. Null base values remain null.

Current left navigation has BusinessQuery as one page. The page contains the
query form, result display, save action, saved-query list, load action, and CSV
export action.

## BusinessQuery V2 Result Requirements

BusinessQuery V2 must display the selected tax year in result context. For
`All available years`, the result context must clearly show `All available
years` instead of implying a single selected year.

If selected-year data is missing for an ISIN, BusinessQuery V2 must show this
message:

```text
Data for ISIN {ISIN} is not available for the selected year.
```

The missing-year message must be per affected ISIN and must not hide valid
rows for other submitted ISINs.

Each result row must display both original/home currency values and EUR values:

- Original/home currency for the ISIN/result row.
- Original-currency base value.
- Original-currency calculated value.
- EUR base value.
- EUR calculated value.
- Amount multiplier.
- FX rate used for EUR conversion.
- FX date used for EUR conversion.

The implementation must preserve:

- Amount multiplication.
- Legal entity type filtering.
- Legal entity subcategory filtering.
- `All available years` behavior.
- Existing structured input validation and whitelist boundaries.
- No raw SQL input and no arbitrary source table selection.

## Reporting View Design

BusinessQuery V2 should add a new reporting view named `V2_TAXDATHOMCCY`.

The view should expose original/home currency amounts alongside the existing
EUR-converted output shape. `V2_TAXDATEUR` should remain the current EUR-only
reporting view and compatibility boundary for existing BusinessQuery behavior.
`V2_TAXDATHOMCCY` should relate to `V2_TAXDATEUR` as a dual-currency sibling:
same report grain and tax/legal category columns, with explicit original
currency values retained and EUR conversion traceability included.

Expected identity and traceability columns:

| Column | Requirement |
|---|---|
| `TAXISN` | ISIN. |
| `TAXOKBIDN` | OeKB report id. |
| `TAXYEA` | Tax year. |
| `FNDCCY` | Original/home currency for the fund/report row. |
| `TAXMDT` | FX date or report date used to source the conversion rate. |
| `FXRAT` | FX rate used to convert original currency to EUR. |

Expected amount columns should cover each supported tax field and legal entity
category in both original currency and EUR:

| Original/home currency column | EUR column |
|---|---|
| `K40PVM_HOMCCY` | `K40PVM_EUR` |
| `K40PVO_HOMCCY` | `K40PVO_EUR` |
| `K40BVM_HOMCCY` | `K40BVM_EUR` |
| `K40BVO_HOMCCY` | `K40BVO_EUR` |
| `K40BVJ_HOMCCY` | `K40BVJ_EUR` |
| `K40STI_HOMCCY` | `K40STI_EUR` |
| `K61PVM_HOMCCY` | `K61PVM_EUR` |
| `K61PVO_HOMCCY` | `K61PVO_EUR` |
| `K61BVM_HOMCCY` | `K61BVM_EUR` |
| `K61BVO_HOMCCY` | `K61BVO_EUR` |
| `K61BVJ_HOMCCY` | `K61BVJ_EUR` |
| `K61STI_HOMCCY` | `K61STI_EUR` |
| `K62PVM_HOMCCY` | `K62PVM_EUR` |
| `K62PVO_HOMCCY` | `K62PVO_EUR` |
| `K62BVM_HOMCCY` | `K62BVM_EUR` |
| `K62BVO_HOMCCY` | `K62BVO_EUR` |
| `K62BVJ_HOMCCY` | `K62BVJ_EUR` |
| `K62STI_HOMCCY` | `K62STI_EUR` |

Column names may be refined during implementation if the service maps them
through a stable whitelist, but the view must keep both original currency and
EUR values distinct and auditable.

Source tables and lineage:

- `TAXRPT` provides report identity, ISIN, OeKB report id, tax year, tax
  currency, and report date metadata.
- `TAXDAT` provides source tax amount values.
- `TAXLIN` provides tax line/code metadata such as `K40`, `K61`, and `K62`.
- `TAXCAT` provides legal entity category keys used to pivot category-specific
  values.
- `REFEXC` provides FX rates by source currency and FX date.
- `V1_TAXDATPRE` is the existing pre-pivot lineage layer and can remain the
  original/home currency source for the new V2 view.
- `V2_TAXDATEUR` is the existing EUR-converted view and can be used as a
  behavioral reference, but `V2_TAXDATHOMCCY` should make original/home
  currency values first-class instead of requiring reconstruction.

FX traceability requirements:

- Preserve the original value before EUR conversion.
- Preserve source currency as `FNDCCY`.
- Preserve FX rate as `FXRAT`.
- Preserve FX date as `TAXMDT` or an explicitly named FX date column if later
  implementation separates report date from FX date.
- Preserve converted EUR value.
- If `FNDCCY` is `EUR`, the FX rate should be `1` and original currency values
  should match EUR values before amount multiplication.
- If FX data is missing or zero, EUR converted values should be null while
  original currency values remain visible when source amounts exist.

## Navigation Requirements

The left navigation should show BusinessQuery with two options:

- Add New Query.
- Queries.

`Add New Query` uses the current query form. It should remain the place where a
user enters ISINs, chooses legal entity type, subcategory, selected year or
`All available years`, amount, and runs a query.

`Queries` is for saved-query management. It should be the primary place to
view, group, duplicate, and edit saved queries.

The navigation split must preserve the existing authenticated boundary. It must
not introduce unauthenticated access to BusinessQuery or saved queries.

## Saved-Query Management Requirements

Users can view existing saved queries.

Users can edit existing saved queries.

Users can duplicate saved queries.

Users can group saved queries.

Groups are scoped to the authenticated username. Saved queries remain scoped to
the authenticated username. No cross-user sharing is required yet.

Saved-query groups should support low-technical-user organization without
changing execution semantics. A group is metadata for finding and managing
queries; running a query must still use the saved query's structured legal
entity type, subcategory, tax year, amount, and optional default ISINs.

No delete/archive behavior is required unless planned later. If delete or
archive is added in a later ticket, it should be explicit and separately
validated.

## Low-Technical-User Tools

BusinessQuery V2 should add these user-facing tools and explanations:

- Missing-year availability messages.
- Last-run summary.
- Query groups.
- Edit saved query.
- Duplicate saved query.
- Data freshness indicator.
- CSV export with original currency and EUR values.
- Clear explanation of original currency vs EUR.

The original currency vs EUR explanation should be short and plain: original or
home currency is the currency reported for the fund/source tax data; EUR is the
converted value using the displayed FX rate and FX date.

The data freshness indicator should identify the latest relevant data refresh
or report availability signal available to the application. If no reliable
freshness timestamp is available, the UI should say that freshness is unknown
instead of implying current data.

The last-run summary should include at minimum:

- Query name.
- Selected tax year or `All available years`.
- Submitted ISIN count.
- Result row count.
- Missing-year ISIN count when applicable.
- Run timestamp.

## CSV Export Requirements

CSV export must include original currency and EUR values. At minimum, each CSV
result row should include:

- Query name.
- ISIN.
- Selected tax year context.
- Row tax year.
- Tax field code.
- Tax field label.
- Legal entity category.
- Original/home currency.
- Original-currency base value.
- Amount multiplier.
- Original-currency calculated value.
- FX rate.
- FX date.
- EUR base value.
- EUR calculated value.

CSV export must use the same validated query input and selected-year behavior
as the rendered result flow.

## Follow-Up Tickets

| Ticket | Scope |
|---|---|
| BQ2-002 | add V2_TAXDATHOMCCY reporting view. |
| BQ2-003 | extend BusinessQuery service for dual-currency result rows. |
| BQ2-004 | add missing-year availability reporting. |
| BQ2-005 | update BusinessQuery result UI for original currency and EUR. |
| BQ2-006 | update CSV export for dual-currency results. |
| BQ2-007 | split BusinessQuery navigation into Add New Query and Queries. |
| BQ2-008 | add saved query groups schema. |
| BQ2-009 | add saved query edit flow. |
| BQ2-010 | add query management UI. |
| BQ2-011 | update documentation and user guidance. |
| BQ2-012 | final handoff. |

## Non-Goals

This ticket does not implement:

- Code changes.
- Test changes.
- Migrations.
- Schema changes.
- Render configuration changes.
- `.env` changes.
- `V2_TAXDATHOMCCY`.
- BusinessQuery service changes.
- UI changes.
- CSV behavior changes.
- Saved-query group persistence.
- Saved-query edit routes.
- Delete/archive behavior.
- Cross-user saved query sharing.

## Acceptance Criteria

- Current BusinessQuery saved-query, input, result, source-view, and navigation
  state is documented.
- New selected year, missing-year, original currency, home currency, and EUR
  result requirements are documented.
- The `V2_TAXDATHOMCCY` reporting view design, expected columns, relationship
  to `V2_TAXDATEUR`, source lineage, and FX traceability are documented.
- Navigation requirements for `Add New Query` and `Queries` are documented.
- Saved-query management requirements for view, edit, duplicate, and group are
  documented.
- Username scoping and no cross-user sharing are documented.
- Low-technical-user tools are documented.
- Follow-up tickets `BQ2-002` through `BQ2-012` are defined.
- No files outside `Documentation/Validation/` are changed.

## Validation Commands

Run:

```powershell
rg -n "V2_TAXDATHOMCCY|V2_TAXDATEUR|original currency|home currency|EUR|selected year|not available for the selected year|Add New Query|Queries|group|edit|BQSAVED" Documentation/Validation/BQ2-001_BUSINESS_QUERY_V2_REQUIREMENTS.md
git diff --check
git status --short --branch --untracked-files=all
```

## Open Risks And Questions

- The exact `V2_TAXDATHOMCCY` column names should be finalized in BQ2-002
  before service work begins.
- The project should confirm whether `TAXMDT` is sufficient as the displayed FX
  date or whether a separate FX date column should be exposed.
- Data freshness needs a source-backed definition before UI implementation.
- Query group naming and uniqueness rules should be defined with the BQ2-008
  schema ticket.
- Delete/archive remains intentionally out of scope and should not be inferred
  during BQ2 implementation unless a later ticket explicitly adds it.
