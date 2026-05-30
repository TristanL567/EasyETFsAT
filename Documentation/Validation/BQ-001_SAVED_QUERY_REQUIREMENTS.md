# BQ-001 BusinessQuery Saved Query Requirements

Ticket: BQ-001

Scope: requirements and architecture documentation only. This ticket does not
change application behavior, migrations, tests, scripts, credentials, API
routes, templates, styles, or database objects.

## Objective

Define implementation-ready requirements for two BusinessQuery improvements
before application work begins:

1. Legal subcategory selection for natural person and business users.
2. Saved BusinessQuery definitions that authenticated users can load, adjust
   with new ISINs, and rerun.
3. Addendum requirements for tax year selection, result table layout, and tax
   field description display.

## Source Context

This document is based on the current BusinessQuery service, authenticated web
routes, server-rendered portal template, and route/service tests:

| Source | Relevant current behavior |
|---|---|
| `fondant/business_query.py` | Defines `BusinessQueryInput`, validates structured inputs, maps legal entity types to whitelisted suffixes, selects from `V2_TAXDATEUR`, and returns structured result rows. |
| `fondant/api/routes/web.py` | Defines the current BusinessQuery form fields, authenticated route guards, form validation, service call boundary, and CSV export behavior. |
| `fondant/api/templates/app.html` | Renders the authenticated BusinessQuery form, result summary, result table, CSV export button, and documentation page help text. |
| `tests/test_business_query_service.py` | Characterizes `V2_TAXDATEUR` selection, legal entity suffix mapping, amount multiplication, input validation, and raw-SQL rejection. |
| `tests/test_web_routes.py` | Characterizes authenticated form rendering, valid submission, no-row state, CSV export, documentation text, and unauthenticated redirects. |
| `Documentation/Validation/GUI-014_FRONTEND_PORTAL_FINAL_HANDOFF.md` | Confirms current portal behavior and records saved-query persistence as a known follow-up. |
| `Documentation/Validation/UD-010_UPDATE_DATA_FINAL_HANDOFF.md` | Provides local validation handoff style and notes that successful Update Data jobs refresh database rows used by BusinessQuery. |

## Current Behavior

BusinessQuery is a structured, whitelist-driven query flow. It does not accept
raw SQL, arbitrary table names, or user-selected source tables.

The service queries `V2_TAXDATEUR` only. The selected source columns are fixed
identity columns plus whitelisted amount columns derived from supported tax
fields and legal entity suffixes. The service currently supports tax fields
`K40`, `K61`, and `K62`.

The current legal entity grouping is:

| Legal entity type | Current mapped suffixes |
|---|---|
| `natural person` | `PVM`, `PVO` |
| `business` | `BVM`, `BVO`, `BVJ` |
| `Stiftung` | `STI` |

The current authenticated BusinessQuery UI input fields are:

| Field | Current behavior |
|---|---|
| Query name | Required free-text label, trimmed before execution. |
| ISINs | Required text area; accepts whitespace, comma, or semicolon separated values; normalizes to uppercase; rejects malformed ISIN-like values. |
| Legal entity type | Required selector with `natural person`, `business`, and `Stiftung`. |
| Amount | Required positive numeric amount used as `amount_multiplier`. |

The current output behavior is:

- Valid submissions render a result summary with query name, submitted ISINs,
  legal entity type, and amount.
- Matching rows render in a result table with ISIN, tax year, tax field, legal
  entity category, base value, multiplier, and calculated value.
- Empty result sets render the message `No tax rows matched the submitted ISINs.`
- Each database source row expands into one output row for every selected tax
  field and every suffix mapped by the selected legal entity type.
- `base_eur_value` is the raw value from `V2_TAXDATEUR`.
- `calculated_eur_value` is `base_eur_value * amount_multiplier`.
- Null base values are preserved as null calculated values.

The current CSV behavior is:

- Export uses the same structured BusinessQuery validation and execution path as
  the rendered result flow.
- Export returns `text/csv; charset=utf-8` with filename
  `business-query.csv`.
- CSV exports include these columns in order: `query_name`, `isin`,
  `tax_year`, `tax_field_code`, `tax_field_label`,
  `legal_entity_category`, `base_eur_value`, `amount_multiplier`, and
  `calculated_eur_value`.
- Empty exports return the CSV header row only.

The current service behavior is source-backed for tax year and tax field
metadata:

- The service already supports structured tax year filtering.
- The service already exposes tax field metadata from the existing `TAXLIN`
  source, including `TAXDSC` description text and `TAXUSE` usage text.
- The current UI does not expose a primary tax year selector and does not yet
  integrate `TAXDSC` or `TAXUSE` descriptions into the result display.

## Legal Subcategory Requirements

BusinessQuery must add a legal subcategory selector for legal entity types where
multiple business meanings exist. The selector should control which internal
suffixes are used by the service.

The UI must use non-technical primary labels. It must not expose raw internal
codes such as `PVM`, `PVO`, `BVM`, `BVO`, `BVJ`, or `STI` as the primary user
label. Help text or technical documentation may show mappings where useful.

Natural person must allow:

| User-facing subcategory | Internal suffix selection |
|---|---|
| PA mit Option | `PVM` |
| PA ohne Option | `PVO` |
| All private investor categories | `PVM`, `PVO` |

Business must allow:

| User-facing subcategory | Internal suffix selection |
|---|---|
| BV mit Option | `BVM` |
| BV ohne Option | `BVO` |
| BV jur. Person | `BVJ` |
| All business categories | `BVM`, `BVO`, `BVJ` |

Stiftung has no subcategory selector and maps to `STI`.

Implementation requirements:

- The selected subcategory/categories must be validated against the selected
  legal entity type.
- Natural person selections must never produce `BVM`, `BVO`, `BVJ`, or `STI`.
- Business selections must never produce `PVM`, `PVO`, or `STI`.
- Stiftung must not accept a client-supplied subcategory override.
- The default selection should preserve current behavior by selecting all
  categories for natural person and business.
- Result rows and CSV exports may continue to expose the internal
  `legal_entity_category` value for auditability, but the primary input control
  should use the user-facing labels above.

## Saved Query Requirements

Users must be able to save a named BusinessQuery rule and reuse it later.

A saved query stores:

| Field | Requirement |
|---|---|
| Query name | Required user-facing saved query name. |
| Legal entity type | Required; one of the supported BusinessQuery legal entity types. |
| Selected subcategory/categories | Required according to legal entity type; natural person and business can store one or all supported subcategories; Stiftung stores the implicit `STI` mapping without a visible selector. |
| Amount | Required positive numeric amount. |
| Selected tax year | Required saved run setting; stores either one specific tax year or `All available years`. |
| Note/rule description | Optional user-facing explanation of the saved rule. |
| Default ISIN list | Optional normalized list of ISINs used to prefill the run form. |
| Created timestamp | Required server-generated timestamp. |
| Updated timestamp | Required server-generated timestamp, changed on updates. |

Saved queries are per authenticated username for now. A user should only see,
load, update, and run saved queries associated with the authenticated username
from the current session.

Saved query execution requirements:

- A user can load a saved query into the BusinessQuery form.
- A loaded saved query should prefill query name, legal entity type, selected
  subcategory/categories, selected tax year, amount, note/rule description if
  displayed, and optional default ISIN list.
- A user can paste new ISINs or replace the prefilled ISINs before running.
- Rerun execution uses the same structured BusinessQuery execution path as a
  fresh form submission.
- Saved queries must not store or execute raw SQL.
- Saved queries must not allow arbitrary table selection.
- Saved queries must continue to target `V2_TAXDATEUR` through the service
  whitelist.

## Addendum Requirements

These addendum requirements must be treated as part of BQ-001 remediation and
included in follow-up implementation tickets. They do not change current
application behavior in this documentation-only ticket.

### Tax Year Selector Requirements

BusinessQuery must expose a tax year selector as a structured input.

Tax year selection requirements:

- The selector must allow choosing one specific available tax year.
- The selector must allow choosing `All available years`.
- `All available years` must preserve the current unfiltered tax year behavior.
- Specific year selection must filter rendered results to the selected tax
  year through the existing structured service path.
- CSV export must use the same tax year filter as the rendered result flow.
- Empty CSV exports for a selected tax year must still return the CSV header
  row only.
- Saved queries must store the selected tax year setting as either one specific
  tax year or `All available years`.
- Loading a saved query must prefill the stored tax year setting before rerun.
- Rerun execution must apply the stored or user-adjusted tax year setting
  through the same validated BusinessQuery execution path as a fresh run.

Implementation guidance:

- The UI should derive selectable specific years from available
  `V2_TAXDATEUR` data or another source-backed list that reflects the same
  BusinessQuery dataset.
- The saved-query storage key should be stable and semantic, such as
  `tax_year_filter`, with a reserved value for `All available years`.
- A specific tax year value should be validated as an allowed year before the
  service runs.

### Result Table Layout Requirement

The result table should use more available page width so users can inspect
BusinessQuery output with less immediate horizontal scrolling.

Layout requirements:

- The authenticated BusinessQuery result area should use more available page
  width than the current narrow content column where practical.
- The table layout should reduce immediate horizontal scroll on common desktop
  widths.
- Column sizing should prioritize readable identifiers, tax year, tax field,
  legal entity category, base value, multiplier, and calculated value.
- Overflow support must remain for small screens and unusually wide content.
- Horizontal scroll behavior should remain available as a fallback instead of
  forcing clipped or overlapping table content.

### Tax Field Description Integration

BusinessQuery results should expose tax field descriptions from existing tax
metadata.

Description requirements:

- The implementation must use existing `TAXLIN` metadata rather than a new
  hard-coded description table.
- The result display must include `TAXDSC` description text where available.
- The result display must include `TAXUSE` usage text where available.
- Missing `TAXDSC` or `TAXUSE` values must not block result rendering.
- Descriptions may be exposed through a tooltip, expandable detail row,
  expandable row detail, or compact detail panel.
- The description UI should keep the main table scannable while still making
  the meaning and usage of each tax field discoverable.
- CSV export may continue to include the current field set unless a later
  implementation ticket explicitly adds description columns.

Recommended saved query uniqueness rule:

- Enforce unique saved query names per authenticated username, or provide a
  clear duplicate-name resolution rule before implementation.

## Architecture Requirements

The implementation should keep BusinessQuery as a structured query system:

- The service API should accept legal subcategory selections as structured
  values, not raw suffix columns from untrusted input.
- Any suffix expansion should happen through a single whitelist mapping owned by
  the BusinessQuery domain.
- Database persistence should store stable semantic choices where possible,
  such as legal entity type and subcategory key, while allowing deterministic
  reconstruction of the suffix list.
- Routes should validate form input before calling the service.
- CSV export should use the same validated query object as the rendered result
  flow, including the selected tax year filter.
- Result rendering should enrich tax field rows from source-backed `TAXLIN`
  metadata when displaying `TAXDSC` and `TAXUSE`.
- The saved-query model should belong to the authenticated web workflow and
  should not change the existing public JSON ETF tax API behavior.

Suggested domain naming:

| Concept | Suggested stored key | User-facing label | Internal suffixes |
|---|---|---|---|
| Natural person with option | `natural_person_pa_with_option` | PA mit Option | `PVM` |
| Natural person without option | `natural_person_pa_without_option` | PA ohne Option | `PVO` |
| All private investor categories | `natural_person_all` | All private investor categories | `PVM`, `PVO` |
| Business with option | `business_bv_with_option` | BV mit Option | `BVM` |
| Business without option | `business_bv_without_option` | BV ohne Option | `BVO` |
| Business legal person | `business_bv_legal_person` | BV jur. Person | `BVJ` |
| All business categories | `business_all` | All business categories | `BVM`, `BVO`, `BVJ` |
| Stiftung | `stiftung` | Stiftung | `STI` |

The exact key names may change during implementation, but the implementation
must preserve the user-facing labels, legal entity boundaries, and suffix
selection rules.

## Non-Goals

This requirements ticket does not implement:

- A database model or Alembic migration.
- Service changes for subcategory filtering.
- UI changes for subcategory selection.
- UI changes for tax year selection.
- UI layout changes for result table width or horizontal scroll behavior.
- UI description integration for `TAXLIN`, `TAXDSC`, or `TAXUSE`.
- Saved query routes.
- Rerun UI behavior.
- Non-technical help text in the application.
- Final handoff validation.
- Raw SQL support.
- Arbitrary table selection.
- Cross-user saved query sharing.

## Follow-Up Implementation Tickets

| Ticket | Scope |
|---|---|
| BQ-002 | Add database model/migration for saved BusinessQuery definitions. |
| BQ-003 | Extend BusinessQuery service to accept legal subcategories. |
| BQ-004 | Update BusinessQuery UI with subcategory selector. |
| BQ-005 | Add save/load saved query routes. |
| BQ-006 | Add rerun flow with pasted/replaced ISINs. |
| BQ-007 | Add documentation/help text for non-technical users. |
| BQ-008 | Final handoff. |
| BQ-009 | Add tax year selector, including `All available years`, saved-query storage, rerun behavior, and CSV filter parity. |
| BQ-010 | Improve BusinessQuery result table width and responsive horizontal scroll behavior. |
| BQ-011 | Integrate `TAXLIN` metadata descriptions using `TAXDSC` and `TAXUSE` in the result display. |

## Acceptance Criteria

- Current behavior is explicit and source-backed.
- New natural person, business, and Stiftung subcategory requirements are
  explicit.
- Saved-query persistence, loading, ownership, and rerun requirements are
  explicit.
- Tax year selector, `All available years`, saved-query storage, rerun, and CSV
  export filter-parity requirements are explicit.
- Result table width and horizontal scroll requirements are explicit.
- `TAXLIN`, `TAXDSC`, and `TAXUSE` description integration requirements are
  explicit.
- Raw SQL and arbitrary table selection remain out of scope.
- Follow-up implementation tickets BQ-002 through BQ-011 are defined.
- No files outside `Documentation/Validation/` are changed by this ticket.

## Validation Commands

Run these commands for BQ-001:

```powershell
rg -n "PVM|PVO|BVM|BVO|BVJ|STI|PA mit Option|BV jur|saved query|V2_TAXDATEUR|raw SQL|rerun" Documentation/Validation/BQ-001_SAVED_QUERY_REQUIREMENTS.md
rg -n "tax year|All available years|TAXLIN|TAXDSC|TAXUSE|description|usage|horizontal scroll|saved query|rerun" Documentation/Validation/BQ-001_SAVED_QUERY_REQUIREMENTS.md
git diff --check
git status --short --branch --untracked-files=all
```

## Open Questions And Risks

- The exact saved-query name uniqueness rule should be confirmed before BQ-002
  and BQ-005. Unique per username is recommended.
- The current authentication model is a single config-backed authenticated
  username. Saved queries are scoped per authenticated username for now, but a
  future multi-user auth model may require migration or ownership changes.
- The current service supports year and tax-field filters, but the current UI
  does not expose them as primary controls. The addendum now requires a tax
  year selector and source-backed tax description display in later
  implementation tickets.
- The exact source of the UI's available tax year list should be selected
  during implementation. It should remain source-backed to BusinessQuery data
  and consistent with CSV export behavior.
- The final description presentation pattern should be selected during UI work:
  tooltip, expandable detail, expandable row detail, or compact detail panel are
  acceptable if they expose both description and usage without making the table
  hard to scan.
- The display language for `PA`, `BV`, and `Stiftung` labels should be reviewed
  by a domain owner before BQ-007 user-facing help text is finalized.
