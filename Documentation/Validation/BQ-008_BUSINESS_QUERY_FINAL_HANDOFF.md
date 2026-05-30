# BQ-008 BusinessQuery Final Handoff

Ticket: BQ-008

Scope: documentation-only final handoff for BusinessQuery saved queries,
subcategory selection, tax-year selection, tax-field descriptions, and CSV
behavior. No code, tests, migrations, Render config, or environment files are
changed by this ticket.

## Ticket Summary

| Ticket | Outcome |
|---|---|
| BQ-001 | Defined implementation-ready requirements for BusinessQuery legal subcategories, saved query rules, tax-year selection including `All available years`, result-table description behavior, and follow-up work. |
| BQ-002 | Added saved-query persistence through the `BQSAVED` table/model and Alembic migration `20260530_0014_add_saved_business_query_schema.py`. |
| BQ-003 | Extended the structured BusinessQuery service to validate legal subcategories, expand them through whitelist mappings, and preserve the existing `V2_TAXDATEUR` query boundary. |
| BQ-004 | Updated the authenticated BusinessQuery UI with legal entity, subcategory, tax-year, note, save, load, run, and CSV export controls. |
| BQ-005 | Added saved query save/load routes scoped to the authenticated user, with duplicate-name handling and no raw SQL or arbitrary table selection. |
| BQ-006 | Confirmed the rerun workflow: load a saved query, replace or paste ISINs, and rerun through the same validated BusinessQuery POST path. |
| BQ-007 | Added user-facing help text for subcategories, `All available years`, saved query reuse, amount multiplier behavior, CSV exports, and tax field metadata. |

## Final User Flow

1. Open the authenticated BusinessQuery page.
2. Enter a custom query name or load a saved query rule.
3. Choose a legal entity type.
4. Choose a subcategory where applicable.
5. Choose a specific tax year or `All available years`.
6. Enter the amount multiplier.
7. Enter or paste ISINs.
8. Run query.
9. Review rows and tax field details.
10. Save query rule when the structured inputs should be reusable.
11. Load saved query later.
12. Replace ISINs or paste a new ISIN list.
13. Rerun the query through the normal BusinessQuery execution path.
14. Export CSV from the currently submitted form values.

## Legal Entity And Subcategory Mappings

| User selection | Internal suffix selection |
|---|---|
| `PA mit Option` | `PVM` |
| `PA ohne Option` | `PVO` |
| all private investor categories | `PVM`, `PVO` |
| `BV mit Option` | `BVM` |
| `BV ohne Option` | `BVO` |
| `BV jur. Person` | `BVJ` |
| all business categories | `BVM`, `BVO`, `BVJ` |
| `Stiftung` | `STI` |

Legal entity boundaries are enforced before service execution:

- `natural person` accepts `natural_person_pa_with_option`,
  `natural_person_pa_without_option`, or `natural_person_all`; these map only to
  `PVM`, `PVO`, or both.
- `business` accepts `business_bv_with_option`,
  `business_bv_without_option`, `business_bv_legal_person`, or `business_all`;
  these map only to `BVM`, `BVO`, `BVJ`, or all three.
- `Stiftung` accepts only `stiftung`; it maps to `STI`.

## Saved Query Persistence

Saved-query persistence uses SQLAlchemy model `BQSAVED` and database table
`BQSAVED`.

Owner scoping:

- Saved queries are scoped by `BQSUSR`, exposed in the model as
  `owner_username`.
- Save, list, and load operations use the authenticated username.
- Another user's saved query is not displayed and cannot be loaded by ID.

Per-user unique query names:

- Constraint `uq_bqsaved_user_name` enforces uniqueness on `BQSUSR` and
  `BQSNAM`.
- The same `BQSNAM` can be used by different owners.
- A duplicate saved query name for the same owner returns a validation message
  instead of exposing database error details.

Stored fields:

| Model field | Database column | Purpose |
|---|---|---|
| `id` | `BQSIDN` | Primary key. |
| `created_at` | `BQSCRTDTS` | Server-created timestamp. |
| `updated_at` | `BQSUPDDTS` | Server-updated timestamp. |
| `owner_username` | `BQSUSR` | Authenticated saved query owner. |
| `query_name` | `BQSNAM` | User-facing saved query name. |
| `legal_entity_type` | `BQSLENTYP` | Structured legal entity type. |
| `subcategory_key` | `BQSSUBCAT` | Structured subcategory key. |
| `tax_year_filter` | `BQSTXYR` | Specific year or `all_available_years`. |
| `amount` | `BQSAMT` | Positive amount multiplier. |
| `note` | `BQSNOTE` | Optional user-facing rule description. |
| `default_isins` | `BQSISNS` | Optional normalized default ISIN list. |

## Tax Year Behavior

Tax year selection is part of the validated BusinessQuery input.

- A specific year filter, such as `2025`, adds a `TAXYEA = 2025` predicate to
  the `V2_TAXDATEUR` query.
- `All available years` is stored and submitted as `all_available_years`; it
  preserves the unfiltered tax-year behavior.
- Saved queries persist `tax_year_filter` and load it back into the form.
- Reruns may use the saved tax year or a user-adjusted tax year.
- CSV export has parity with rendered results because it rebuilds the same
  validated `BusinessQueryInput` from the submitted form fields.
- Empty CSV results still return the CSV header row.

## Tax Field Description Behavior

Result rows expose tax field metadata from `TAX_LINES`.

- `TAX_LINES` provides the tax field code, German label, description, and usage.
- The result table displays the row's tax field code and label, then exposes
  metadata in row details when available.
- Documentation also lists the tax field code, German label, description, and
  usage for every `TAX_LINES` entry.
- Missing metadata does not block result rendering.
- CSV currently exports calculation columns only: query name, ISIN, tax year,
  tax field code, tax field label, legal entity category, base EUR value,
  amount multiplier, and calculated EUR value.

## Migration Requirement

Operators must run:

```powershell
alembic upgrade head
```

Expected Alembic head: `20260530_0014`.

This head creates the `BQSAVED` table, per-user unique saved query constraint,
and owner index required by saved-query list/load behavior.

## known limitations

- No saved-query edit/delete yet.
- No cross-user sharing.
- No dynamic tax-year discovery if implementation used fixed list; current UI
  uses a conservative rolling server-provided list.
- No advanced portfolio grouping.
- CSV metadata columns for tax field description and usage are not included yet.

## Recommended follow-up

- Add edit/delete saved queries.
- Add richer year discovery backed by available BusinessQuery data.
- Add portfolio-level saved ISIN groups.
- Add CSV metadata columns if users need tax field description and usage in
  exported files.

## Validation

Run:

```powershell
rg -n "BQ-001|BQ-007|PVM|PVO|BVM|BVO|BVJ|STI|saved query|All available years|TAX_LINES|description|usage|20260530_0014|alembic upgrade head|known limitations|follow-up" Documentation/Validation/BQ-008_BUSINESS_QUERY_FINAL_HANDOFF.md
git diff --check
```
