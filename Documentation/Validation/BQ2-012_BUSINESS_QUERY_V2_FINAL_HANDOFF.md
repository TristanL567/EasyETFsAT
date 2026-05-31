# BQ2-012 BusinessQuery V2 Final Handoff

Ticket: BQ2-012

Scope: documentation-only final handoff for BusinessQuery V2. This ticket does
not change application code, tests, migrations, Render configuration, or
environment files.

## Ticket Summary

| Ticket | Outcome |
|---|---|
| BQ2-001 | Defined the BusinessQuery V2 requirements for dual-currency rows, selected-year availability messages, `V2_TAXDATHOMCCY`, split navigation, saved-query grouping/editing, CSV export, and final handoff. |
| BQ2-002 | Added reporting view `V2_TAXDATHOMCCY` in Alembic revision `20260530_0015`, preserving original/home currency values and adding EUR-converted sibling columns with FX rate and FX date traceability. |
| BQ2-003 | Updated the BusinessQuery service to query `V2_TAXDATHOMCCY` instead of `V2_TAXDATEUR`, returning original/home currency values, EUR values, amount-scaled calculated values, FX rate, and FX date. |
| BQ2-004 | Added selected-year missing data reporting through `missing_year_isins` and the message `Data for ISIN {ISIN} is not available for the selected year.` |
| BQ2-005 | Updated the authenticated result UI to display selected tax year context, original/home currency, original/home base and calculated values, EUR base and calculated values, multiplier, FX rate, and FX date. |
| BQ2-006 | Updated CSV export to include dual-currency result data: original/home currency, home-currency base/calculated values, EUR base/calculated values, FX rate, and FX date. |
| BQ2-007 | Split BusinessQuery navigation into `Add New Query` for query execution and `Queries` for saved-query management while preserving the authenticated boundary. |
| BQ2-008 | Added saved-query group persistence through `BQGROUP` and the `BQSAVED.BQSGRPIDN` group assignment in Alembic revision `20260531_0016`. |
| BQ2-009 | Added saved-query edit behavior for owner-scoped saved queries, including query name, default ISINs, legal entity type, subcategory, tax year, amount, note, and group assignment. |
| BQ2-010 | Added the saved-query management UI for creating groups, filtering by group, viewing grouped saved queries, loading saved queries, and editing saved queries. |
| BQ2-011 | Updated user guidance and authenticated documentation text for original/home currency, EUR, FX rate/date, selected-year messages, Add New Query, Queries, saved-query reuse, and CSV export. |

## Final User Flow

1. Open the authenticated workspace and use the BusinessQuery section.
2. Choose `Add New Query` to enter or paste ISINs, name the query, choose legal
   entity type, choose subcategory, choose a tax year or `All available years`,
   enter an amount multiplier, and run the query.
3. For a specific tax year, BusinessQuery filters to that year. For `All
   available years`, BusinessQuery runs without a tax-year predicate and returns
   all matching available tax years.
4. Review dual-currency query results. Each row shows original/home currency,
   original/home base value, original/home calculated value, EUR base value,
   amount multiplier, EUR calculated value, and FX rate/date.
5. If a submitted ISIN has no data for the selected year, the result context
   shows `Data for ISIN {ISIN} is not available for the selected year.` Valid
   rows for other ISINs remain visible.
6. Use `Save query` from `Add New Query` to save the structured rule. Saved
   queries can include default ISINs, but edited saved queries may also have no
   default ISINs.
7. Choose `Queries` to create groups, filter saved queries by group, view saved
   queries grouped under `BQGROUP`, load saved queries, and edit saved queries.
8. Loading a saved query returns to the query form with the saved structured
   values. The user can replace or paste ISINs and rerun through the same
   validated BusinessQuery execution path.
9. Editing a saved query updates the owner-scoped `BQSAVED` row, including
   group assignment where applicable.
10. Export CSV from the current submitted form values. The export reruns the
    same validated query and emits dual-currency CSV output.

## Reporting View

`V2_TAXDATHOMCCY` is the BusinessQuery V2 reporting view. It is a
dual-currency sibling to `V2_TAXDATEUR`: `V2_TAXDATEUR` remains the existing
EUR-only reporting view, while `V2_TAXDATHOMCCY` exposes the same report grain
with original/home currency and EUR values side by side.

The view identity and traceability columns are:

| Column | Meaning |
|---|---|
| `TAXISN` | ISIN. |
| `TAXOKBIDN` | OeKB report id. |
| `TAXYEA` | Tax year. |
| `FNDCCY` | Original/home currency from the fund or source tax data. |
| `TAXMDT` | FX date used for conversion, sourced from report metadata. |
| `FXRAT` | FX rate used to convert original/home currency values into EUR. |

The view keeps original/home-currency values in `*_HOMCCY` columns and EUR
values in `*_EUR` columns for the supported tax field/category combinations:
`K40`, `K61`, and `K62` across `PVM`, `PVO`, `BVM`, `BVO`, `BVJ`, and `STI`.

For EUR-denominated rows, `FXRAT` is `1`. If the source currency is not EUR and
FX data is missing or zero, EUR values are null while original/home currency
values remain available when source values exist.

## BusinessQuery Result Behavior

BusinessQuery V2 reads from `V2_TAXDATHOMCCY` through a whitelisted SQLAlchemy
table definition. It does not accept raw SQL input or arbitrary source table
selection.

Each result row includes:

| Result field | Behavior |
|---|---|
| `original_currency_code` / `home_currency_code` / `fund_currency` | Original/home currency for the row, sourced from `FNDCCY`. |
| `base_home_currency_value` | Original/home-currency base tax value before applying the amount multiplier. |
| `calculated_home_currency_value` | Original/home-currency value multiplied by the submitted amount. |
| `base_eur_value` | EUR base value from the `*_EUR` column. |
| `calculated_eur_value` | EUR value multiplied by the submitted amount. |
| `fx_rate` | FX rate from `FXRAT`. |
| `fx_date` / `report_date` | FX date from `TAXMDT`; `report_date` currently mirrors the same value. |
| `missing_year_messages` | Per-ISIN selected-year availability messages when a specific selected year has no matching row. |

Null base values remain null after amount multiplication. `All available years`
does not produce missing-year messages because no selected-year availability
check applies.

## Saved-Query Management

Saved queries remain stored in `BQSAVED` and are owner-scoped by `BQSUSR`.
Groups are stored in `BQGROUP` and are owner-scoped by `BQGUSR`.

The group relationship is enforced with `BQSAVED.BQSGRPIDN` plus a composite
foreign key to `BQGROUP.BQGIDN` and `BQGROUP.BQGUSR`, so a saved query can only
reference a group owned by the same user. Group names are unique per owner
through `uq_bqgroup_user_name`. Saved query names remain unique per owner
through `uq_bqsaved_user_name`.

Current saved-query management behavior:

- Create groups from the `Queries` page.
- View saved queries grouped by `BQGROUP`, with ungrouped queries shown under
  `Ungrouped`.
- Filter the saved-query list by all groups, one group, or ungrouped.
- Load an owner-scoped saved query into `Add New Query`.
- Edit an owner-scoped saved query from `Queries`.
- Update query name, legal entity type, subcategory, tax year, amount, note,
  default ISIN list, and group assignment.

There is no cross-user sharing. Another user's `BQSAVED` or `BQGROUP` records
are not listed, loaded, edited, or assignable through the authenticated routes.

## Migration Requirements

Operators must run:

```powershell
alembic upgrade head
```

Expected Alembic head: `20260531_0016`.

This head includes:

- `20260530_0015`, which creates `V2_TAXDATHOMCCY`.
- `20260531_0016`, which creates `BQGROUP` and adds `BQSAVED.BQSGRPIDN`.

## Validation Evidence From The Epic

Validation evidence is represented by the current implementation and relevant
tests:

- `tests/test_business_query_service.py` verifies `V2_TAXDATHOMCCY` is the
  service source, `V2_TAXDATEUR` is not queried by the service, dual-currency
  result values are mapped, missing FX keeps home-currency values visible,
  selected-year filtering works, and missing selected-year messages are
  produced.
- `tests/test_web_routes.py` verifies authenticated navigation, `Add New
  Query`, `Queries`, saved-query group UI, saved-query load/edit behavior,
  selected-year display, missing-year messages, dual-currency table columns,
  dual-currency CSV headers/content, and authenticated documentation text.
- `tests/test_saved_business_query_model.py` verifies `BQGROUP`, `BQSAVED`,
  owner-scoped uniqueness, optional default ISINs, optional grouping, and group
  assignment.
- `tests/test_migrations.py` verifies fresh Alembic upgrade creates
  `V2_TAXDATEUR`, `V2_TAXDATHOMCCY`, `BQSAVED`, `BQGROUP`, the group foreign
  key, expected indexes/constraints, and Alembic revision `20260531_0016`.
- `Documentation/Validation/BQ2-001_BUSINESS_QUERY_V2_REQUIREMENTS.md`
  provides the implementation requirements for BQ2-001 through BQ2-012.
- `Documentation/Validation/BQ-008_BUSINESS_QUERY_FINAL_HANDOFF.md` provides
  the BusinessQuery V1 baseline for saved queries, subcategories, tax-year
  selection, CSV behavior, and initial known limitations.

## Known Limitations

- No saved-query delete/archive behavior.
- No cross-user sharing.
- No browser visual QA unless performed separately.
- No automated data freshness badge unless implemented elsewhere.
- No duplicate saved query action is present in the current saved-query
  management UI.
- Data freshness display remains limited to other application areas and is not
  a BusinessQuery V2 freshness badge.

## Recommended Follow-Up

- Add delete/archive saved queries.
- Add a duplicate saved query action.
- Add portfolio-level ISIN groups.
- Add a durable background worker for data updates.
- Add richer data freshness display.

## Scope Confirmation

BQ2-012 changes documentation only. No code, tests, migrations, Render config,
`.env`, staging, commit, or push is part of this ticket.

## Validation Commands

Run:

```powershell
rg -n "BQ2-001|BQ2-011|V2_TAXDATHOMCCY|V2_TAXDATEUR|original/home currency|EUR|FX rate|FX date|not available for the selected year|BQSAVED|BQGROUP|20260531_0016|alembic upgrade head|known limitations|follow-up" Documentation/Validation/BQ2-012_BUSINESS_QUERY_V2_FINAL_HANDOFF.md
git diff --check
git status --short --branch --untracked-files=all
```

## Risks And Questions

- Browser visual QA was not required for this documentation-only ticket and is
  not claimed here unless performed separately.
- BQ2-001 requested saved-query duplicate behavior, but the current route and
  template surface load/edit/group rather than duplicate; duplicate remains a
  recommended follow-up.
- The current FX date display uses `TAXMDT`; if a separate FX date is added
  later, the view, service, CSV, UI, and documentation should distinguish it
  from report date.
