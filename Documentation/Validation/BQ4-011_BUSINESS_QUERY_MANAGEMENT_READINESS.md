# BQ4-011 BusinessQuery Management Readiness

## Scope

BQ4-011 is a regression and readiness checkpoint for the BQ4 BusinessQuery
management epic. It does not add product behavior. The reviewed workflow covers:

- Add New Query with selectable tax fields.
- Saved-query persistence of selected tax fields.
- Queries as an existing-query page with Run, Load, and Edit actions.
- Group BusinessQuery as the group creation and review page.
- Direct Run from Queries using saved default ISINs.
- Friendly handling when a saved query has no default ISINs.

## Coverage Evidence

Automated coverage is expected from:

- `tests/test_web_routes.py` for authenticated navigation, form rendering,
  direct saved-query run, owner scoping, missing default ISIN handling, saved
  query editing, group creation, documentation, and CSV/export-adjacent web
  behavior.
- `tests/test_business_query_service.py` for field selection, selected tax-year
  behavior, legal entity/subcategory filtering, and dual-currency result
  service behavior.
- `tests/test_saved_business_query_model.py` for saved-query and group schema
  behavior.
- `tests/test_migrations.py` for Alembic head and reporting-view migration
  readiness.

## Manual Visual QA Status

Browser-based visual QA was not performed during this ticket. The page-level
structure is covered by route/template tests, but final deployment should still
visually inspect:

- Add New Query field selector layout.
- Queries grouping/filter controls and Run, Load, Edit action alignment.
- Direct-run result rendering on Queries.
- Group BusinessQuery creation and empty states.

## Residual Risks

- The direct Run action depends on saved default ISINs. Users who saved only a
  reusable rule without default ISINs must Load or Edit the saved query before
  running it directly.
- The expanded reporting view must be present in the deployed database via
  `alembic upgrade head`.
- Browser-level spacing and responsive behavior should be checked after Render
  deployment because this ticket did not claim visual QA.

