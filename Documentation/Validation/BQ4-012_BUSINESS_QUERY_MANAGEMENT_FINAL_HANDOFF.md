# BQ4-012 BusinessQuery Management Final Handoff

## Epic Summary

BQ4 refined BusinessQuery management after the BQ2 dual-currency work and BQ3
display rounding. The epic delivered a clearer split between creating,
managing, running, and grouping saved BusinessQuery rules.

- BQ4-001 documented requirements for Queries, Group BusinessQuery, direct Run,
  field selection, and the expanded reporting view.
- BQ4-002 expanded `V2_TAXDATHOMCCY` so BusinessQuery can read every active
  registry-backed tax field in original/home currency and EUR.
- BQ4-003 added service-level tax field selection while preserving whitelisted
  query construction.
- BQ4-004 added the Add New Query tax field selector.
- BQ4-005 persisted selected tax fields on saved queries.
- BQ4-006 wired selected tax fields through save, load, edit, run, and export
  web flows.
- BQ4-007 separated Group BusinessQuery from Queries.
- BQ4-008 added direct Run on saved queries.
- BQ4-009 tightened Queries so it focuses on existing saved queries with Run,
  Load, and Edit actions.
- BQ4-010 updated in-app documentation.
- BQ4-011 added the readiness note and regression evidence.

## Final User Flow

Add New Query is where a user enters ISINs, chooses legal entity and
subcategory, chooses a tax year, selects one or more tax fields, enters an
amount, runs the query, exports CSV, and saves the reusable rule.

Queries is where a user reviews existing saved queries. Each saved query can be
run directly with its default ISINs, loaded into Add New Query for an adjusted
run, or edited. If a saved query has no default ISINs, direct Run shows a
friendly message instead of silently running an empty query.

Group BusinessQuery is where a user creates named groups and reviews existing
groups. Saved queries are assigned to groups while editing the saved query.

## Data And Migration Notes

The expected Alembic head after BQ4 is `20260531_0018`.

Deployment must run:

```powershell
alembic upgrade head
```

The key reporting-view migration is the BQ4 `V2_TAXDATHOMCCY` expansion. It
keeps `V2_TAXDATEUR` available and gives BusinessQuery original/home currency
and EUR values for selectable tax fields.

## Validation Evidence

The final readiness regression command passed during BQ4-011:

```powershell
py -3.10 -m pytest tests/test_web_routes.py tests/test_business_query_service.py tests/test_saved_business_query_model.py tests/test_migrations.py
```

Observed result: `129 passed, 1 skipped`.

Additional checks passed during the final tickets:

```powershell
py -3.10 -m ruff check fondant tests
git diff --check
py -3.10 -m alembic heads
```

Observed Alembic head: `20260531_0018`.

## Known Limitations

- Browser visual QA was not performed during BQ4-011 or BQ4-012. Route and
  template tests cover structure and content, but Render deployment should still
  be visually inspected.
- Direct Run requires saved default ISINs. A saved query without default ISINs
  must be loaded or edited before direct Run can execute it.
- Saved-query groups can be created and assigned, but delete/archive behavior is
  intentionally out of scope.
- Field selection is limited to registry-backed whitelisted tax fields; users
  cannot enter raw SQL or arbitrary database columns.

## Follow-Up

- Add browser-level responsive visual QA for Add New Query, Queries, and Group
  BusinessQuery.
- Consider saved-query delete/archive if the saved-query list becomes crowded.
- Consider showing a compact count of default ISINs and selected tax fields in
  the Queries table.
- Consider importing ISIN lists into saved queries from Search or Update Data.

