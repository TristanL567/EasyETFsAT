# BQ6-010 Per-ISIN Amount Final Handoff

Ticket: BQ6-010

Scope: final handoff for the BusinessQuery per-ISIN amount implementation. This
ticket changes documentation only. It does not change application behavior,
database schema, migrations, tests, scripts, Render configuration, or
environment files.

## Executive Status

BQ6 is ready for handoff as the per-ISIN amount implementation for
BusinessQuery. Users can run BusinessQuery with either the legacy global amount
or ordered per-ISIN amounts, switch between table and paste entry modes, request
the latest common available tax year, export results, and persist saved-query
positions through the `BQSAVED.BQSPOSNS` field added by BQ6-006.

Automated validation from BQ6-009 passed for focused web, service,
saved-query-model, and migration coverage. Browser visual QA was not run during
BQ6-009, so a human merge-gate review must decide whether visual QA is required
before merge.

## Epic Summary

| Ticket | Area | Outcome |
|---|---|---|
| BQ6-001 | Requirements contract | Defined the target per-ISIN amount behavior, table and paste mode expectations, latest common available year semantics, saved-query persistence needs, CSV/result display expectations, migration considerations, and validation plan. |
| BQ6-002 | Backend positions | Added backend support for ordered per-ISIN position input while preserving the existing global amount fallback for legacy flows. |
| BQ6-003 | Latest common year | Implemented BusinessQuery resolution for the latest common available tax year across submitted ISINs and returned structured no-common-year behavior when needed. |
| BQ6-004 | UI table entry | Replaced the Add New Query ISIN textarea path with a table-first per-ISIN amount input surface and paste-mode toggle. |
| BQ6-004A | UI settings | Preserved the user's BusinessQuery input-mode setting so table and paste mode choices remain stable across relevant form interactions. |
| BQ6-004B | Add-row and order | Added row-management behavior that preserves submitted per-ISIN values, validation messages, and ordered position semantics. |
| BQ6-005 | Web execution and export | Wired BusinessQuery web execution and CSV export to submitted per-ISIN positions and the latest common available year option. |
| BQ6-006 | Migration and model persistence | Added Alembic migration `20260602_0019` with nullable `BQSAVED.BQSPOSNS` storage and model support for optional structured positions. |
| BQ6-007 | Saved-query flows | Wired save, load, edit, direct run, and export flows so saved queries can round-trip persisted per-ISIN positions without losing legacy saved-query compatibility. |
| BQ6-008 | Documentation guidance | Updated BusinessQuery user guidance to describe per-ISIN amounts, paste mode, latest common available year behavior, saved-query persistence, and export expectations. |
| BQ6-009 | Regression readiness | Recorded focused automated validation evidence, residual risks, and the browser visual QA gap. Committed as `10aae65`. |

## Final User Flow

Add New Query remains the main BusinessQuery execution surface. A user selects
legal entity, subcategory, tax year, tax fields, and amount inputs. For amount
entry, the user can stay with the legacy global amount behavior or enter
per-ISIN amounts as ordered positions.

In table mode, each position row carries an ISIN and its amount. The UI
preserves row order, entered values, validation errors, and added blank rows
during form submissions. The submitted ordered positions become the
authoritative per-ISIN amount source for execution when present.

In paste mode, users can enter multiple ISIN/amount rows in a paste-friendly
shape. The paste path is covered by route validation and preserves entered
values when validation errors need to be shown.

For tax year selection, users can choose the latest common available year. The
backend resolves the most recent tax year available across all submitted ISINs
before running the main BusinessQuery. If no common year exists, the response is
structured so the UI can report that condition instead of silently falling back
to an incorrect year.

Saved-query behavior now includes persisted positions. A saved query can store
ordered per-ISIN amounts in `BQSAVED.BQSPOSNS`, load them back into Add New
Query, preserve them during edit/save cycles, run directly from Queries, and use
the loaded positions for CSV export. Legacy saved queries without `BQSPOSNS`
continue to behave as global-amount saved queries.

## Migration Requirements

BQ6 added one migration:

```text
20260602_0019 add nullable BQSAVED.BQSPOSNS
```

Run the database migration before relying on saved per-ISIN positions in any
environment whose database may not already include this migration:

```powershell
alembic upgrade head
```

The column is nullable so existing saved queries remain valid. Runtime
per-ISIN execution can work from submitted form positions, but saved-query
persistence of per-ISIN positions depends on the database having the
`BQSAVED.BQSPOSNS` column.

## Render Deployment Notes

No Render configuration was changed by BQ6-010. For Render deployment, apply
`alembic upgrade head` against the target Render database before users rely on
saved per-ISIN positions. If the Render database lacks migration
`20260602_0019`, saved-query persistence for per-ISIN positions will not be
ready even if the application code has been deployed.

Recommended Render gate:

- Confirm the deployed code includes the BQ6 implementation through BQ6-009.
- Confirm the Render database includes `BQSAVED.BQSPOSNS` via
  `alembic upgrade head`.
- Confirm operators understand that saved-query per-ISIN persistence depends on
  that migration being applied before use.

## Validation Evidence

BQ6-009 recorded these focused validation results:

```powershell
py -3.10 -m pytest tests/test_web_routes.py tests/test_business_query_service.py tests/test_saved_business_query_model.py tests/test_migrations.py
```

Observed result: `154 passed, 1 skipped`.

Additional BQ6-009 checks:

```powershell
py -3.10 -m ruff check fondant tests
git diff --check
```

Observed result: both passed.

Representative automated coverage included table-mode entry, paste-mode entry,
latest common available year resolution, per-ISIN execution, saved-query reload,
direct saved-query run, CSV export, model persistence, and migration assertions
for `BQSAVED.BQSPOSNS`.

For this documentation-only BQ6-010 handoff, required validation is:

```powershell
rg -n "BQ6-001|BQ6-009|per-ISIN|paste|latest common|saved-query|migration|Render|known limitations|follow-up" Documentation/Validation/BQ6-010_PER_ISIN_AMOUNT_FINAL_HANDOFF.md
git diff --check
git status --short --branch --untracked-files=all
```

## Known Limitations

- Browser visual QA was not run during BQ6-009. Route tests validate rendered
  markup and request behavior, but not visual layout, responsive behavior,
  focus order, or browser-only interaction details.
- Many route tests use mocked BusinessQuery execution, so they validate request
  shaping and rendering rather than full database-backed browser behavior.
- The focused BQ6-009 pytest run had one skipped test under existing test
  configuration; BQ6-009 did not change skip behavior.
- Saved per-ISIN positions require migration `20260602_0019`; environments
  without `BQSAVED.BQSPOSNS` are not ready for saved-query position
  persistence.

## Manual Merge-Gate Requirement

Before merge, a human or planner must review the BQ6-009 browser visual QA gap
and decide whether to run browser visual QA for table mode, paste mode,
saved-query reload presentation, direct-run/result controls, and CSV export
controls. If visual QA is deferred, the merge decision should explicitly accept
that browser-only layout and interaction risk remains.

## Recommended Follow-Ups

| Follow-up | Purpose |
|---|---|
| Browser visual QA | Verify table and paste mode layout, validation messaging, saved-query reload display, and result/export controls in a real browser. |
| Render migration confirmation | Add an operator checklist item that confirms `alembic upgrade head` has applied `20260602_0019` before saved per-ISIN positions are used. |
| End-to-end database-backed smoke test | Exercise a real browser or live route path against a migrated database with saved per-ISIN positions. |
| Saved-query position observability | Consider showing a compact position count or amount-source indicator in Queries so persisted per-ISIN saved queries are easier to audit. |

## Operational Decision

BQ6-010 closes the per-ISIN BusinessQuery amount sequence as a documented,
validated handoff. The implementation is ready for merge only after the human
merge gate considers the BQ6-009 browser visual QA gap and confirms target
deployment databases, including Render, have migration `20260602_0019` applied
where saved-query per-ISIN persistence is expected.
