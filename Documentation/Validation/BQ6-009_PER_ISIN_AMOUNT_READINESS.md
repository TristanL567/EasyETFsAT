# BQ6-009 Per-ISIN Amount Regression Readiness

Date: 2026-06-02
Branch: development
Dependency reviewed: BQ6-008 committed as `ab2f2a7`

## Scope

This note records focused regression readiness for BusinessQuery per-ISIN amount workflows after the BQ6-008 implementation. The review was validation-only: no product code was changed, and no test changes were needed because existing focused tests already cover the requested workflows.

## Coverage Reviewed

- Table mode entry: `tests/test_web_routes.py::test_business_query_input_view_setting_persists_in_cookie`, `test_business_query_add_row_preserves_table_values_and_adds_blank_row`, `test_business_query_add_row_preserves_invalid_values_and_errors`, and `test_business_query_structured_row_post_passes_positions_to_service`.
- Paste mode entry: `tests/test_web_routes.py::test_business_query_invalid_position_rows_preserve_values_and_errors` and the loaded saved-query assertions that render position rows in paste-compatible form.
- Latest common available year: `tests/test_business_query_service.py::test_business_query_most_recent_common_available_year_resolves_before_main_query`, `test_business_query_most_recent_common_available_year_returns_structured_no_common_result`, and `tests/test_web_routes.py::test_business_query_latest_common_year_post_passes_positions_to_service`.
- Per-ISIN execution: `tests/test_business_query_service.py::test_business_query_applies_position_amounts_by_matching_isin` and `test_business_query_positions_can_provide_isin_filter_without_legacy_isins`.
- Saved-query reload: `tests/test_web_routes.py::test_current_user_can_load_saved_business_query_with_position_amounts` and `test_loaded_saved_business_query_can_replace_isins_and_rerun_existing_post_flow`.
- Direct saved-query run: `tests/test_web_routes.py::test_owner_can_run_saved_business_query_with_position_amounts_from_queries_page`.
- CSV export: `tests/test_web_routes.py::test_business_query_valid_export_returns_csv_with_expected_rows`, `test_business_query_export_uses_current_submitted_fields_after_saved_query_load`, and `test_business_query_export_uses_loaded_position_amount_fields`.
- Model and migration persistence: `tests/test_saved_business_query_model.py::test_saved_business_query_optional_fields_accept_null_and_structured_lists`, `test_saved_business_query_preserves_legacy_defaults_without_ordered_positions`, and the BQSAVED/BQSPOSNS assertions in `tests/test_migrations.py`.

## Validation Results

- `py -3.10 -m pytest tests/test_web_routes.py tests/test_business_query_service.py tests/test_saved_business_query_model.py tests/test_migrations.py`
  - Result: passed, `154 passed, 1 skipped`.
  - Note: pytest emitted an existing `RequestsDependencyWarning` about installed `urllib3` and `chardet`/`charset_normalizer` versions.
- `py -3.10 -m ruff check fondant tests`
  - Result: passed.
- `git diff --check`
  - Result: passed before creating this note. Re-run after this note should remain clean because the change is documentation-only.

## Manual and Browser QA Gap

No browser visual QA was run for BQ6-009. Existing route tests validate server-rendered markup, form data preservation, saved-query reload values, direct run behavior, and CSV output, but they do not visually inspect the BusinessQuery page in a browser. A human or planner should decide before merge whether visual QA is needed for table/paste mode layout, saved-query reload presentation, and result/export controls.

## Residual Risks

- Browser-only regressions such as layout wrapping, focus order, or JavaScript-assisted input-mode switching are not covered by the focused Python regression run.
- The focused checks use mocked BusinessQuery execution for many route paths, so they validate request shaping and rendering rather than end-to-end database-backed browser behavior.
- The skipped test in the focused pytest run remains skipped by existing test configuration; this ticket did not change that behavior.

## Conclusion

BusinessQuery per-ISIN amount workflows are regression-ready from the focused automated validation available in this ticket. No narrow test gap was found, so no test files were changed.
