# GUI-014 Frontend Portal Final Handoff

Ticket: GUI-014  
Scope: final validation and handoff documentation for the Frontend User Portal epic. No product behavior, application code, tests, migrations, deployment configuration, credentials, or public API contracts were changed by this ticket.

## Executive Status

The server-rendered EasyETFsAT portal has validation coverage for the core v1 user flow:

1. Public landing page.
2. Login with config-backed credentials.
3. Authenticated portal shell with `BusinessQuery`, `Search`, and `Documentation`.
4. BusinessQuery form submission.
5. BusinessQuery result rendering.
6. BusinessQuery CSV export.
7. Search page and empty-database handling.
8. Documentation page.
9. Logout and route protection after logout.

The automated GUI route tests characterize the flow with `httpx` ASGI requests and mocked service responses where database content is not required. The broader test suite passes when pytest temp output is directed to the repo's ignored `.pytest_cache` area.

## Validated User Flow

| Flow area | Validation evidence | Status |
|---|---|---|
| Landing page | `tests/test_web_routes.py::test_root_renders_template_html` verifies HTML rendering, project name, public description, stylesheet link, and login link. `test_root_does_not_expose_internal_operations` verifies internal terms are not exposed. | Ready for v1. |
| Login | `test_login_form_renders`, `test_login_sets_http_only_signed_session_cookie`, and `test_login_fails_generically_without_session_cookie` verify the login form, signed HTTP-only session cookie, and generic failed-login behavior. | Ready for v1 with config-backed auth. |
| Authenticated portal shell | `test_app_redirects_unauthenticated_users_to_login` and `test_app_renders_for_authenticated_users` verify protected access, left navigation, signed-in username, and logout control. | Ready for v1. |
| BusinessQuery form | `test_business_query_form_renders_for_authenticated_users` verifies the form, query name, ISIN input, legal entity selector, amount field, and submit behavior. `test_business_query_get_prefills_selected_isin_from_search_link` verifies Search-to-BusinessQuery prefill. | Ready for v1 with the limitations below. |
| BusinessQuery result rendering | `test_business_query_valid_post_calls_service_and_renders_result_rows`, `test_business_query_valid_post_with_no_rows_renders_empty_state`, and invalid-input tests verify result table rendering, empty state, validation errors, and service call boundaries. | Ready for v1. |
| CSV export | `test_business_query_valid_export_returns_csv_with_expected_rows`, `test_business_query_empty_export_returns_csv_headers_only`, and unauthenticated export redirect tests verify CSV response, headers, rows, empty export, and auth guard. | Ready for v1. |
| Search page | `test_search_form_renders_for_authenticated_users`, `test_authenticated_search_with_mocked_data_renders_matching_rows`, `test_authenticated_search_renders_no_results_state`, and `test_search_renders_empty_database_state_without_searching` verify search form, matching rows, link into BusinessQuery, no-result state, empty-database state, and no ingestion trigger. | Ready for v1. |
| Documentation page | `test_documentation_page_renders_authenticated_help_content` verifies key BusinessQuery terms, tax fields, entity mappings, multiplier, CSV export notes, Search notes, and `V2_TAXDATEUR` limitation. | Ready for v1. |
| Logout | `test_logout_clears_session_and_protects_app` verifies cookie clearing and protected redirect after logout. | Ready for v1. |

## BusinessQuery Scope Confirmed

BusinessQuery is whitelist-driven and targets `V2_TAXDATEUR` only. `tests/test_business_query_service.py::test_business_query_executes_only_v2_taxdateur_with_parameterized_filters` verifies the generated statement selects from `V2_TAXDATEUR` and does not query `TAXRPT` or `TAXDAT` directly.

The service supports:

- ISIN normalization and deduplication.
- Entity mappings: `natural person` to `PVM`/`PVO`, `business` to `BVM`/`BVO`/`BVJ`, and `Stiftung` to `STI`.
- Tax fields `K40`, `K61`, and `K62`.
- Single-year and year-range filters at the service layer.
- Amount multiplication and null preservation.
- Rejection of invalid ISINs, entity types, tax fields, years, and raw-SQL-shaped constructor arguments.

## Automated Verification

| Command | Result |
|---|---|
| `py -3.10 -m pytest tests` | Initial exact command reached 78 passed and 2 skipped, then errored during pytest temp-directory creation under `C:\Users\Tristan Leiter\AppData\Local\Temp\pytest-of-Tristan Leiter`. The error was filesystem temp allocation, not a product assertion failure. |
| `py -3.10 -m pytest tests --basetemp '.pytest_cache\pytest-gui014'` | Passed: 83 passed, 2 skipped in 9.53s. The skipped tests are Docker/testcontainers-gated PostgreSQL checks. |
| `py -3.10 -m ruff check fondant tests` | Passed: all checks passed. |
| `git diff --check` | Passed. |

Note: `pyproject.toml` declares `requires-python = ">=3.11"`, while the ticket-specified verification command used `py -3.10`. The suite passed under Python 3.10 with the local environment, but deployment should follow the declared project runtime unless the Python support policy is updated intentionally.

## Render Deployment Readiness

Status: conditionally ready for a Render pilot after deployment packaging is completed.

Ready:

- The app is a FastAPI ASGI application with server-rendered templates and static CSS included as package data.
- Runtime configuration is environment-variable driven through `pydantic-settings`.
- Required production secrets are identifiable: `DATABASE_URL`, `WEB_AUTH_USERNAME`, `WEB_AUTH_PASSWORD_HASH`, and `WEB_SESSION_SECRET`.
- Database migrations exist through Alembic and are covered by automated migration tests where the environment supports them.
- The portal avoids public ingestion controls and preserves existing JSON API route coverage.

Not yet fully ready:

- No Render-specific service definition was found in the repo.
- No production Dockerfile was found in the repo.
- No committed Render start command or release/migration step documentation was found beyond local `uvicorn` quick-start guidance.
- `.env.example` does not list the web auth/session settings required for production deployment.
- Database backup, migration execution, rollback, and secret rotation procedures remain operational follow-up work.
- Docker-backed PostgreSQL tests were skipped in this local test run; treat production-like migration verification as a release gate before external deployment.

Recommended Render baseline before external users:

- Web service command: run Uvicorn against `fondant.api.main:app`, binding to `0.0.0.0` and Render's `$PORT`.
- Managed PostgreSQL with `DATABASE_URL` set as a secret.
- `WEB_SESSION_SECRET` set to a high-entropy secret, not the development default.
- `WEB_AUTH_USERNAME` and `WEB_AUTH_PASSWORD_HASH` set as secrets.
- Alembic `upgrade head` run as an explicit release step before serving traffic.
- Managed database backups enabled and documented.

## Known Limitations

- Hosted database may be empty until the ingestion or seed process is run.
- Auth is config-backed, not database-backed users.
- No saved-query persistence exists yet.
- BusinessQuery currently targets `V2_TAXDATEUR` only.
- The current BusinessQuery UI exposes query name, ISINs, legal entity type, and amount. Tax-field and year filters are supported at the service layer but are not fully exposed as portal controls in the current UI.
- CSV export includes the current stable export columns, but not every field displayed or available in the richer service result object.
- PostgreSQL migration/view validation depends on Docker/testcontainers availability and skipped in this run.

## Recommended Follow-Up Tickets

| Ticket | Purpose | Suggested scope |
|---|---|---|
| GUI-FUP-001 Saved Query Persistence | Add database-backed saved BusinessQuery definitions. | Add saved-query table, CRUD service, authenticated UI controls, tests, and documentation. |
| GUI-FUP-002 BusinessQuery Filter Controls | Expose year/year-range and tax-field controls in the portal UI. | Add server-rendered controls, validation, result tests, and CSV parity tests. |
| GUI-FUP-003 Render Deployment Packaging | Make Render deployment operationally explicit. | Add Render start/release documentation or config, `.env.example` web settings, migration step, backup notes, and rollback checklist. |
| GUI-FUP-004 Production Auth Hardening | Move beyond single config-backed operator auth if external usage expands. | Decide user model, password rotation, account lifecycle, and audit expectations. |
| GUI-FUP-005 PostgreSQL Release Gate | Make Docker-backed PostgreSQL migration/view tests a required pre-deployment gate. | Add or document CI/release execution for `pytest -m postgres` and record skip policy. |
| GUI-FUP-006 Seed/Ingestion Readiness Runbook | Prevent empty hosted portal confusion. | Document first-run ingestion/seed steps, expected row-count checks, and empty-state operator actions. |

## Handoff Decision

GUI-014 is ready for independent validator review as a documentation-only handoff. No application code changed and no product behavior was intentionally modified.
