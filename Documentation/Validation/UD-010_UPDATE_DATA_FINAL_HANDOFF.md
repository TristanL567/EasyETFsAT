# UD-010 Update Data Final Handoff

Ticket: UD-010

Scope: final operations handoff for the Update Data epic. This ticket changes
documentation only. It does not change application behavior, migrations, tests,
scripts, credentials, Render configuration, or ingestion code.

## Executive Status

The Update Data epic is ready for operational handoff as a queued, operator-run
workflow. Authenticated users can request OeKB updates for one or more ISINs
from the portal. Valid requests create `INGJOB` rows, duplicate active jobs are
skipped, and an operator or scheduled command must run the job runner separately
to process queued work.

Successful jobs update the same database-backed rows used by `Search` and
`BusinessQuery`. Failed jobs preserve job status and error information for
operator review.

## Epic Summary

| Ticket | Area | Outcome |
|---|---|---|
| UD-001 | Requirements contract | Defined the authenticated Update Data section, accepted user workflow, job-state expectations, duplicate active job rule, and non-goals before implementation. |
| UD-002 | Navigation and placeholder | Added the authenticated portal navigation entry and placeholder Update Data page below `Search`. |
| UD-003 | ISIN form | Added the Update Data form for entering one or more ISINs and submitting through the `Update ISIN` action. |
| UD-004 | Parsing and checksum validation | Normalized submitted ISINs, parsed multi-ISIN input, rejected invalid entries, and applied checksum validation before accepting work. |
| UD-005 | `INGJOB` migration and model | Added the database-backed `INGJOB` job table and model needed to store queued, running, success, and failed update work. Latest expected Alembic head: `20260530_0013`. |
| UD-006 | Single-ISIN update service | Connected a single-ISIN update service to the existing OeKB ingestion/update path so completed work refreshes the normal reporting database rows. |
| UD-007 | UI job queueing | Changed the authenticated Update Data submit flow so valid ISINs enqueue `INGJOB` rows, while duplicate active jobs are skipped instead of creating parallel work. |
| UD-008 | Job runner CLI | Added the command-line runner that claims and processes queued update jobs with an operator-provided limit. |
| UD-009 | Job history UI | Added job history visibility so users and operators can inspect queued, running, success, and failed states after submission. |

## User Flow

1. User opens the authenticated Update Data page.
2. User enters one or more ISINs.
3. User clicks `Update ISIN`.
4. The application normalizes and validates submitted ISINs.
5. Valid ISINs create queued `INGJOB` rows.
6. Duplicate active jobs for the same ISIN are skipped when an existing queued
   or running job already exists.
7. An operator or external runner processes queued jobs.
8. Job history shows each job as queued, running, success, or failed.
9. After a successful job, `Search` and `BusinessQuery` can use the updated
   database rows through their existing database-backed paths.

## Migration Requirement

Run the database migration before using the Update Data queue in any
environment:

```powershell
alembic upgrade head
```

The latest expected Alembic head for this epic is `20260530_0013`. Operators
should confirm the deployed database is at that head before queueing or running
Update Data jobs.

## Job Runner Command

Local Windows development command:

```powershell
py -3.10 -m fondant.jobs.run_update_data_jobs --limit 10
```

Render shell command:

```bash
python -m fondant.jobs.run_update_data_jobs --limit 10
```

The `--limit` value controls how many queued jobs the runner attempts in one
invocation.

## Render Notes

- The web request queues jobs only.
- The runner must be invoked separately.
- No persistent background worker was configured by this epic.
- The Render shell command processes jobs against Render Postgres through the
  configured `DATABASE_URL`.
- Run `alembic upgrade head` against the Render database before processing
  queued jobs.
- For production use, Render needs a separate cron job, worker service, or
  documented operator procedure to invoke the runner.

## Known Limitations

- No automatic background execution is configured.
- No polling or AJAX refresh is implemented for live job status updates.
- No cancel or retry buttons are available in the UI.
- No batch limit enforcement exists beyond the runner `--limit` argument.
- No admin-only permission split exists beyond the current authenticated login.
- OeKB failures surface through job status and stored error context.
- Long-running ingestion should be monitored by an operator.

## Validation Evidence

Key validation commands across the epic should include:

| Area | Representative command |
|---|---|
| Web route tests | `py -3.10 -m pytest tests/test_web_routes.py` |
| Job model tests | `py -3.10 -m pytest tests -k "INGJOB or update_data_job"` |
| Update service tests | `py -3.10 -m pytest tests -k "update_data"` |
| Migration tests | `py -3.10 -m pytest tests -k "migration or alembic"` |
| Lint | `py -3.10 -m ruff check fondant tests` |

For this documentation-only handoff, the required validation commands are:

```powershell
rg -n "UD-001|UD-009|Update ISIN|INGJOB|20260530_0013|run_update_data_jobs|Render|known limitations|follow-up" Documentation/Validation/UD-010_UPDATE_DATA_FINAL_HANDOFF.md
git diff --check
git status --short --branch --untracked-files=all
```

## Recommended Follow-Ups

| Follow-up | Purpose |
|---|---|
| Render cron or worker automation | Process queued jobs without relying on manual Render shell execution. |
| Admin-only Update Data access control | Restrict ingestion-triggering controls to explicitly authorized operators. |
| Retry and cancel actions | Let operators recover failed jobs or stop queued work from the UI. |
| Batch size limits | Prevent oversized submissions before they create too many queued jobs. |
| Better live status refresh | Add polling, AJAX, or another refresh mechanism for job history state changes. |
| Production readiness checklist with DL diagnostics | Integrate Update Data operations with existing data-lineage, data-readiness, and DL diagnostic checks before production rollout. |

## Operational Decision

UD-010 closes the Update Data epic as a documented, manually operated queue.
The application flow is usable after `alembic upgrade head`, but production
operations still need an explicit Render runner strategy and the follow-up
controls listed above. The known limitations and follow-up recommendations are
explicitly listed for validator and operator review.
