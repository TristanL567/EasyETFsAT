# UDT-006 Web-Triggered Update Final Handoff

Ticket: UDT-006

Scope: final documentation handoff for the Update Data Web Trigger epic. This
ticket changes documentation only. It does not change application behavior,
database schema, tests, scripts, Render configuration, credentials, or ingestion
code.

## Executive Status

The Update Data web trigger is implemented as an intermediate in-process
background execution mode. Authenticated users submit ISINs on
`/app/update-data`; valid submissions create queued `INGJOB` rows; duplicate
active jobs are skipped; and when at least one new queued job exists, the
FastAPI web process schedules background execution.

The persisted `INGJOB` table remains the source of truth. The page may tell a
user that work was queued or submitted for processing, but completion is true
only when the persisted job row says `success`. Failed execution is recorded as
`failed` with message and error detail where available.

## UDT-001 Through UDT-005 Summary

| Ticket | Area | Outcome |
|---|---|---|
| UDT-001 | Requirements contract | Defined the intended web-triggered behavior, duplicate active-job rule, persisted job-table source of truth, manual runner continuity, Render constraints, and failure semantics before implementation. |
| UDT-002 | Background helper | Added the `run_queued_update_jobs(limit=10)` helper path so the web process can open an app database session and delegate to the existing queued update runner behavior. |
| UDT-003 | Web route trigger | Wired `POST /app/update-data` to accept `BackgroundTasks`, queue validated `INGJOB` rows, skip duplicate active jobs, and schedule execution only when new jobs were queued. |
| UDT-004 | UI copy and status | Updated the authenticated Update Data page so users see that valid submissions are queued and processing starts automatically, while the job table remains the source of truth for `queued`, `running`, `success`, and `failed`. |
| UDT-005 | Tests and validation | Added coverage for successful trigger scheduling, invalid submission behavior, duplicate-only submissions, duplicate active-job skips, helper delegation, runner limits, and contained background failure logging. |

## Final User Flow

1. A signed-in user opens `/app/update-data`.
2. The user submits one or more ISINs.
3. The app normalizes, deduplicates, validates format and checksum, and queues
   accepted ISINs as `INGJOB` rows with `status="queued"`.
4. Duplicate active jobs are skipped when an existing `queued` or `running`
   `INGJOB` already exists for the same ISIN.
5. If the request creates at least one new queued job, FastAPI
   `BackgroundTasks` schedules background execution in the web service process.
6. If the request is invalid or contains only duplicate active jobs, no
   background execution is scheduled.
7. The persisted `INGJOB` table remains the source of truth for status,
   message, error detail, and timestamps.
8. Users and operators read job state from the page history:
   `queued`, `running`, `success`, or `failed`.

## Runtime Behavior

The web route schedules `_run_queued_update_jobs_background(limit=10)` after new
jobs are queued. That wrapper calls:

```python
run_queued_update_jobs(limit=10)
```

`run_queued_update_jobs()` opens an application database session and uses the
existing runner path through `run_update_jobs(session, limit=limit)`. The runner
claims queued work, marks a job `running`, runs the existing single-ISIN update
service, then records `success` or `failed`.

The manual runner remains available for operator recovery and explicit
execution:

```bash
python -m fondant.jobs.run_update_data_jobs --limit 10
```

The manual runner uses the same queued execution semantics and is not replaced
by the web trigger.

## Render Behavior

No extra infrastructure is required for this intermediate mode. The FastAPI web
service can queue work and start background execution from the authenticated web
request.

Operational constraints on Render:

- The web service must remain alive while a background task runs.
- If the web process restarts before the background task starts, affected jobs
  may remain `queued`.
- If the web process restarts while a job is already claimed, affected jobs may
  remain `running`.
- The `INGJOB` table is still the operational source of truth after restarts,
  not request-local state.
- Operators can still use the manual runner:

```bash
python -m fondant.jobs.run_update_data_jobs --limit 10
```

This mode is intentionally not a durable worker design. Render Cron, a Render
Background Worker, or another durable queue/worker mechanism should be handled
as a follow-up operations ticket.

## Validation Evidence From UDT-002 Through UDT-005

| Area | Evidence |
|---|---|
| Background helper | Tests verify `run_queued_update_jobs()` opens a configured session, delegates to `run_update_jobs()`, defaults to limit `10`, and passes through explicit limits. |
| Existing runner path | Tests verify `run_update_jobs()` honors limits, processes the oldest queued job first, marks work `running`, and records `success` or `failed`. |
| Manual runner | Tests verify `python -m fondant.jobs.run_update_data_jobs --limit 10` semantics through the CLI runner function, including processed and no-queued summaries. |
| Web trigger scheduling | Route tests verify valid submissions create queued `INGJOB` rows and schedule background execution with limit `10`. |
| Duplicate active jobs | Route and queue tests verify active `queued` or `running` jobs are skipped and are not duplicated. |
| Duplicate-only submissions | Route tests verify duplicate-only valid submissions do not schedule a background trigger. |
| Invalid submissions | Route tests verify blank, malformed, or checksum-invalid submissions create no jobs and schedule no background trigger. |
| Background failure containment | Route tests verify a background trigger exception is logged and contained while the queued `INGJOB` row remains persisted. |
| UI status copy | Route tests verify the Update Data page says valid submissions are queued, processing starts automatically, the job table is the source of truth, and status meanings cover `queued`, `running`, `success`, and `failed`. |

## Known Limitations

- No durable worker exists yet.
- No polling/AJAX refresh exists for live job status.
- No stale-running recovery exists yet.
- No retry controls exist.
- No cancel controls exist.
- Background execution depends on the current web process staying alive.
- Interrupted jobs may remain `queued` or `running` until a later operational
  process or follow-up feature handles recovery.

## Recommended Follow-Ups

| Follow-up | Purpose |
|---|---|
| Render Cron or Background Worker | Process queued jobs durably without relying on request-triggered in-process background execution. |
| Stale-running recovery | Detect and recover jobs left `running` after process interruption. |
| UI refresh/polling | Refresh job history without requiring a manual page reload. |
| Retry controls | Let operators requeue or rerun failed jobs intentionally. |
| Cancel controls | Let operators cancel queued work before it is claimed. |

## Operational Validation Commands

Run these commands for this documentation-only handoff:

```powershell
rg -n "UDT-001|UDT-005|run_queued_update_jobs|INGJOB|BackgroundTasks|Render|manual runner|queued|running|success|failed|known limitations|follow-up" Documentation/Validation/UDT-006_WEB_TRIGGERED_UPDATE_FINAL_HANDOFF.md
git diff --check
git status --short --branch --untracked-files=all
```

## Acceptance Criteria Confirmation

- Documentation is standalone and operationally useful.
- Final behavior and Render constraints are explicit.
- Validation evidence and known limitations are explicit.
- The manual runner remains documented.
- No code, tests, migrations, scripts, configuration, or `.env` files are
  changed by UDT-006.
- The only intended changed file is
  `Documentation/Validation/UDT-006_WEB_TRIGGERED_UPDATE_FINAL_HANDOFF.md`.
