# UDT-001 Web-Triggered Update Execution Requirements

Ticket: UDT-001

Scope: documentation-only requirements and architecture contract for triggering
Update Data execution from the authenticated web workflow. This ticket does not
change runtime behavior, database schema, tests, scripts, Render configuration,
or ingestion code.

## Purpose

The current Update Data workflow accepts authenticated ISIN submissions at
`/app/update-data`, validates the input, creates queued `INGJOB` rows, skips
duplicate active jobs, and leaves execution to the manual runner:

```bash
python -m fondant.jobs.run_update_data_jobs --limit 10
```

This contract defines the intended next behavior before any implementation
ticket changes the FastAPI route, background execution helper, UI copy, tests,
or operations guidance.

## Source Context

The intended design builds on the existing implementation:

| Area | Existing behavior to preserve |
|---|---|
| Web route | `POST /app/update-data` authenticates the user, validates submitted ISINs, and calls the queueing path only after validation succeeds. |
| Queueing | Valid ISINs create `INGJOB` rows with `status="queued"` and message `Queued for update.` |
| Duplicate prevention | `INGJOB.active_for_isin()` treats `queued` and `running` jobs as active, and the web route skips duplicate active submissions for the same ISIN. |
| Runner | `fondant.jobs.run_update_data_jobs` opens an async database session and calls `run_update_jobs()`. |
| Job execution | `run_next_update_job()` claims the oldest queued job, marks it `running`, runs ingestion, then records `success` or `failed` with message and error detail where possible. |
| UI history | The Update Data page already renders recent `INGJOB` rows including `queued`, `running`, `success`, and `failed` states. |

## Intended Web-Triggered Behavior

1. An authenticated user submits one or more ISINs on `/app/update-data`.
2. The route validates and queues `INGJOB` rows exactly as today.
3. The route preserves duplicate active-job prevention exactly as today:
   submissions for an ISIN with an existing `queued` or `running` job are
   skipped instead of creating another active job.
4. After queueing at least one new `INGJOB` row, the app triggers
   asynchronous/background execution inside the FastAPI web process.
5. The HTTP response returns quickly. It must not wait for OeKB ingestion to
   finish.
6. The returned page tells the user that queued jobs have been submitted for
   processing.
7. The UI shows status from persisted `INGJOB` rows. It may say that work is
   queued or submitted for processing, but it must not claim completion until
   the job row says `success`.
8. The existing manual runner remains supported:

```bash
python -m fondant.jobs.run_update_data_jobs --limit 10
```

The background trigger is intended as a convenience path from the authenticated
web workflow, not as a replacement for the queue or operator recovery command.

## Execution Boundary

The trigger should wrap the existing runner/service behavior rather than adding
a second ingestion implementation. The implementation should prefer reusing the
same `run_update_jobs()` path that the manual runner uses, with a bounded limit
chosen by the follow-up implementation ticket.

The web route should schedule the trigger only after the queue transaction for
new jobs has committed successfully. If validation fails or the submission only
contains duplicate active jobs, there is no new queued work from that request
and the route should not schedule a trigger.

FastAPI `BackgroundTasks` is a likely implementation mechanism, but this
document does not require that exact API. UDT-002 should choose and document the
minimal in-process helper around the existing runner path.

## Safety Constraints

- Do not run ingestion synchronously while the HTTP request waits.
- Do not remove the `INGJOB` queue.
- Do not bypass duplicate active-job prevention.
- Do not create new infrastructure or Render config yet.
- Do not change public API routes.
- Do not change database schema in this ticket.
- Do not introduce raw SQL.
- Do not expose secrets, database URLs, credentials, or environment-specific
  connection strings in docs.
- Do not replace the manual runner command.
- Do not add a parallel ingestion code path separate from the existing update
  service and queued runner semantics.
- Do not mark UI work as complete from request-local state alone.

## Failure Semantics

If background execution fails after a job has been queued, the `INGJOB` row must
record `failed` and error detail where possible. The existing execution path
already records failed jobs through `status`, `message`, `error_detail`, and
`finished_at`; follow-up implementation should preserve that behavior.

If the web process restarts before a background trigger starts, a job may remain
`queued`. If the process restarts mid-job, a job may remain `running`. This
ticket does not introduce a schema or lease change to distinguish abandoned
`running` jobs. The manual runner must remain available so an operator can
recover or continue future queued jobs, and later tickets may define explicit
stale-running recovery if needed.

The UI must treat persisted job status as the source of truth:

| Job state | UI meaning |
|---|---|
| `queued` | Accepted and waiting for processing or recovery by a runner. |
| `running` | Claimed by execution, but not complete. |
| `success` | Completed successfully; updated data may be available through Search and BusinessQuery. |
| `failed` | Execution failed; show stored message/error detail where appropriate. |

The UI must not claim success when a request only scheduled background work.
Completion is true only after the corresponding `INGJOB` row says `success`.

## Render And Operations Constraints

This ticket does not add Render services, cron jobs, workers, release commands,
or deployment configuration. The intended next implementation runs background
execution inside the existing FastAPI web process only.

Render operations must still keep the manual runner available for recovery:

```bash
python -m fondant.jobs.run_update_data_jobs --limit 10
```

No new infrastructure is required by UDT-001. Any future Render worker, cron, or
external queue decision should be a separate operations ticket after the
in-process behavior is implemented and validated.

## Non-Goals For UDT-001

- No code changes.
- No migration.
- No tests.
- No frontend/template changes.
- No script changes.
- No Render config changes.
- No database mutation.
- No public route changes.
- No raw SQL design.
- No secret or database URL documentation.

## Likely Implementation Tickets

| Ticket | Purpose |
|---|---|
| UDT-002 | Add a background trigger helper around the existing runner path. |
| UDT-003 | Wire `POST /app/update-data` to schedule the trigger after queueing at least one new job. |
| UDT-004 | Update UI copy and status behavior so submitted jobs are described as queued/submitted, not completed. |
| UDT-005 | Add tests for trigger scheduling and no-trigger-on-invalid/duplicate-only submissions. |
| UDT-006 | Final handoff and Render/manual recovery instructions. |

## Validation Commands

Run these commands for this documentation-only ticket:

```powershell
rg -n "INGJOB|BackgroundTasks|run_update_data_jobs|manual runner|queued|running|success|failed|Render|no new infrastructure" Documentation/Validation/UDT-001_WEB_TRIGGERED_UPDATE_EXECUTION_REQUIREMENTS.md
git diff --check
git status --short --branch --untracked-files=all
```

## Acceptance Criteria

- The document is standalone and implementation-ready.
- Intended web-trigger behavior is clear.
- Safety constraints and failure semantics are explicit.
- Follow-up tickets are listed.
- No files outside `Documentation/Validation/` are changed.
- The manual runner command remains documented and supported.
