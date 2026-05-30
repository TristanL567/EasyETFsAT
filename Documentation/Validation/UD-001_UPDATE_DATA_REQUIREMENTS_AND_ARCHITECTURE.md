# UD-001 Update Data Requirements And Architecture

## Purpose

This contract defines the planned authenticated Update Data portal section before
implementation begins. It is a requirements and architecture note only. The
master-agent must wait for this contract to be reviewed before starting code,
migration, frontend, ingestion, Render configuration, or database mutation work.

## Planned User Workflow

The portal left navigation should add `Update Data` below `Search`. The page is
authenticated only and must not be available to anonymous users.

On the `Update Data` page, a user can enter one or more ISINs and trigger
`Update ISIN`. The submitted ISINs should be normalized consistently with the
existing ingestion path before any job is accepted.

For new ISINs, `Update ISIN` should fetch OeKB data through the existing OeKB
ingestion pipeline. For existing ISINs, it should check whether newer OeKB data
is available and refresh only when appropriate. Successful updates should make
the resulting data available to `Search` and `BusinessQuery` through the normal
database-backed reporting and API paths.

The UI should show enough status for the user to know whether the request is
queued, running, successful, or failed. Operators should also be able to inspect
failures without relying on browser state alone.

## Architecture Decisions

Production ingestion should run against the configured PostgreSQL database for
the deployed environment. The design must not depend on a local-to-prod sync
step for OeKB data.

Existing CLI jobs remain supported. The authenticated portal should reuse the
existing OeKB ingestion pipeline rather than introducing a separate ingestion
implementation.

Long ingestion should not block a normal request/response cycle if avoidable. A
job/status model is preferred before full UI-triggered execution, so the portal
can enqueue work, display status, and keep request handling responsive.

Duplicate active jobs for the same ISIN should be prevented. At minimum, an
active `queued` or `running` job for an ISIN should stop another active job for
that same ISIN from being accepted until the prior job reaches a terminal state.

Failures should be visible to the user or operator. Failed jobs should preserve
enough error context to distinguish validation errors, OeKB availability issues,
database failures, and unexpected ingestion failures.

## Expected Job States

- `queued`: accepted for ingestion or freshness check, but not started.
- `running`: actively checking OeKB or ingesting data.
- `success`: completed and any updated data is available through `Search` and
  `BusinessQuery`.
- `failed`: stopped because validation, OeKB access, ingestion, database, or
  unexpected processing failed.
- `cancelled`: optional later state if operator cancellation is introduced.
- `skipped`: optional later state if the system records that an existing ISIN
  did not require a newer OeKB refresh.

## Non-Goals

This ticket does not make implementation changes:

- No code changes.
- No migration.
- No frontend changes.
- No ingestion execution.
- No Render config changes.
- No database mutation.

## Open Questions

- Which background execution mechanism should be used on Render?
- Should the portal support batch size limits for multi-ISIN submissions?
- Is ISIN checksum validation required before accepting a job?
- May ordinary authenticated users trigger ingestion, or should this be limited
  to admin users?
- Should the application store a managed ISIN registry separate from ingestion
  jobs?

## Implementation Gate

The master-agent must wait before assigning or starting code and migration work.
Follow-up implementation tickets should reference this contract, resolve the
open questions that affect scope, and keep requirements separate from the
mechanics of the selected job runner, schema changes, and UI implementation.
