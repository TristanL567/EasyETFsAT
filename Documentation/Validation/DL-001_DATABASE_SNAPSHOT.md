# DL-001 Database Snapshot

## Purpose

`scripts/database_snapshot.py` prints a repeatable, plain-text diagnostic snapshot of the configured database. It uses the normal `DATABASE_URL` setting from `fondant.config` and masks credentials in its metadata output.

The command is read-only. It uses SQLAlchemy inspection plus `SELECT` queries for counts and coverage checks. It does not run migrations, does not create or alter schema objects, does not call ingestion code, and does not call OeKB or ECB clients.

## How to Run

From the repository root:

```powershell
py -3.10 scripts/database_snapshot.py
```

The command uses the same database configuration as the application. To point it at another local database, set `DATABASE_URL` before running it.

## Reported Sections

- `== Snapshot Metadata ==`: masked database URL, SQL dialect, and database timestamp for the run.
- `== Schema Objects ==`: inspected tables and views.
- `== Row Counts ==`: `COUNT(*)` for each inspected table and view.
- `== Report Coverage By ISIN ==`: source and curated report counts by ISIN, with observed report year and report date ranges.
- `== FX Coverage By Currency ==`: FX row counts by currency, with first and latest rate dates.
- `== Recent Ingestion Runs ==`: latest import log rows and their status, counts, timestamps, and messages.
- `== Recent Ingestion Errors ==`: grouped recent error summaries by stage and error code.
- `== Alembic Revision ==`: current `alembic_version.version_num` values when the table exists.

## Interpreting Output

Empty output in a section usually means the relevant table exists but has no rows. For example, no rows under `Report Coverage By ISIN` means there are no source or curated tax report records yet.

Missing-table messages mean the database is empty, unmigrated, or only partially migrated. For example, `(table missing: REFEXC)` under FX coverage means the snapshot could not inspect exchange-rate coverage because the FX table is not present.

Stale output should be interpreted by comparing the latest dates in the coverage sections:

- Old `latest_report_date` values suggest report data has not been refreshed recently.
- Old `latest_rate_date` values suggest FX rates may not cover recent report dates.
- Recent ingestion runs with failure statuses or repeated error summaries identify the likely ingestion stage to investigate.

Partially populated output is expected while diagnostics are being used during setup or recovery. For example, `source_reports` may be non-zero while `tax_reports` is zero if source records exist but curated report rows have not been produced.

The snapshot is diagnostic only. It is safe to run repeatedly because it performs inspection and read-only queries only.
