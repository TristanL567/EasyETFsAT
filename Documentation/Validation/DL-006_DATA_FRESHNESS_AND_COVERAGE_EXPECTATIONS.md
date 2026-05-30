# DL-006 Data Freshness And Coverage Expectations

## Purpose

This note defines conservative operator expectations for OeKB report coverage and
ECB FX freshness. It does not define an upstream publication schedule, and it
does not change ingestion, curation, scheduler, FX backfill, or reporting-view
behavior.

Use these checks to detect stale or incomplete data early, then investigate with
the existing diagnostics from DL-001 and DL-003.

## OeKB Coverage Expectations

The configured/storage ISIN universe should be compared against both
`SOURCERPT` and `TAXRPT`.

An ISIN is considered loaded only when both layers are present:

- source reports exist in `SOURCERPT`
- curated tax reports exist in `TAXRPT`

Interpret coverage gaps conservatively:

- Missing `SOURCERPT` means source ingestion has not loaded that ISIN.
- Missing `TAXRPT` where source rows exist means curation/readiness should be
  investigated.
- An ISIN present only in `TAXRPT` is not treated as a fully loaded ISIN for
  readiness purposes, because the source archive is the audit trail.

Current data expectations should be validated with:

```powershell
py -3.10 scripts/database_snapshot.py
py -3.10 scripts/database_consistency_checks.py
```

DL-001 shows table presence, row counts, report coverage by ISIN, and FX
coverage by currency. DL-003 checks source-to-curated consistency for source
reports that reached a finished state and have expected source detail rows.

## ECB FX Freshness Expectations

`REFEXC` should have recent rates for currencies needed by non-EUR reporting
rows. Current expected currencies are at least `USD`, `GBP`, and `CHF` unless
the database contains other non-EUR fund currencies in `TAXRPT.TAXCCY`.

Weekends and ECB holidays mean the latest `REFEXC.REFDAT` may legitimately lag
calendar today. Operators should therefore use a conservative freshness
threshold instead of requiring an exact match to today. The default readiness
threshold is:

```text
latest FX date should be within 7 calendar days of the check date
```

This threshold is an operational alerting rule only. Do not change exact-date FX
conversion semantics in `V2_TAXDATEUR` as part of this ticket.

## Readiness Command

Run the read-only readiness check from the repository root:

```powershell
py -3.10 scripts/data_readiness.py
```

The command uses configured database settings through `DATABASE_URL`, converts
async SQLAlchemy drivers to synchronous command-line drivers, and reads
`Documentation/isin_storage.csv` when it is available.

The command reports:

- ISIN universe count from `Documentation/isin_storage.csv`
- ISINs missing source reports
- ISINs with source reports but missing curated tax reports
- observed non-EUR currencies in `TAXRPT`
- `REFEXC` latest date by currency
- currencies stale beyond the 7 calendar day threshold

Readiness gaps are printed as findings and the command exits `0` when checks can
run. A non-zero exit means the database connection or required schema inspection
failed, not that a data gap was found.

## Read-Only Confirmation

The readiness check performs SQLAlchemy introspection, reads
`Documentation/isin_storage.csv`, and executes `SELECT` statements only. It does
not fetch OeKB data, does not fetch ECB data, does not mutate rows, does not run
migrations, and does not alter `V2_TAXDATEUR`.
