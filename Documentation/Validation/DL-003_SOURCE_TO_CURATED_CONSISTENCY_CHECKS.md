# DL-003 Source-To-Curated Consistency Checks

Run the read-only database checks with:

```powershell
py -3.10 scripts/database_consistency_checks.py
```

The script uses the configured `DATABASE_URL` through the same settings path as the
database snapshot tooling, converting async SQLAlchemy drivers to synchronous drivers
for command-line execution.

## Invariants Checked

1. Every `SOURCERPT` row with `SRCSTS = 'FIN'` and matching `SOURCERAW` plus
   `SOURCEAGE` rows has a matching `TAXRPT` by ISIN and OeKB report ID.
2. Every `TAXDAT` row has a valid parent `TAXRPT` by `TAXRPTIDN` and
   `TAXOKBIDN`.
3. Every `TAXADJ` row has a valid parent `TAXRPT` by `TAXRPTIDN` and
   `TAXOKBIDN`.
4. Every `TAXDAT.TAXLINIDN` resolves to an active `TAXLIN` row
   (`TAXLIN.TAXACT = true`).
5. Every `TAXDAT.TAXCATIDN` resolves to the current `TAXCAT` dictionary. The
   current schema has no `TAXCAT.TAXACT` column, so existing `TAXCAT` rows are
   treated as the active category dictionary. If a future database adds
   `TAXCAT.TAXACT`, the script enforces that it is true.

## Interpreting Failures

Each section prints `status: PASS` or `status: FAIL`. Failing rows include the
most actionable available identifiers, including ISIN and OeKB report ID where a
report context can be resolved.

- `Source To Curated Report Alignment` failures mean source ingestion reached a
  finished report state with both raw and parsed source records, but curation did
  not leave a matching `TAXRPT`.
- `TAXDAT Parent Integrity` and `TAXADJ Parent Integrity` failures mean curated
  child rows point to a missing or mismatched `TAXRPT`.
- `TAXDAT Tax Line Dictionary Integrity` failures mean a curated tax fact points
  to a missing or inactive tax line dictionary row.
- `TAXDAT Tax Category Dictionary Integrity` failures mean a curated tax fact
  points to a missing category dictionary row, or to an inactive category if the
  deployed schema has `TAXCAT.TAXACT`.
- `Schema Diagnostics` failures mean required source or curated tables are
  unavailable, so the consistency checks cannot be trusted.

## Read-Only Confirmation

The script performs SQLAlchemy introspection and `SELECT` queries only. It does
not run migrations, does not issue DDL or DML, does not invoke ingestion, and does
not call OeKB, ECB, or any other network service.
