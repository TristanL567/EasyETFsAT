# DL-008 Operator Data Readiness Runbook

Use this runbook before reporting, API use, or frontend `BusinessQuery` use.
Run commands from the repository root on the intended environment. The default
readiness path is read-only and must not fetch OeKB data, fetch ECB FX data,
run migrations, or fill dictionaries.

## Read-Only Readiness Checks

Run these checks in order:

```powershell
docker compose ps
py -3.10 -m alembic heads
py -3.10 scripts/database_snapshot.py
py -3.10 scripts/database_consistency_checks.py
py -3.10 scripts/ingestion_health.py
py -3.10 scripts/data_readiness.py
py -3.10 scripts/reporting_view_readiness.py
```

## Operator Questions

### Is Postgres reachable?

Use `docker compose ps` first. The PostgreSQL service should be running before
any Python diagnostic is expected to succeed. Then run
`py -3.10 scripts/database_snapshot.py`; connection errors mean the configured
`DATABASE_URL` cannot reach the database.

### Is the schema current?

Use `py -3.10 -m alembic heads` to see the repository migration head, then use
the `== Alembic Revision ==` section from
`py -3.10 scripts/database_snapshot.py` to compare the database revision. The
database should be at the expected head before reporting or API use.

### Are required tables and views present?

Use `py -3.10 scripts/database_snapshot.py`. Missing-table or missing-view
messages for core objects such as `SOURCERPT`, `TAXRPT`, `REFEXC`, `IMPLOG`,
`V1_TAXDATPRE`, or `V2_TAXDATEUR` mean the database is not ready for normal
reporting use.

### Are ISINs loaded?

Use `py -3.10 scripts/database_snapshot.py` for report coverage by ISIN and row
counts in `SOURCERPT`, `SOURCERAW`, `SOURCEAGE`, and curated tax tables. Empty
source rows mean OeKB source ingestion has not loaded those ISINs in this
database.

### Are source and curated rows aligned?

Use `py -3.10 scripts/database_consistency_checks.py`. Treat missing curated
rows where matching source rows exist as readiness gaps unless the source record
is intentionally not curatable under the documented lineage contract.

### Is ingestion healthy?

Use `py -3.10 scripts/ingestion_health.py`. Review `IMPLOG` run status, recent
failures, stale in-progress runs, and error classification before trusting new
or recently refreshed source data.

### Is FX fresh enough?

Use `py -3.10 scripts/data_readiness.py`. For `REFEXC`, the operational
freshness expectation is that the latest FX date is within 7 calendar days of
the check date, allowing normal weekend and ECB holiday lag. Freshness does not
change current exact-date `V2_TAXDATEUR` conversion behavior.

### Are V1_TAXDATPRE and V2_TAXDATEUR usable?

Use `py -3.10 scripts/reporting_view_readiness.py`. `V1_TAXDATPRE` is usable
when source and curated tax facts are present for the requested reports.
`V2_TAXDATEUR` is usable when matching `V1_TAXDATPRE` rows exist and EUR or
exact-date non-EUR `REFEXC` rates make converted values available. Null
EUR-converted values can indicate missing source tax facts, missing exact-date
FX, or zero/null FX rates.

## Optional Mutating Fill Commands

Do not run these as part of the default read-only readiness pass. They modify
database state and may call external data sources.

```powershell
py -3.10 -m fondant.jobs.fetch_missing_isins
py -3.10 -m fondant.jobs.refresh_existing_isins
py -3.10 -m fondant.jobs.refresh_tax_dictionaries
```

Use `py -3.10 -m fondant.jobs.fetch_missing_isins` only when storage contains
ISINs that are missing from source tables. Use
`py -3.10 -m fondant.jobs.refresh_existing_isins` only when already loaded
ISINs need a source refresh. Use
`py -3.10 -m fondant.jobs.refresh_tax_dictionaries` only when static tax
dictionaries need to be filled or refreshed.

ECB FX backfill or latest-rate fetch commands are also mutating because they
populate `REFEXC`. They should be approved as a fill operation and must not be
included in the default read-only readiness workflow.

## Diagnostic References

- [Data and Query Guide](../DATA_AND_QUERY_GUIDE.md)
- [DL-001 Database Snapshot](DL-001_DATABASE_SNAPSHOT.md)
- [DL-002 Database Lineage Contract](DL-002_DATABASE_LINEAGE_CONTRACT.md)
- [DL-003 Source to Curated Consistency Checks](DL-003_SOURCE_TO_CURATED_CONSISTENCY_CHECKS.md)
- [DL-004 Reference Dictionary Fill Policy](DL-004_REFERENCE_DICTIONARY_FILL_POLICY.md)
- [DL-005 Ingestion Health Classification](DL-005_INGESTION_HEALTH_CLASSIFICATION.md)
- [DL-006 Data Freshness and Coverage Expectations](DL-006_DATA_FRESHNESS_AND_COVERAGE_EXPECTATIONS.md)
- [DL-007 Reporting View Readiness](DL-007_REPORTING_VIEW_READINESS.md)
