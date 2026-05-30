# DL-005 Ingestion Health Classification

## Scope

DL-005 adds a read-only ingestion health classifier at `scripts/ingestion_health.py`.
The script uses configured database settings, converts async SQLAlchemy driver URLs to synchronous driver URLs for inspection, and queries only the existing ingestion log tables:

- `IMPLOG`
- `IMPERR`

It does not import or execute OeKB, ECB, ingestion, scheduler, alerting, deployment, or mutation code.

## Command

```powershell
py -3.10 scripts/ingestion_health.py
```

## Output

The script prints a concise summary containing:

- classification
- detail explaining the classification
- recent run count inspected
- success and failure counts
- records seen and records written totals
- grouped recent error evidence from `IMPERR`, when present

The script exits `0` when it can inspect the log schema, including non-error classifications and failure classifications based on available rows. It exits non-zero only when the required log schema is unavailable, the database connection is unavailable, or an unexpected SQLAlchemy execution failure occurs.

## Classification Rules

The classifier inspects the latest 20 `IMPLOG` rows ordered by `IMPSTADTS DESC`, `IMPCRTDTS DESC`, and `IMPISN ASC`.

- `schema_unavailable`: `IMPLOG` or `IMPERR` is missing.
- `no_recent_runs`: both required tables exist, but `IMPLOG` has no rows.
- `systemic_failure`: all recent runs failed, at least 80% of recent runs failed, or grouped recent `IMPERR` evidence shows repeated connection/client failures across at least two affected runs.
- `isolated_failure`: a small number of failed runs are mixed with recent successful runs and systemic failure rules do not apply.
- `successful_update`: recent successful runs wrote one or more records.
- `healthy_noop_refresh`: recent successful runs completed with records seen and zero records written.

Connection/client evidence is grouped by `IMPSTG` and `IMPECD`, with sample messages included. Repeated groups containing markers such as `connection`, `timeout`, `client`, `network`, `http`, `ssl`, `dns`, `refused`, or `unavailable` are treated as systemic evidence when they affect at least two recent runs.

## Read-Only Safety

The implementation uses SQLAlchemy `SELECT` and schema introspection only. It does not run `INSERT`, `UPDATE`, `DELETE`, DDL, external HTTP calls, ingestion jobs, scheduler jobs, or alerting logic. The script is safe to run repeatedly because it has no write path.

## Test Coverage

`tests/test_ingestion_health.py` covers:

- all-success update
- healthy no-op refresh
- mixed isolated failure
- all/systemic failure
- repeated connection/client error evidence
- no recent runs
- missing schema/table behavior
- no network access
- configured async-to-sync database URL conversion

## Validation Commands

```powershell
py -3.10 -m pytest tests
py -3.10 -m ruff check scripts tests
git diff --check
git status --short --branch --untracked-files=all
```

If the full test run hits the known Windows temp-dir issue:

```powershell
py -3.10 -m pytest tests --basetemp .pytest-tmp-dl005
```
