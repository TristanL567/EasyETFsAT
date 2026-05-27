# SE-002 Validation

## Ticket

SE-002: Add Parser Diagnostics Without Schema Churn

## Result

Accepted.

## Scope Check

Changed files reviewed:

- `fondant\oekb\parser.py`
- `fondant\ingestion\pipeline.py`
- `tests\test_oekb_parser.py`
- `tests\test_ingestion.py`

Validator artifact added:

- `Documentation\Validation\SE-002_VALIDATION.md`

The worker stayed inside the allowed parser, ingestion, test, and validation
documentation scope. No Alembic migration, database schema, public API response
shape, live credentials, network ingestion, commits, pushes, or adjacent ticket
work were changed by the worker.

An unrelated untracked documentation file was present in the worktree during
validation and was intentionally left unstaged.

## Findings

- `build_sourceage_values(...)` remains compatible and delegates to the new
  diagnostics-returning parser path.
- `build_sourceage_result(...)` returns source-age values plus structured
  diagnostics for unknown tax fields, unknown categories, and invalid numeric
  values.
- `IngestionResult` now exposes aggregated parser diagnostics without changing
  persisted schema or curated `TAXDAT` output.
- The structured ingestion success log includes parser diagnostic codes.
- The implementation keeps diagnostics local to parser and ingestion code.

## Verification

```text
py -3.10 -m pytest tests/test_oekb_parser.py tests/test_ingestion.py
```

Result:

```text
12 passed in 2.75s
```

```text
py -3.10 -m ruff check fondant/oekb fondant/ingestion tests
```

Result:

```text
All checks passed!
```

Additional regression check:

```text
py -3.10 -m pytest --basetemp .pytest-basetemp
```

Result:

```text
24 passed, 1 skipped in 19.11s
```

A plain full-suite run without `--basetemp` first failed because pytest could
not create another numbered directory under the user profile temp path. A retry
using repo-local `.pytest-basetemp` passed. The temporary basetemp directory was
removed after validation.

Pytest emitted the pre-existing requests/urllib3 compatibility warning. It did
not fail the run.

## Decision

SE-002 is accepted and ready to commit on `development`.
