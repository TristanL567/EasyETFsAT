# SE-009 Validation

## Ticket

SE-009: Add PostgreSQL View Semantics Tests

## Result

Accepted.

## Scope Check

Changed files reviewed:

- `tests\test_tax_views_postgres.py`

Validator artifact added:

- `Documentation\Validation\SE-009_VALIDATION.md`

No `fondant` files or Alembic migrations were changed. The worker did not use
SQLite for view semantics, stage, commit, push, or start adjacent ticket work.

An unrelated untracked documentation file was present in the worktree during
validation and was intentionally left unstaged.

## Findings

- The new test module is PostgreSQL/testcontainers-backed and does not use
  SQLite for `V1_TAXDATPRE` or `V2_TAXDATEUR` semantics.
- The seeded scenario covers key pivots: `K61PVM`, `K62STI`, and `K40BVJ`.
- The seeded scenario covers current FX behavior:
  - EUR uses rate `1`.
  - Non-EUR exact-date FX converts values through `REFEXC`.
  - Missing exact-date FX returns null converted values.
  - Zero FX returns null converted values.
- Docker was available during validator inspection, but the active Python
  environment did not have `testcontainers.postgres` importable. The targeted
  PostgreSQL test skipped with a clear dependency message.

## Verification

```text
docker info
```

Result: passed. Docker Desktop was available.

```text
py -3.10 -m pytest tests/test_tax_views_postgres.py -rs --basetemp .pytest_tmp
```

Result:

```text
SKIPPED [1] tests\test_tax_views_postgres.py:170: testcontainers.postgres is required for PostgreSQL-backed view tests
1 skipped in 2.16s
```

```text
py -3.10 -m ruff check tests
```

Result:

```text
All checks passed!
```

```text
py -3.10 -m pytest tests --basetemp .pytest_tmp
```

Result:

```text
37 passed, 2 skipped in 12.88s
```

```text
git diff --check -- tests\test_tax_views_postgres.py
```

Result: no whitespace errors.

The temporary `.pytest_tmp` directory was removed after validation.

Pytest emitted the pre-existing requests/urllib3 compatibility warning. It did
not fail the accepted runs.

## Decision

SE-009 is accepted and ready to commit on `development`.
