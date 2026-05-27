# SE-007 Validation

## Ticket

SE-007: Harden The Public API Contract

## Result

Accepted.

## Scope Check

Changed files reviewed:

- `tests\test_api_etf.py`
- `Documentation\Validation\SE-007_PUBLIC_API_CONTRACT.md`

Validator artifact added:

- `Documentation\Validation\SE-007_VALIDATION.md`

No `fondant` files or Alembic migrations were changed. The worker did not stage,
commit, push, or start adjacent ticket work.

An unrelated untracked documentation file was present in the worktree during
validation and was intentionally left unstaged.

## Findings

- `/health` contract shape is now tested and documented.
- `GET /etf/{isin}/tax` tests cover no-data ISINs, invalid-looking ISINs,
  multiple reports, tied report dates, null report metadata/date handling,
  null-year fallback, OpenAPI route shape, and current numeric serialization.
- The contract note correctly states current behavior:
  - ISINs are uppercased but not syntax-validated.
  - Unknown and invalid-looking ISINs both return the current 404 no-data
    response.
  - Reports are ordered by `meldg_datum DESC`; tied dates and null placement do
    not have an explicit secondary ordering contract.
  - Tax amounts are JSON numbers converted from `Decimal`/`Numeric` to Python
    `float`, not decimal-safe strings.
- No blocking implementation bug was found within this ticket.

## Verification

```text
py -3.10 -m pytest tests/test_api_etf.py
```

Result:

```text
9 passed in 2.00s
```

```text
py -3.10 -m ruff check tests/test_api_etf.py
```

Result:

```text
All checks passed!
```

Additional regression check:

```text
py -3.10 -m pytest tests --basetemp .pytest_tmp
```

Result:

```text
37 passed, 1 skipped in 6.57s
```

The temporary `.pytest_tmp` directory was removed after validation.

Pytest emitted the pre-existing requests/urllib3 compatibility warning. It did
not fail the runs.

## Decision

SE-007 is accepted and ready to commit on `development`.
