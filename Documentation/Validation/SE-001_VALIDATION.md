# SE-001 Validation

## Ticket

SE-001: Characterize Parser Drops And Bad Values

## Result

Accepted.

## Scope Check

Changed files reviewed:

- `tests/test_oekb_parser.py`

Validator artifact added:

- `Documentation\Validation\SE-001_VALIDATION.md`

The worker stayed inside the allowed test scope. No application code,
migrations, environment files, live network calls, staging, commits, pushes, or
adjacent tickets were touched by the worker.

An unrelated untracked documentation file was present in the worktree during
validation and was intentionally left unstaged.

## Findings

- Unknown OeKB tax line values are characterized as silently dropped.
- Unknown OeKB category values are characterized as silently dropped.
- Malformed numeric values are characterized as parsing to `None`.
- Missing expected category values are characterized as remaining `None`.
- These tests document current behavior only; they do not approve it as product
  behavior.

## Verification

```text
py -3.10 -m pytest tests/test_oekb_parser.py
```

Result:

```text
6 passed in 0.18s
```

Pytest emitted the pre-existing requests/urllib3 compatibility warning. It did
not fail the run.

```text
py -3.10 -m ruff check tests/test_oekb_parser.py
```

Result:

```text
All checks passed!
```

## Decision

SE-001 is accepted and ready to commit on `development`.
