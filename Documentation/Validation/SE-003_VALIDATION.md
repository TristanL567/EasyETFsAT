# SE-003 Validation

## Ticket

SE-003: Create A Tax Code And Category Registry

## Result

Accepted.

## Scope Check

Changed files reviewed:

- `fondant\tax_registry.py`
- `fondant\oekb\parser.py`
- `fondant\ingestion\pipeline.py`
- `tests\test_tax_registry.py`
- `tests\test_migrations.py`
- `Documentation\Validation\SE-003_TAX_CODE_REGISTRY.md`

Validator artifact added:

- `Documentation\Validation\SE-003_VALIDATION.md`

No Alembic migration files were changed. No API module files were changed. No
API field names were renamed. An unrelated untracked documentation file was
present in the worktree during validation and was intentionally left unstaged.

## Findings

- `fondant\tax_registry.py` is a small data-oriented registry for tax lines,
  categories, parser aliases, source aliases, and reporting-view aliases.
- Parser tax maps and ingestion seed dictionaries now derive from the registry
  instead of maintaining local duplicate dictionaries.
- The registry includes all live `TAXLIN` codes required by the ticket:
  `K40`, `K11`, `K12`, `K81`, `K82`, `K10`, `K55`, `K61`, `K62`, `K36`,
  `K21`.
- The registry includes all live `TAXCAT` codes required by the ticket:
  `PVM`, `PVO`, `BVM`, `BVO`, `BVJ`, `STF`.
- The `STF` source/category code versus `STI` reporting-view alias decision is
  explicit in both the registry and validation documentation.
- Consistency tests compare the registry against parser maps, ingestion seed
  dictionaries, parser output models, `SOURCEAGE` source columns, and migration
  view expectations.

## Verification

```text
py -3.10 -m ruff check fondant tests
```

Result:

```text
All checks passed!
```

```text
py -3.10 -m pytest tests
```

Result:

```text
25 passed, 1 skipped, 3 setup errors
```

The setup errors were caused by pytest being unable to create another numbered
directory under the user profile temp path:

```text
C:\Users\Tristan Leiter\AppData\Local\Temp\pytest-of-Tristan Leiter
```

The same test target passed with a repo-local basetemp:

```text
py -3.10 -m pytest tests --basetemp .pytest_tmp
```

Result:

```text
28 passed, 1 skipped in 3.66s
```

The temporary `.pytest_tmp` directory was removed after validation.

Pytest emitted the pre-existing requests/urllib3 compatibility warning. It did
not fail the accepted run.

## Decision

SE-003 is accepted and ready to commit on `development`.
