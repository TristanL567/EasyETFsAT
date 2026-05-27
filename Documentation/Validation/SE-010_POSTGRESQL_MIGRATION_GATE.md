# SE-010 PostgreSQL Migration Verification Gate

## Purpose

SQLite migration checks remain useful for quick local feedback, but they cannot
prove PostgreSQL-specific reporting contracts: quoted identifiers, view
creation, numeric precision, exact-date FX behavior, and PostgreSQL constraint
semantics. The standard gate for migration/view validation is now the
Docker-backed PostgreSQL pytest marker.

## One-Command Gate

Run this command before validating migration or reporting-view changes:

```powershell
py -3.10 -m pytest -m postgres
```

The marker currently covers:

- `tests/test_migrations.py::test_migrations_postgres_fresh_install`
- `tests/test_tax_views_postgres.py::test_tax_views_apply_current_pivot_and_fx_semantics_on_postgres`

These tests start disposable PostgreSQL 16 containers through
`testcontainers.postgres`, apply Alembic migrations to a fresh database, and
verify the reporting views with static seeded data. They do not call live OeKB
or ECB services.

## Local Validation Sequence

Use this sequence when validating the full local database posture:

```powershell
docker compose ps
py -3.10 -m alembic current
py -3.10 -m pytest -m postgres
py -3.10 -m pytest tests
```

`docker compose ps` and `py -3.10 -m alembic current` check the developer
database configured in `alembic.ini`. The pytest marker is the repeatable gate:
it uses disposable test databases and avoids writing to the Compose
`postgres_data` volume.

If Docker is not running or the testcontainers dependency is unavailable, the
PostgreSQL tests skip with an explicit reason. That skip is acceptable for quick
unit-only feedback, but not for a migration/view validation signoff.

## CI Feasibility

No CI workflow configuration is present in this repository as of SE-010, and CI
edits were not approved for this ticket. A future CI ticket can add a workflow
that installs the `dev` extras, enables Docker service access, and runs:

```powershell
py -3.10 -m pytest -m postgres
```

The existing tests are CI-feasible because they use disposable PostgreSQL
containers and static in-test data, with no live OeKB or ECB access.
