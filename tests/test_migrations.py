from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic.config import Config
from sqlalchemy import create_engine, text

from alembic import command
from fondant.config import get_settings


@contextmanager
def _database_url_env(database_url: str):
    previous_database_url = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = database_url
    get_settings.cache_clear()
    try:
        yield
    finally:
        if previous_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous_database_url
        get_settings.cache_clear()


def _run_alembic_upgrade(database_url: str, revision: str = "head") -> None:
    with _database_url_env(database_url):
        cfg = Config("alembic.ini")
        command.upgrade(cfg, revision)


def _assert_rebuilt_architecture(database_url: str) -> None:
    engine = create_engine(database_url, future=True)
    inspector = sa.inspect(engine)

    expected_tables = {
        "SECMDA",
        "SECDIV",
        "SOURCERPT",
        "SOURCEAGE",
        "SOURCERAW",
        "TAXRPT",
        "TAXDAT",
        "TAXADJ",
        "TAXLIN",
        "TAXCAT",
        "TAXCOR",
        "REFCCY",
        "REFCTR",
        "REFEXC",
        "IMPLOG",
        "IMPERR",
    }
    existing_tables = set(inspector.get_table_names())
    assert expected_tables.issubset(existing_tables)

    assert "TAXAGE" not in existing_tables
    assert "TAXRAW" not in existing_tables
    assert "TAXLST" not in existing_tables

    sourcerpt_columns = {column["name"] for column in inspector.get_columns("SOURCERPT")}
    assert {
        "SRCISN",
        "SRCOKBIDN",
        "SRCYEA",
        "SRCBUSYEABEG",
        "SRCBUSYEAEND",
        "SRCENTDTS",
    }.issubset(sourcerpt_columns)

    taxdat_columns = {column["name"] for column in inspector.get_columns("TAXDAT")}
    assert {"TAXRPTIDN", "TAXOKBIDN", "TAXLINIDN", "TAXCATIDN", "TAXAMT"}.issubset(taxdat_columns)
    sourceage_columns = {column["name"] for column in inspector.get_columns("SOURCEAGE")}
    assert {"SRCK40PVM", "SRCK40STF", "SRCK62PVM", "SRCK62STF"}.issubset(sourceage_columns)
    refexc_columns = {column["name"] for column in inspector.get_columns("REFEXC")}
    assert {"REFDAT", "REFCCY", "REFRAT"}.issubset(refexc_columns)
    view_names = set(inspector.get_view_names())
    assert "V1_TAXDATPRE" in view_names
    assert "V2_TAXDATEUR" in view_names

    view_cols = {column["name"] for column in inspector.get_columns("V1_TAXDATPRE")}
    assert {
        "FNDCCY",
        "K61PVM",
        "K61PVO",
        "K61BVM",
        "K61BVO",
        "K61BVJ",
        "K61STI",
        "K62PVM",
        "K62PVO",
        "K62BVM",
        "K62BVO",
        "K62BVJ",
        "K62STI",
        "K40PVM",
        "K40PVO",
        "K40BVM",
        "K40BVO",
        "K40BVJ",
        "K40STI",
    }.issubset(view_cols)
    assert not {"AGEPVM", "AGEPVO", "CORAMTPVM", "CORAMTPVO"}.intersection(view_cols)

    view2_cols = {column["name"] for column in inspector.get_columns("V2_TAXDATEUR")}
    assert {"TAXMDT", "FXRAT", "K61PVM", "K62PVM", "K40PVM"}.issubset(view2_cols)

    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == "20260419_0011"
    engine.dispose()


def _normalize_postgres_url(raw_url: str) -> str:
    if raw_url.startswith("postgresql+psycopg://"):
        return raw_url
    if raw_url.startswith("postgresql+psycopg2://"):
        return raw_url.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return raw_url


def _require_docker_or_skip() -> None:
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Docker is not available for PostgreSQL container tests: {exc}")


@pytest.fixture
def postgres_url() -> str:
    _require_docker_or_skip()
    tc_postgres = pytest.importorskip("testcontainers.postgres")
    postgres_container = tc_postgres.PostgresContainer

    try:
        with postgres_container("postgres:16") as container:
            yield _normalize_postgres_url(container.get_connection_url())
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Could not start PostgreSQL container: {exc}")


def test_migrations_sqlite_fresh_install(tmp_path: Path) -> None:
    sqlite_file = tmp_path / "fresh.sqlite3"
    database_url = f"sqlite:///{sqlite_file.as_posix()}"

    _run_alembic_upgrade(database_url)
    _assert_rebuilt_architecture(database_url)


def test_migrations_postgres_fresh_install(postgres_url: str) -> None:
    _run_alembic_upgrade(postgres_url)
    _assert_rebuilt_architecture(postgres_url)
