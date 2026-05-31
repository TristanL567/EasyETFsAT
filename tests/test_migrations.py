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
from fondant.tax_registry import TAX_CATEGORIES, TAX_LINES


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
        "INGJOB",
        "BQGROUP",
        "BQSAVED",
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
    taxlin_columns = {column["name"] for column in inspector.get_columns("TAXLIN")}
    assert {"TAXDSC", "TAXUSE", "TAXSRC"}.issubset(taxlin_columns)
    ingjob_columns = {column["name"]: column for column in inspector.get_columns("INGJOB")}
    assert {
        "JOBIDN",
        "JOBCRTDTS",
        "JOBUPDDTS",
        "JOBISN",
        "JOBREQUSR",
        "JOBSTS",
        "JOBMSG",
        "JOBERR",
        "JOBSTADTS",
        "JOBFINDTS",
    }.issubset(ingjob_columns)
    assert ingjob_columns["JOBISN"]["nullable"] is False
    assert ingjob_columns["JOBSTS"]["nullable"] is False
    ingjob_indexes = {index["name"]: index for index in inspector.get_indexes("INGJOB")}
    assert ingjob_indexes["ix_ingjob_isin_status"]["column_names"] == ["JOBISN", "JOBSTS"]
    bqgroup_columns = {column["name"]: column for column in inspector.get_columns("BQGROUP")}
    assert {
        "BQGIDN",
        "BQGCRTDTS",
        "BQGUPDDTS",
        "BQGUSR",
        "BQGNAM",
        "BQGDSC",
    }.issubset(bqgroup_columns)
    assert bqgroup_columns["BQGUSR"]["nullable"] is False
    assert bqgroup_columns["BQGNAM"]["nullable"] is False
    assert bqgroup_columns["BQGDSC"]["nullable"] is True
    bqgroup_constraints = {constraint["name"]: constraint for constraint in inspector.get_unique_constraints("BQGROUP")}
    assert bqgroup_constraints["uq_bqgroup_user_name"]["column_names"] == ["BQGUSR", "BQGNAM"]
    bqgroup_indexes = {index["name"]: index for index in inspector.get_indexes("BQGROUP")}
    assert bqgroup_indexes["ix_bqgroup_owner"]["column_names"] == ["BQGUSR"]
    bqsaved_columns = {column["name"]: column for column in inspector.get_columns("BQSAVED")}
    assert {
        "BQSIDN",
        "BQSCRTDTS",
        "BQSUPDDTS",
        "BQSUSR",
        "BQSGRPIDN",
        "BQSNAM",
        "BQSLENTYP",
        "BQSSUBCAT",
        "BQSTXYR",
        "BQSAMT",
        "BQSNOTE",
        "BQSISNS",
    }.issubset(bqsaved_columns)
    assert bqsaved_columns["BQSUSR"]["nullable"] is False
    assert bqsaved_columns["BQSGRPIDN"]["nullable"] is True
    assert bqsaved_columns["BQSNAM"]["nullable"] is False
    assert bqsaved_columns["BQSLENTYP"]["nullable"] is False
    assert bqsaved_columns["BQSSUBCAT"]["nullable"] is False
    assert bqsaved_columns["BQSTXYR"]["nullable"] is False
    assert bqsaved_columns["BQSAMT"]["nullable"] is False
    assert bqsaved_columns["BQSNOTE"]["nullable"] is True
    assert bqsaved_columns["BQSISNS"]["nullable"] is True
    bqsaved_constraints = {constraint["name"]: constraint for constraint in inspector.get_unique_constraints("BQSAVED")}
    assert bqsaved_constraints["uq_bqsaved_user_name"]["column_names"] == ["BQSUSR", "BQSNAM"]
    bqsaved_indexes = {index["name"]: index for index in inspector.get_indexes("BQSAVED")}
    assert bqsaved_indexes["ix_bqsaved_owner"]["column_names"] == ["BQSUSR"]
    assert bqsaved_indexes["ix_bqsaved_group"]["column_names"] == ["BQSGRPIDN"]
    bqsaved_foreign_keys = {foreign_key["name"]: foreign_key for foreign_key in inspector.get_foreign_keys("BQSAVED")}
    assert bqsaved_foreign_keys["fk_bqsaved_group_owner"]["constrained_columns"] == ["BQSGRPIDN", "BQSUSR"]
    assert bqsaved_foreign_keys["fk_bqsaved_group_owner"]["referred_table"] == "BQGROUP"
    assert bqsaved_foreign_keys["fk_bqsaved_group_owner"]["referred_columns"] == ["BQGIDN", "BQGUSR"]
    sourceage_columns = {column["name"] for column in inspector.get_columns("SOURCEAGE")}
    assert {"SRCK40PVM", "SRCK40STF", "SRCK62PVM", "SRCK62STF"}.issubset(sourceage_columns)
    refexc_columns = {column["name"] for column in inspector.get_columns("REFEXC")}
    assert {"REFDAT", "REFCCY", "REFRAT"}.issubset(refexc_columns)
    view_names = set(inspector.get_view_names())
    assert "V1_TAXDATPRE" in view_names
    assert "V2_TAXDATEUR" in view_names
    assert "V2_TAXDATHOMCCY" in view_names

    view_cols = {column["name"] for column in inspector.get_columns("V1_TAXDATPRE")}
    registry_view_columns = {
        f"{line_code}{category.view_alias}"
        for line_code in ("K61", "K62", "K40")
        for category in TAX_CATEGORIES
    }
    assert {
        "FNDCCY",
    }.union(registry_view_columns).issubset(view_cols)
    assert not {"AGEPVM", "AGEPVO", "CORAMTPVM", "CORAMTPVO"}.intersection(view_cols)

    view2_cols = {column["name"] for column in inspector.get_columns("V2_TAXDATEUR")}
    assert {"TAXMDT", "FXRAT", "K61PVM", "K62PVM", "K40PVM"}.issubset(view2_cols)

    homccy_view_cols = {column["name"] for column in inspector.get_columns("V2_TAXDATHOMCCY")}
    expanded_tax_columns = {
        f"{line.line_code}{category.view_alias}_{currency_suffix}"
        for line in TAX_LINES
        for category in TAX_CATEGORIES
        for currency_suffix in ("HOMCCY", "EUR")
    }
    assert {
        "TAXISN",
        "TAXOKBIDN",
        "TAXYEA",
        "FNDCCY",
        "TAXMDT",
        "FXRAT",
        "K40PVM_HOMCCY",
        "K61BVM_HOMCCY",
        "K62STI_HOMCCY",
        "K40PVM_EUR",
        "K61BVM_EUR",
        "K62STI_EUR",
    }.issubset(homccy_view_cols)
    assert expanded_tax_columns.issubset(homccy_view_cols)
    assert {
        "K11PVM_HOMCCY",
        "K11BVO_EUR",
        "K40PVM_HOMCCY",
        "K40PVM_EUR",
        "K61BVM_HOMCCY",
        "K61BVM_EUR",
        "K62STI_HOMCCY",
        "K62STI_EUR",
    }.issubset(homccy_view_cols)

    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
    assert revision == "20260531_0017"
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


@pytest.mark.postgres
def test_migrations_postgres_fresh_install(postgres_url: str) -> None:
    _run_alembic_upgrade(postgres_url)
    _assert_rebuilt_architecture(postgres_url)
