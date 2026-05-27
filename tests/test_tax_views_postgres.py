from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from decimal import Decimal

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


def _run_alembic_upgrade(database_url: str) -> None:
    with _database_url_env(database_url):
        cfg = Config("alembic.ini")
        command.upgrade(cfg, "head")


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
        pytest.skip(f"Docker is not available for PostgreSQL-backed view tests: {exc}")


@pytest.fixture
def postgres_url() -> str:
    _require_docker_or_skip()
    tc_postgres = pytest.importorskip(
        "testcontainers.postgres",
        reason="testcontainers.postgres is required for PostgreSQL-backed view tests",
    )
    postgres_container = tc_postgres.PostgresContainer

    try:
        with postgres_container("postgres:16") as container:
            yield _normalize_postgres_url(container.get_connection_url())
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Could not start PostgreSQL container for view tests: {exc}")


def _seed_reporting_view_data(connection: sa.Connection) -> None:
    connection.execute(
        text(
            """
            INSERT INTO "SECMDA" ("SECISN", "SECNAM", "SECCCY")
            VALUES
                ('AT000000001', 'EUR contract fund', 'EUR'),
                ('AT000000002', 'USD exact-rate fund', 'USD'),
                ('AT000000003', 'USD missing-rate fund', 'USD'),
                ('AT000000004', 'USD zero-rate fund', 'USD')
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO "TAXLIN" ("TAXCOD", "TAXKEY", "TAXNDE", "TAXORD")
            VALUES
                ('K61', 'korrekturbetrag_age_ak', 'K61', 61),
                ('K62', 'korrekturbetrag_aussch_ak', 'K62', 62),
                ('K40', 'steuerpflichtige_einkuenfte', 'K40', 40)
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO "TAXCAT" ("TAXCOD", "TAXKEY", "TAXNDE", "TAXORD")
            VALUES
                ('PVM', 'pv_mit', 'Privatvermoegen mit Option', 10),
                ('STF', 'stiftung', 'Stiftung', 60),
                ('BVJ', 'bv_jur', 'Betriebsvermoegen juristisch', 50)
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO "TAXRPT"
                ("TAXISN", "TAXOKBIDN", "TAXYEA", "TAXMDT", "TAXCCY")
            VALUES
                ('AT000000001', 1001, 2026, DATE '2026-04-01', 'EUR'),
                ('AT000000002', 1002, 2026, DATE '2026-04-01', 'USD'),
                ('AT000000003', 1003, 2026, DATE '2026-04-02', 'USD'),
                ('AT000000004', 1004, 2026, DATE '2026-04-03', 'USD')
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO "REFEXC" ("REFDAT", "REFCCY", "REFRAT")
            VALUES
                (DATE '2026-03-31', 'USD', 9),
                (DATE '2026-04-01', 'USD', 2),
                (DATE '2026-04-03', 'USD', 0)
            """
        )
    )
    connection.execute(
        text(
            """
            INSERT INTO "TAXDAT"
                ("TAXRPTIDN", "TAXOKBIDN", "TAXLINIDN", "TAXCATIDN", "TAXAMT", "TAXCCY")
            SELECT r."TAXIDN", r."TAXOKBIDN", l."TAXIDN", c."TAXIDN", v.amount, r."TAXCCY"
            FROM (
                VALUES
                    (1001, 'K61', 'pv_mit', 10),
                    (1001, 'K62', 'stiftung', 20),
                    (1001, 'K40', 'bv_jur', 30),
                    (1002, 'K61', 'pv_mit', 24),
                    (1002, 'K62', 'stiftung', 48),
                    (1002, 'K40', 'bv_jur', 96),
                    (1003, 'K61', 'pv_mit', 11),
                    (1004, 'K61', 'pv_mit', 15)
            ) AS v(okbidn, line_code, category_key, amount)
            JOIN "TAXRPT" AS r ON r."TAXOKBIDN" = v.okbidn
            JOIN "TAXLIN" AS l ON l."TAXCOD" = v.line_code
            JOIN "TAXCAT" AS c ON c."TAXKEY" = v.category_key
            """
        )
    )


def _rows_by_okbidn(connection: sa.Connection, view_name: str) -> dict[int, sa.RowMapping]:
    return {
        row["TAXOKBIDN"]: row
        for row in connection.execute(
            text(f'SELECT * FROM "{view_name}" ORDER BY "TAXOKBIDN"')
        ).mappings()
    }


def test_tax_views_apply_current_pivot_and_fx_semantics_on_postgres(postgres_url: str) -> None:
    _run_alembic_upgrade(postgres_url)
    engine = create_engine(postgres_url, future=True)
    try:
        with engine.begin() as connection:
            _seed_reporting_view_data(connection)

            v1_rows = _rows_by_okbidn(connection, "V1_TAXDATPRE")
            assert set(v1_rows) == {1001, 1002, 1003, 1004}

            eur_v1 = v1_rows[1001]
            assert eur_v1["FNDCCY"] == "EUR"
            assert eur_v1["K61PVM"] == Decimal("10.0000000000")
            assert eur_v1["K62STI"] == Decimal("20.0000000000")
            assert eur_v1["K40BVJ"] == Decimal("30.0000000000")
            assert eur_v1["K61STI"] is None

            usd_v1 = v1_rows[1002]
            assert usd_v1["FNDCCY"] == "USD"
            assert usd_v1["K61PVM"] == Decimal("24.0000000000")
            assert usd_v1["K62STI"] == Decimal("48.0000000000")
            assert usd_v1["K40BVJ"] == Decimal("96.0000000000")

            v2_rows = _rows_by_okbidn(connection, "V2_TAXDATEUR")
            assert set(v2_rows) == {1001, 1002, 1003, 1004}

            eur_v2 = v2_rows[1001]
            assert eur_v2["FXRAT"] == Decimal("1.0000000000")
            assert eur_v2["K61PVM"] == Decimal("10.0000000000")
            assert eur_v2["K62STI"] == Decimal("20.0000000000")
            assert eur_v2["K40BVJ"] == Decimal("30.0000000000")

            exact_usd_v2 = v2_rows[1002]
            assert exact_usd_v2["FXRAT"] == Decimal("2.0000000000")
            assert exact_usd_v2["K61PVM"] == Decimal("12.00000000000000000000")
            assert exact_usd_v2["K62STI"] == Decimal("24.00000000000000000000")
            assert exact_usd_v2["K40BVJ"] == Decimal("48.00000000000000000000")

            missing_rate_v2 = v2_rows[1003]
            assert missing_rate_v2["FXRAT"] is None
            assert missing_rate_v2["K61PVM"] is None

            zero_rate_v2 = v2_rows[1004]
            assert zero_rate_v2["FXRAT"] == Decimal("0E-10")
            assert zero_rate_v2["K61PVM"] is None
    finally:
        engine.dispose()
