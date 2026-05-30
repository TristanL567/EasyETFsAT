from __future__ import annotations

import socket
from datetime import date

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from scripts import reporting_view_readiness


class PostgreSQLConnectionStub:
    dialect = postgresql.dialect()


def test_reporting_view_readiness_reports_counts_and_gap_reasons() -> None:
    engine = _create_engine()
    with engine.begin() as connection:
        _create_schema(connection)
        _seed_rows(connection)

    report, exit_code = reporting_view_readiness.build_readiness_report(engine)

    assert exit_code == 0
    assert "TAXRPT       | 5" in report
    assert "V1_TAXDATPRE | 5" in report
    assert "V2_TAXDATEUR | 4" in report
    assert "AT000000005" in report
    assert "AT000000006" in report
    assert "USD    | 2026-04-02 | 1" in report
    assert "USD    | 2026-04-03 | 0" in report
    assert "K61PVM | 1" in report
    assert "K61PVM | 0                      | 1                | 1" in report
    assert "K62PVM | 4                      | 0                | 0" in report
    assert "Read-only: SELECT/introspection only" in report

    engine.dispose()


def test_reporting_view_readiness_exits_zero_when_gaps_are_found() -> None:
    engine = _create_engine()
    with engine.begin() as connection:
        _create_schema(connection)
        connection.execute(
            text(
                """
                INSERT INTO TAXRPT (TAXISN, TAXOKBIDN, TAXYEA, TAXMDT, TAXCCY)
                VALUES ('AT000000005', 1005, 2026, :report_date, 'USD')
                """
            ),
            {"report_date": date(2026, 4, 4)},
        )

    report, exit_code = reporting_view_readiness.build_readiness_report(engine)

    assert exit_code == 0
    assert "AT000000005" in report
    assert "USD    | 2026-04-04 | 1" in report

    engine.dispose()


def test_reporting_view_readiness_missing_schema_raises_execution_failure() -> None:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)

    with pytest.raises(RuntimeError, match="Missing required reporting readiness schema"):
        reporting_view_readiness.build_readiness_report(engine)

    engine.dispose()


def test_reporting_view_readiness_does_not_use_network(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _create_engine()
    with engine.begin() as connection:
        _create_schema(connection)

    def fail_socket(*_args: object, **_kwargs: object) -> socket.socket:
        raise AssertionError("network access is not allowed")

    monkeypatch.setattr(socket, "socket", fail_socket)

    report, exit_code = reporting_view_readiness.build_readiness_report(engine)

    assert exit_code == 0
    assert "Read-only: SELECT/introspection only" in report

    engine.dispose()


def test_sync_database_url_uses_synchronous_drivers() -> None:
    assert (
        reporting_view_readiness.sync_database_url("postgresql+asyncpg://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )
    assert (
        reporting_view_readiness.sync_database_url("sqlite+aiosqlite:///tmp/test.db")
        == "sqlite+pysqlite:///tmp/test.db"
    )


def test_postgresql_query_text_quotes_uppercase_identifiers() -> None:
    connection = PostgreSQLConnectionStub()

    sql = str(reporting_view_readiness._null_counts_statement(connection, "V1_TAXDATPRE"))

    assert '"V1_TAXDATPRE"' in sql
    assert '"K61PVM"' in sql


def _create_engine() -> sa.Engine:
    return sa.create_engine("sqlite+pysqlite:///:memory:", future=True)


def _create_schema(connection: sa.Connection) -> None:
    selected_columns_sql = ", ".join(
        f"{column} NUMERIC" for column in reporting_view_readiness.SELECTED_VALUE_COLUMNS
    )
    connection.execute(
        text(
            """
            CREATE TABLE TAXRPT (
                TAXISN TEXT NOT NULL,
                TAXOKBIDN INTEGER NOT NULL,
                TAXYEA INTEGER NOT NULL,
                TAXMDT DATE,
                TAXCCY TEXT
            )
            """
        )
    )
    connection.execute(
        text(
            """
            CREATE TABLE REFEXC (
                REFDAT DATE NOT NULL,
                REFCCY TEXT NOT NULL,
                REFRAT NUMERIC
            )
            """
        )
    )
    connection.execute(
        text(
            f"""
            CREATE TABLE V1_SOURCE (
                TAXISN TEXT NOT NULL,
                TAXOKBIDN INTEGER NOT NULL,
                TAXYEA INTEGER NOT NULL,
                FNDCCY TEXT NOT NULL,
                {selected_columns_sql}
            )
            """
        )
    )
    connection.execute(text("CREATE VIEW V1_TAXDATPRE AS SELECT * FROM V1_SOURCE"))
    converted_columns_sql = ", ".join(
        f"""
                CASE
                    WHEN fx.FXRAT IS NULL OR fx.FXRAT = 0 THEN NULL
                    ELSE v.{column} / fx.FXRAT
                END AS {column}"""
        for column in reporting_view_readiness.SELECTED_VALUE_COLUMNS
    )
    connection.execute(
        text(
            f"""
            CREATE VIEW V2_TAXDATEUR AS
            WITH fx AS (
                SELECT
                    v.TAXISN,
                    v.TAXOKBIDN,
                    v.TAXYEA,
                    v.FNDCCY,
                    CASE
                        WHEN v.FNDCCY = 'EUR' THEN 1
                        ELSE r.REFRAT
                    END AS FXRAT
                FROM V1_SOURCE AS v
                LEFT JOIN TAXRPT AS t
                  ON t.TAXISN = v.TAXISN
                 AND t.TAXOKBIDN = v.TAXOKBIDN
                 AND t.TAXYEA = v.TAXYEA
                 AND t.TAXCCY = v.FNDCCY
                LEFT JOIN REFEXC AS r
                  ON r.REFCCY = v.FNDCCY
                 AND r.REFDAT = t.TAXMDT
                WHERE v.TAXOKBIDN IN (1001, 1002, 1003, 1004)
            )
            SELECT
                v.TAXISN,
                v.TAXOKBIDN,
                v.TAXYEA,
                v.FNDCCY,
                fx.FXRAT,
                {converted_columns_sql}
            FROM V1_SOURCE AS v
            JOIN fx
              ON fx.TAXISN = v.TAXISN
             AND fx.TAXOKBIDN = v.TAXOKBIDN
             AND fx.TAXYEA = v.TAXYEA
             AND fx.FNDCCY = v.FNDCCY
            """
        )
    )


def _seed_rows(connection: sa.Connection) -> None:
    connection.execute(
        text(
            """
            INSERT INTO TAXRPT (TAXISN, TAXOKBIDN, TAXYEA, TAXMDT, TAXCCY)
            VALUES
                ('AT000000001', 1001, 2026, :date_1, 'EUR'),
                ('AT000000002', 1002, 2026, :date_1, 'USD'),
                ('AT000000003', 1003, 2026, :date_2, 'USD'),
                ('AT000000004', 1004, 2026, :date_3, 'USD'),
                ('AT000000005', 1005, 2026, :date_4, 'USD')
            """
        ),
        {
            "date_1": date(2026, 4, 1),
            "date_2": date(2026, 4, 2),
            "date_3": date(2026, 4, 3),
            "date_4": date(2026, 4, 4),
        },
    )
    connection.execute(
        text(
            """
            INSERT INTO REFEXC (REFDAT, REFCCY, REFRAT)
            VALUES
                (:date_1, 'USD', 2),
                (:date_3, 'USD', 0)
            """
        ),
        {"date_1": date(2026, 4, 1), "date_3": date(2026, 4, 3)},
    )
    connection.execute(
        text(
            """
            INSERT INTO V1_SOURCE (
                TAXISN, TAXOKBIDN, TAXYEA, FNDCCY, K61PVM, K62STI, K40BVJ
            )
            VALUES
                ('AT000000001', 1001, 2026, 'EUR', 10, 20, 30),
                ('AT000000002', 1002, 2026, 'USD', 24, 48, 96),
                ('AT000000003', 1003, 2026, 'USD', 11, NULL, NULL),
                ('AT000000004', 1004, 2026, 'USD', 15, NULL, NULL),
                ('AT000000006', 1006, 2026, 'USD', NULL, NULL, NULL)
            """
        )
    )
