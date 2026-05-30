from __future__ import annotations

import socket
from datetime import date, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

import fondant.db.models  # noqa: F401
from fondant.db.base import Base
from scripts import data_readiness


class PostgreSQLConnectionStub:
    dialect = postgresql.dialect()


def test_readiness_reports_coverage_and_stale_fx(tmp_path) -> None:
    engine = _create_engine()
    storage_path = tmp_path / "isin_storage.csv"
    storage_path.write_text("ISIN\nIE00BMTX1Y45\nLU0380865021\n", encoding="utf-8")

    now = datetime(2026, 5, 1, 12, 0, 0)
    with engine.begin() as connection:
        _insert_security(connection, "IE00BMTX1Y45", now)
        _insert_security(connection, "LU1681044993", now)
        _insert_source_report(connection, "IE00BMTX1Y45", 1001, now)
        _insert_source_report(connection, "LU1681044993", 1002, now)
        _insert_tax_report(connection, "IE00BMTX1Y45", 1001, "USD", now)
        _insert_fx(connection, "USD", date(2026, 5, 28), now)
        _insert_fx(connection, "GBP", date(2026, 5, 10), now)

    report, exit_code = data_readiness.build_readiness_report(
        engine,
        storage_path=storage_path,
        check_date=date(2026, 5, 30),
    )

    assert exit_code == 0
    assert "storage_isin_count: 2" in report
    assert "LU0380865021" in report
    assert "LU1681044993" in report
    assert "USD" in report
    assert "GBP" in report
    assert "CHF" in report
    assert "20 calendar days old; threshold is 7" in report
    assert "missing REFEXC rate" in report

    engine.dispose()


def test_readiness_exits_zero_when_storage_file_is_missing(tmp_path) -> None:
    engine = _create_engine()

    report, exit_code = data_readiness.build_readiness_report(
        engine,
        storage_path=tmp_path / "missing.csv",
        check_date=date(2026, 5, 30),
    )

    assert exit_code == 0
    assert "storage_isin_count: unavailable" in report
    assert "missing source report comparison skipped" in report

    engine.dispose()


def test_missing_schema_raises_execution_failure() -> None:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)

    with pytest.raises(RuntimeError, match="Missing required readiness tables"):
        data_readiness.build_readiness_report(engine, check_date=date(2026, 5, 30))

    engine.dispose()


def test_readiness_check_does_not_use_network(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _create_engine()

    def fail_socket(*_args: object, **_kwargs: object) -> socket.socket:
        raise AssertionError("network access is not allowed")

    monkeypatch.setattr(socket, "socket", fail_socket)

    report, exit_code = data_readiness.build_readiness_report(
        engine,
        check_date=date(2026, 5, 30),
    )

    assert exit_code == 0
    assert "Read-only: SELECT/introspection only" in report

    engine.dispose()


def test_sync_database_url_uses_synchronous_drivers() -> None:
    assert (
        data_readiness.sync_database_url("postgresql+asyncpg://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )
    assert (
        data_readiness.sync_database_url("sqlite+aiosqlite:///tmp/test.db")
        == "sqlite+pysqlite:///tmp/test.db"
    )


def test_postgresql_query_text_quotes_uppercase_identifiers() -> None:
    connection = PostgreSQLConnectionStub()

    sql = str(data_readiness._distinct_values_statement(connection, "SOURCERPT", "SRCISN"))

    assert '"SOURCERPT"' in sql
    assert '"SRCISN"' in sql


def _create_engine() -> sa.Engine:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


def _insert_security(connection: sa.Connection, isin: str, now: datetime) -> None:
    connection.execute(
        text(
            """
            INSERT INTO SECMDA (SECISN, SECNAM, SECCRTDTS, SECUPDDTS)
            VALUES (:isin, 'Sample ETF', :now, :now)
            """
        ),
        {"isin": isin, "now": now},
    )


def _insert_source_report(connection: sa.Connection, isin: str, okb_id: int, now: datetime) -> None:
    connection.execute(
        text(
            """
            INSERT INTO SOURCERPT (
                SRCISN, SRCOKBIDN, SRCVRN, SRCYEA, SRCMDT, SRCCRTDTS, SRCUPDDTS
            )
            VALUES (:isin, :okb_id, 1, 2025, :report_date, :now, :now)
            """
        ),
        {"isin": isin, "okb_id": okb_id, "report_date": date(2025, 7, 28), "now": now},
    )


def _insert_tax_report(
    connection: sa.Connection,
    isin: str,
    okb_id: int,
    currency: str,
    now: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO TAXRPT (
                TAXISN, TAXOKBIDN, TAXVRN, TAXYEA, TAXMDT, TAXCCY, TAXCRTDTS, TAXUPDDTS
            )
            VALUES (:isin, :okb_id, 1, 2025, :report_date, :currency, :now, :now)
            """
        ),
        {
            "isin": isin,
            "okb_id": okb_id,
            "currency": currency,
            "report_date": date(2025, 7, 28),
            "now": now,
        },
    )


def _insert_fx(connection: sa.Connection, currency: str, rate_date: date, now: datetime) -> None:
    connection.execute(
        text(
            """
            INSERT INTO REFEXC (REFDAT, REFCCY, REFRAT, REFCRTDTS, REFUPDDTS)
            VALUES (:rate_date, :currency, 1.0800000000, :now, :now)
            """
        ),
        {"rate_date": rate_date, "currency": currency, "now": now},
    )
