from __future__ import annotations

import socket
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

import fondant.db.models  # noqa: F401
from fondant.db.base import Base
from scripts import ingestion_health


class PostgreSQLConnectionStub:
    dialect = postgresql.dialect()


def test_all_success_update_classifies_successful_update() -> None:
    engine = _create_engine()
    _insert_runs(engine, [("SUCCESS", 3, 2), ("SUCCESS", 1, 1)])

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 0
    assert "classification: successful_update" in report
    assert "success_count: 2" in report
    assert "records_written_total: 3" in report

    engine.dispose()


def test_healthy_noop_refresh_classifies_success_without_writes() -> None:
    engine = _create_engine()
    _insert_runs(engine, [("SUCCESS", 3, 0), ("SUCCESS", 1, 0)])

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 0
    assert "classification: healthy_noop_refresh" in report
    assert "records_seen_total: 4" in report
    assert "records_written_total: 0" in report

    engine.dispose()


def test_mixed_failure_classifies_isolated_failure() -> None:
    engine = _create_engine()
    run_ids = _insert_runs(engine, [("SUCCESS", 1, 1), ("FAILED", 0, 0), ("SUCCESS", 2, 0)])
    _insert_error(engine, run_ids[1], stage="parse", code="bad_payload", message="Invalid payload")

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 0
    assert "classification: isolated_failure" in report
    assert "failure_count: 1" in report
    assert "parse | bad_payload" in report

    engine.dispose()


def test_all_failure_classifies_systemic_failure_with_connection_evidence() -> None:
    engine = _create_engine()
    run_ids = _insert_runs(engine, [("FAILED", 0, 0), ("FAILED", 0, 0), ("FAILED", 0, 0)])
    for run_id in run_ids:
        _insert_error(
            engine,
            run_id,
            stage="oekb_client",
            code="connection_timeout",
            message="Connection timed out while calling OeKB",
        )

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 0
    assert "classification: systemic_failure" in report
    assert "failure_count: 3" in report
    assert "oekb_client | connection_timeout | 3" in report

    engine.dispose()


def test_repeated_connection_errors_classify_systemic_even_with_mixed_runs() -> None:
    engine = _create_engine()
    run_ids = _insert_runs(engine, [("SUCCESS", 1, 1), ("FAILED", 0, 0), ("FAILED", 0, 0)])
    for run_id in run_ids[1:]:
        _insert_error(
            engine,
            run_id,
            stage="oekb_client",
            code="http_client_error",
            message="HTTP client connection refused",
        )

    with engine.connect() as connection:
        summary = ingestion_health.inspect_ingestion_health(connection)

    assert summary.classification == "systemic_failure"
    assert "connection/client failure" in summary.detail

    engine.dispose()


def test_no_recent_runs_classifies_no_recent_runs() -> None:
    engine = _create_engine()

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 0
    assert "classification: no_recent_runs" in report
    assert "recent_runs_inspected: 0" in report

    engine.dispose()


def test_missing_schema_classifies_schema_unavailable_and_exits_nonzero() -> None:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 2
    assert "classification: schema_unavailable" in report
    assert "Missing required ingestion log tables" in report

    engine.dispose()


def test_health_check_does_not_use_network(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _create_engine()
    _insert_runs(engine, [("SUCCESS", 1, 0)])

    def fail_socket(*_args: object, **_kwargs: object) -> socket.socket:
        raise AssertionError("network access is not allowed")

    monkeypatch.setattr(socket, "socket", fail_socket)

    report, exit_code = ingestion_health.build_health_report(engine)

    assert exit_code == 0
    assert "classification: healthy_noop_refresh" in report

    engine.dispose()


def test_sync_database_url_uses_synchronous_drivers() -> None:
    assert (
        ingestion_health.sync_database_url("postgresql+asyncpg://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )
    assert (
        ingestion_health.sync_database_url("sqlite+aiosqlite:///tmp/test.db")
        == "sqlite+pysqlite:///tmp/test.db"
    )


def test_postgresql_query_text_quotes_uppercase_identifiers() -> None:
    connection = PostgreSQLConnectionStub()

    run_sql = str(ingestion_health._recent_runs_statement(connection))
    error_sql = str(ingestion_health._recent_error_groups_statement(connection))

    for identifier in (
        "IMPLOG",
        "IMPRUNIDN",
        "IMPISN",
        "IMPSTS",
        "IMPRSN",
        "IMPRSW",
        "IMPSTADTS",
        "IMPFINDTS",
        "IMPMSG",
        "IMPCRTDTS",
    ):
        assert f'"{identifier}"' in run_sql

    for identifier in (
        "IMPERR",
        "IMPSTG",
        "IMPECD",
        "IMPRUNIDN",
        "IMPCRTDTS",
        "IMPEMS",
    ):
        assert f'"{identifier}"' in error_sql


def _create_engine() -> sa.Engine:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


def _insert_runs(engine: sa.Engine, rows: list[tuple[str, int, int]]) -> list[str]:
    now = datetime(2026, 5, 1, 12, 0, 0)
    run_ids = [str(uuid4()) for _ in rows]
    with engine.begin() as connection:
        for index, (status, records_seen, records_written) in enumerate(rows):
            timestamp = now - timedelta(minutes=index)
            connection.execute(
                text(
                    """
                    INSERT INTO IMPLOG (
                        IMPRUNIDN, IMPISN, IMPSTS, IMPRSN, IMPRSW, IMPSTADTS, IMPFINDTS,
                        IMPMSG, IMPCRTDTS, IMPUPDDTS
                    )
                    VALUES (
                        :run_id, :isin, :status, :records_seen, :records_written, :started_at,
                        :finished_at, :message, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "run_id": run_ids[index],
                    "isin": f"IE00TEST{index:04d}",
                    "status": status,
                    "records_seen": records_seen,
                    "records_written": records_written,
                    "started_at": timestamp,
                    "finished_at": timestamp,
                    "message": status.lower(),
                    "created_at": timestamp,
                    "updated_at": timestamp,
                },
            )
    return run_ids


def _insert_error(
    engine: sa.Engine,
    run_id: str,
    *,
    stage: str,
    code: str,
    message: str,
) -> None:
    now = datetime(2026, 5, 1, 12, 0, 0)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO IMPERR (
                    IMPRUNIDN, IMPISN, IMPSTG, IMPECD, IMPEMS, IMPCRTDTS, IMPUPDDTS
                )
                VALUES (:run_id, 'IE00TESTERR', :stage, :code, :message, :now, :now)
                """
            ),
            {
                "run_id": run_id,
                "stage": stage,
                "code": code,
                "message": message,
                "now": now,
            },
        )
