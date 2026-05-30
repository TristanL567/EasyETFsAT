from __future__ import annotations

from datetime import date, datetime
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy import text

import fondant.db.models  # noqa: F401
from fondant.db.base import Base
from scripts import database_snapshot


def test_snapshot_renders_expected_sections_and_counts() -> None:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    run_id = str(uuid4())
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO SECMDA (SECISN, SECNAM, SECCRTDTS, SECUPDDTS)
                VALUES ('IE00BMTX1Y45', 'Sample ETF', :now, :now)
                """
            ),
            {"now": datetime(2026, 5, 1, 12, 0, 0)},
        )
        connection.execute(
            text(
                """
                INSERT INTO SOURCERPT (
                    SRCISN, SRCOKBIDN, SRCVRN, SRCYEA, SRCMDT, SRCCRTDTS, SRCUPDDTS
                )
                VALUES ('IE00BMTX1Y45', 1001, 1, 2025, :report_date, :now, :now)
                """
            ),
            {"report_date": date(2025, 7, 28), "now": datetime(2026, 5, 1, 12, 0, 0)},
        )
        connection.execute(
            text(
                """
                INSERT INTO TAXRPT (
                    TAXISN, TAXOKBIDN, TAXVRN, TAXYEA, TAXMDT, TAXCRTDTS, TAXUPDDTS
                )
                VALUES ('IE00BMTX1Y45', 1001, 1, 2025, :report_date, :now, :now)
                """
            ),
            {"report_date": date(2025, 7, 28), "now": datetime(2026, 5, 1, 12, 0, 0)},
        )
        connection.execute(
            text(
                """
                INSERT INTO REFEXC (REFDAT, REFCCY, REFRAT, REFCRTDTS, REFUPDDTS)
                VALUES (:rate_date, 'USD', 1.0800000000, :now, :now)
                """
            ),
            {"rate_date": date(2026, 5, 1), "now": datetime(2026, 5, 1, 12, 0, 0)},
        )
        connection.execute(
            text(
                """
                INSERT INTO IMPLOG (
                    IMPRUNIDN, IMPISN, IMPSTS, IMPRSN, IMPRSW, IMPSTADTS, IMPFINDTS,
                    IMPMSG, IMPCRTDTS, IMPUPDDTS
                )
                VALUES (:run_id, 'IE00BMTX1Y45', 'SUCCESS', 1, 1, :now, :now,
                        'ok', :now, :now)
                """
            ),
            {"run_id": run_id, "now": datetime(2026, 5, 1, 12, 0, 0)},
        )
        connection.execute(
            text(
                """
                INSERT INTO IMPERR (
                    IMPRUNIDN, IMPISN, IMPSTG, IMPECD, IMPEMS, IMPCRTDTS, IMPUPDDTS
                )
                VALUES (:run_id, 'IE00BMTX1Y45', 'parse', 'bad_field',
                        'Unknown field', :now, :now)
                """
            ),
            {"run_id": run_id, "now": datetime(2026, 5, 1, 12, 0, 0)},
        )
        connection.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32))"))
        connection.execute(text("INSERT INTO alembic_version VALUES ('20260419_0011')"))
        connection.execute(text('CREATE VIEW "V_TEST_SNAPSHOT" AS SELECT SECISN FROM SECMDA'))

    snapshot = database_snapshot.build_snapshot(engine)

    for heading in database_snapshot.SECTION_HEADINGS:
        assert heading in snapshot

    assert "table | REFEXC" in snapshot
    assert "view  | V_TEST_SNAPSHOT" in snapshot
    assert "table | SECMDA" in snapshot
    assert "IE00BMTX1Y45 | 1              | 1" in snapshot
    assert "USD      | 1" in snapshot
    assert "SUCCESS" in snapshot
    assert "parse | bad_field" in snapshot
    assert "20260419_0011" in snapshot

    engine.dispose()


def test_snapshot_gracefully_reports_missing_optional_tables() -> None:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)

    snapshot = database_snapshot.build_snapshot(engine)

    assert "(report tables missing: SOURCERPT, TAXRPT)" in snapshot
    assert "(table missing: REFEXC)" in snapshot
    assert "(table missing: IMPLOG)" in snapshot
    assert "(table missing: IMPERR)" in snapshot
    assert "(table missing: alembic_version)" in snapshot

    engine.dispose()


def test_sync_database_url_uses_synchronous_drivers() -> None:
    assert (
        database_snapshot.sync_database_url("postgresql+asyncpg://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )
    assert (
        database_snapshot.sync_database_url("sqlite+aiosqlite:///tmp/test.db")
        == "sqlite+pysqlite:///tmp/test.db"
    )
