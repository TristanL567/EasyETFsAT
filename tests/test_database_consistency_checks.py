from __future__ import annotations

from datetime import date, datetime

import sqlalchemy as sa
from sqlalchemy import text

import fondant.db.models  # noqa: F401
from fondant.db.base import Base
from scripts import database_consistency_checks


def test_consistency_checks_pass_for_coherent_source_and_curated_fixture() -> None:
    engine = _create_engine()
    _seed_consistent_fixture(engine)

    report, passed = database_consistency_checks.build_consistency_report(engine)

    assert passed is True
    for heading in database_consistency_checks.SECTION_HEADINGS:
        assert heading in report
    assert "status: FAIL" not in report
    assert "Queries are SELECT/introspection only." in report

    engine.dispose()


def test_consistency_checks_fail_with_actionable_context_for_broken_fixture() -> None:
    engine = _create_engine()
    _seed_broken_fixture(engine)

    report, passed = database_consistency_checks.build_consistency_report(engine)

    assert passed is False
    assert "== Source To Curated Report Alignment ==" in report
    assert "status: FAIL" in report
    assert "IE00BROKEN01" in report
    assert "9001" in report
    assert "taxdat_id" in report
    assert "expected_taxrpt_id" in report
    assert "taxadj_id" in report
    assert "adjustment_code" in report
    assert "taxlin_id" in report
    assert "taxcat_id" in report

    engine.dispose()


def test_consistency_checks_enforce_inactive_taxcat_when_column_exists() -> None:
    engine = _create_engine()
    _seed_consistent_fixture(engine)

    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE TAXCAT ADD COLUMN TAXACT BOOLEAN DEFAULT 1"))
        connection.execute(text("UPDATE TAXCAT SET TAXACT = 0 WHERE TAXIDN = 21"))

    report, passed = database_consistency_checks.build_consistency_report(engine)

    assert passed is False
    assert "== TAXDAT Tax Category Dictionary Integrity ==" in report
    assert "tax_category_active" in report
    assert "IE00BMTX1Y45" in report
    assert "1001" in report

    engine.dispose()


def test_consistency_checks_fail_clearly_for_missing_tables() -> None:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)

    report, passed = database_consistency_checks.build_consistency_report(engine)

    assert passed is False
    assert "== Schema Diagnostics ==" in report
    assert "missing required table" in report
    assert "SOURCERPT" in report

    engine.dispose()


def test_sync_database_url_uses_synchronous_drivers() -> None:
    assert (
        database_consistency_checks.sync_database_url("postgresql+asyncpg://user:pass@host/db")
        == "postgresql+psycopg://user:pass@host/db"
    )
    assert (
        database_consistency_checks.sync_database_url("sqlite+aiosqlite:///tmp/test.db")
        == "sqlite+pysqlite:///tmp/test.db"
    )


def _create_engine() -> sa.Engine:
    engine = sa.create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return engine


def _seed_consistent_fixture(engine: sa.Engine) -> None:
    now = datetime(2026, 5, 1, 12, 0, 0)
    with engine.begin() as connection:
        _insert_security(connection, "IE00BMTX1Y45", now)
        _insert_source_report_bundle(connection, "IE00BMTX1Y45", 1001, now, status="FIN")
        _insert_tax_dictionaries(connection, taxlin_id=11, taxcat_id=21, taxlin_active=True, now=now)
        _insert_tax_report(connection, taxrpt_id=31, isin="IE00BMTX1Y45", okb_id=1001, now=now)
        _insert_taxdat(connection, taxdat_id=41, taxrpt_id=31, okb_id=1001, taxlin_id=11, taxcat_id=21, now=now)
        _insert_taxadj(connection, taxadj_id=51, taxrpt_id=31, okb_id=1001, taxcat_id=21, now=now)


def _seed_broken_fixture(engine: sa.Engine) -> None:
    now = datetime(2026, 5, 1, 12, 0, 0)
    with engine.begin() as connection:
        _insert_security(connection, "IE00BROKEN01", now)
        _insert_source_report_bundle(connection, "IE00BROKEN01", 9001, now, status="FIN")
        _insert_tax_dictionaries(connection, taxlin_id=11, taxcat_id=21, taxlin_active=False, now=now)
        _insert_tax_report(connection, taxrpt_id=31, isin="IE00BROKEN01", okb_id=9002, now=now)
        _insert_taxdat(connection, taxdat_id=41, taxrpt_id=31, okb_id=9002, taxlin_id=11, taxcat_id=21, now=now)
        _insert_taxdat(connection, taxdat_id=42, taxrpt_id=999, okb_id=9003, taxlin_id=999, taxcat_id=999, now=now)
        _insert_taxadj(connection, taxadj_id=51, taxrpt_id=999, okb_id=9003, taxcat_id=21, now=now)


def _insert_security(connection: sa.Connection, isin: str, now: datetime) -> None:
    connection.execute(
        text(
            """
            INSERT INTO SECMDA (SECISN, SECNAM, SECCRTDTS, SECUPDDTS)
            VALUES (:isin, :name, :now, :now)
            """
        ),
        {"isin": isin, "name": "Sample ETF", "now": now},
    )


def _insert_source_report_bundle(
    connection: sa.Connection,
    isin: str,
    okb_id: int,
    now: datetime,
    *,
    status: str,
) -> None:
    parameters = {
        "isin": isin,
        "okb_id": okb_id,
        "report_date": date(2025, 7, 28),
        "now": now,
        "status": status,
    }
    connection.execute(
        text(
            """
            INSERT INTO SOURCERPT (
                SRCISN, SRCOKBIDN, SRCVRN, SRCSTS, SRCYEA, SRCMDT, SRCCRTDTS, SRCUPDDTS
            )
            VALUES (:isin, :okb_id, 1, :status, 2025, :report_date, :now, :now)
            """
        ),
        parameters,
    )
    connection.execute(
        text(
            """
            INSERT INTO SOURCERAW (SRCISN, SRCOKBIDN, SRCVRN, SRCPAY, SRCCRTDTS, SRCUPDDTS)
            VALUES (:isin, :okb_id, 1, '{}', :now, :now)
            """
        ),
        parameters,
    )
    connection.execute(
        text(
            """
            INSERT INTO SOURCEAGE (SRCISN, SRCOKBIDN, SRCVRN, SRCYEA, SRCCRTDTS, SRCUPDDTS)
            VALUES (:isin, :okb_id, 1, 2025, :now, :now)
            """
        ),
        parameters,
    )


def _insert_tax_dictionaries(
    connection: sa.Connection,
    *,
    taxlin_id: int,
    taxcat_id: int,
    taxlin_active: bool,
    now: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO TAXLIN (
                TAXIDN, TAXCOD, TAXKEY, TAXNDE, TAXORD, TAXACT, TAXCRTDTS, TAXUPDDTS
            )
            VALUES (:id, 'K61', 'korrekturbetrag_age_ak', 'K61', 1, :active, :now, :now)
            """
        ),
        {"id": taxlin_id, "active": taxlin_active, "now": now},
    )
    connection.execute(
        text(
            """
            INSERT INTO TAXCAT (TAXIDN, TAXCOD, TAXKEY, TAXNDE, TAXORD, TAXCRTDTS, TAXUPDDTS)
            VALUES (:id, 'PVM', 'pv_mit', 'Privat mit Option', 1, :now, :now)
            """
        ),
        {"id": taxcat_id, "now": now},
    )


def _insert_tax_report(
    connection: sa.Connection,
    *,
    taxrpt_id: int,
    isin: str,
    okb_id: int,
    now: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO TAXRPT (
                TAXIDN, TAXISN, TAXOKBIDN, TAXVRN, TAXSTS, TAXYEA, TAXMDT,
                TAXCRTDTS, TAXUPDDTS
            )
            VALUES (:taxrpt_id, :isin, :okb_id, 1, 'FIN', 2025, :report_date, :now, :now)
            """
        ),
        {
            "taxrpt_id": taxrpt_id,
            "isin": isin,
            "okb_id": okb_id,
            "report_date": date(2025, 7, 28),
            "now": now,
        },
    )


def _insert_taxdat(
    connection: sa.Connection,
    *,
    taxdat_id: int,
    taxrpt_id: int,
    okb_id: int,
    taxlin_id: int,
    taxcat_id: int,
    now: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO TAXDAT (
                TAXIDN, TAXRPTIDN, TAXOKBIDN, TAXLINIDN, TAXCATIDN, TAXAMT,
                TAXCRTDTS, TAXUPDDTS
            )
            VALUES (:taxdat_id, :taxrpt_id, :okb_id, :taxlin_id, :taxcat_id, 1.23, :now, :now)
            """
        ),
        {
            "taxdat_id": taxdat_id,
            "taxrpt_id": taxrpt_id,
            "okb_id": okb_id,
            "taxlin_id": taxlin_id,
            "taxcat_id": taxcat_id,
            "now": now,
        },
    )


def _insert_taxadj(
    connection: sa.Connection,
    *,
    taxadj_id: int,
    taxrpt_id: int,
    okb_id: int,
    taxcat_id: int,
    now: datetime,
) -> None:
    connection.execute(
        text(
            """
            INSERT INTO TAXADJ (
                TAXIDN, TAXRPTIDN, TAXOKBIDN, TAXCATIDN, TAXCOD, TAXAMT,
                TAXCRTDTS, TAXUPDDTS
            )
            VALUES (:taxadj_id, :taxrpt_id, :okb_id, :taxcat_id, 'AKC', 1.23, :now, :now)
            """
        ),
        {
            "taxadj_id": taxadj_id,
            "taxrpt_id": taxrpt_id,
            "okb_id": okb_id,
            "taxcat_id": taxcat_id,
            "now": now,
        },
    )
