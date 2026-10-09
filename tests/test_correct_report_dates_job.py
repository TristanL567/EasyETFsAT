from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from alembic.config import Config
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from alembic import command
from fondant.config import get_settings
from fondant.db.base import Base
from fondant.db.models import REFEXC, SECDIV, SECMDA, SOURCEAGE, SOURCERPT, TAXRPT
from fondant.jobs import correct_report_dates


@pytest.fixture
async def sqlite_session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    yield factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


def _add_report(
    session: AsyncSession,
    *,
    isin: str,
    stm_id: int,
    currency: str,
    meldg_datum: date,
    report_year: int,
    zufluss: date | None,
) -> None:
    session.add(SECMDA(isin=isin, name=f"{isin} fund", waehrung=currency))
    common = {
        "isin": isin,
        "stm_id": stm_id,
        "versions_nr": 1,
        "status_code": "FIN",
        "report_year": report_year,
        "meldg_datum": meldg_datum,
        "waehrung": currency,
        "zufluss": zufluss,
    }
    session.add(SOURCERPT(**common))
    session.add(TAXRPT(**common))
    session.add(SOURCEAGE(isin=isin, stm_id=stm_id, versions_nr=1, report_year=report_year))
    if zufluss is not None:
        session.add(
            SECDIV(
                isin=isin,
                okb_id=stm_id,
                flow_type="DIST",
                flow_date=zufluss,
                waehrung=currency,
                report_year=report_year,
                status_code="FIN",
            )
        )


async def _seed(factory: async_sessionmaker[AsyncSession]) -> None:
    async with factory() as session:
        _add_report(
            session,
            isin="IE000XZSV718",
            stm_id=626766,
            currency="USD",
            meldg_datum=date(2025, 10, 24),
            report_year=2025,
            zufluss=date(2025, 10, 27),
        )
        _add_report(
            session,
            isin="IE00B41RYL63",
            stm_id=626718,
            currency="EUR",
            meldg_datum=date(2025, 10, 24),
            report_year=2025,
            zufluss=date(2025, 10, 27),
        )
        _add_report(
            session,
            isin="IE00YEAREND1",
            stm_id=700001,
            currency="EUR",
            meldg_datum=date(2025, 12, 30),
            report_year=2025,
            zufluss=date(2026, 1, 2),
        )
        _add_report(
            session,
            isin="IE00NOZFL001",
            stm_id=700002,
            currency="USD",
            meldg_datum=date(2025, 3, 3),
            report_year=2025,
            zufluss=None,
        )
        session.add(REFEXC(rate_date=date(2025, 10, 24), currency_code="USD", rate=Decimal("1.1612")))
        await session.commit()


async def _report_dates(factory: async_sessionmaker[AsyncSession], model: type) -> dict[int, tuple]:
    async with factory() as session:
        rows = (await session.scalars(select(model))).all()
        return {row.stm_id: (row.meldg_datum, row.report_year) for row in rows}


async def _years(factory: async_sessionmaker[AsyncSession], model: type, key: str) -> dict[int, int]:
    async with factory() as session:
        rows = (await session.scalars(select(model))).all()
        return {getattr(row, key): row.report_year for row in rows}


@pytest.mark.asyncio
async def test_dry_run_lists_moves_and_missing_rates_without_writing(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(correct_report_dates, "AsyncSessionFactory", sqlite_session_factory)
    await _seed(sqlite_session_factory)
    before = await _report_dates(sqlite_session_factory, TAXRPT)

    exit_code = await correct_report_dates.run_job(apply=False)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "TAXRPT IE000XZSV718 626766: 2025-10-24/2025 -> 2025-10-27/2025" in output
    assert "SOURCERPT IE00B41RYL63 626718: 2025-10-24/2025 -> 2025-10-27/2025" in output
    assert "TAXRPT IE00YEAREND1 700001: 2025-12-30/2025 -> 2026-01-02/2026" in output
    assert "SOURCEAGE IE00YEAREND1 700001: 2025 -> 2026" in output
    assert "SECDIV IE00YEAREND1 700001: 2025 -> 2026" in output
    assert "missing_fx TAXRPT IE000XZSV718 626766 USD 2025-10-27" in output
    assert "700002" not in output
    assert "taxrpt_moved=3" in output
    assert "applied=false" in output
    assert await _report_dates(sqlite_session_factory, TAXRPT) == before


@pytest.mark.asyncio
async def test_apply_dates_reports_by_zufluss_and_is_idempotent(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(correct_report_dates, "AsyncSessionFactory", sqlite_session_factory)
    await _seed(sqlite_session_factory)

    assert await correct_report_dates.run_job(apply=True) == 0
    assert "applied=true" in capsys.readouterr().out

    expected = {
        626766: (date(2025, 10, 27), 2025),
        626718: (date(2025, 10, 27), 2025),
        700001: (date(2026, 1, 2), 2026),
        700002: (date(2025, 3, 3), 2025),
    }
    assert await _report_dates(sqlite_session_factory, TAXRPT) == expected
    assert await _report_dates(sqlite_session_factory, SOURCERPT) == expected
    assert (await _years(sqlite_session_factory, SOURCEAGE, "stm_id"))[700001] == 2026
    assert (await _years(sqlite_session_factory, SOURCEAGE, "stm_id"))[700002] == 2025
    assert (await _years(sqlite_session_factory, SECDIV, "okb_id"))[700001] == 2026

    assert await correct_report_dates.run_job(apply=True) == 0
    second = capsys.readouterr().out
    assert "sourcerpt_moved=0 taxrpt_moved=0 sourceage_year_moved=0 secdiv_year_moved=0" in second


@pytest.mark.asyncio
async def test_source_age_year_is_corrected_when_sourcerpt_was_already_refreshed(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(correct_report_dates, "AsyncSessionFactory", sqlite_session_factory)
    await _seed(sqlite_session_factory)
    async with sqlite_session_factory() as session:
        source = await session.scalar(select(SOURCERPT).where(SOURCERPT.stm_id == 700001))
        source.meldg_datum = date(2026, 1, 2)
        source.report_year = 2026
        await session.commit()

    assert await correct_report_dates.run_job(apply=True) == 0

    output = capsys.readouterr().out
    assert "SOURCERPT IE00YEAREND1 700001" not in output
    assert "SOURCEAGE IE00YEAREND1 700001: 2025 -> 2026" in output
    assert (await _years(sqlite_session_factory, SOURCEAGE, "stm_id"))[700001] == 2026


@pytest.mark.asyncio
async def test_corrected_usd_report_uses_rate_of_zufluss_in_homccy_view(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "correct_report_dates.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    get_settings.cache_clear()
    command.upgrade(Config("alembic.ini"), "head")

    sync_engine = create_engine(f"sqlite:///{db_path}")
    with sync_engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO "SECMDA" ("SECISN", "SECNAM", "SECCCY", "SECCRTDTS", "SECUPDDTS")
                VALUES ('IE000XZSV718', 'USD fund', 'USD', :ts, :ts)
                """
            ),
            {"ts": "2026-10-09 00:00:00"},
        )
        connection.execute(
            text(
                """
                INSERT INTO "SOURCERPT"
                    ("SRCISN", "SRCOKBIDN", "SRCVRN", "SRCYEA", "SRCMDT", "SRCCCY", "SRCZFL",
                     "SRCCRTDTS", "SRCUPDDTS")
                VALUES ('IE000XZSV718', 626766, 5, 2025, '2025-10-24', 'USD', '2025-10-27', :ts, :ts)
                """
            ),
            {"ts": "2026-10-09 00:00:00"},
        )
        connection.execute(
            text(
                """
                INSERT INTO "TAXRPT"
                    ("TAXISN", "TAXOKBIDN", "TAXVRN", "TAXYEA", "TAXMDT", "TAXCCY", "TAXZFL",
                     "TAXCRTDTS", "TAXUPDDTS")
                VALUES ('IE000XZSV718', 626766, 5, 2025, '2025-10-24', 'USD', '2025-10-27', :ts, :ts)
                """
            ),
            {"ts": "2026-10-09 00:00:00"},
        )
        connection.execute(
            text(
                """
                INSERT INTO "REFEXC" ("REFDAT", "REFCCY", "REFRAT", "REFCRTDTS", "REFUPDDTS")
                VALUES ('2025-10-24', 'USD', 1.1612, :ts, :ts), ('2025-10-27', 'USD', 1.1640, :ts, :ts)
                """
            ),
            {"ts": "2026-10-09 00:00:00"},
        )

    async_engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
    factory = async_sessionmaker(bind=async_engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(correct_report_dates, "AsyncSessionFactory", factory)
    try:
        assert await correct_report_dates.run_job(apply=True) == 0
    finally:
        await async_engine.dispose()

    with sync_engine.connect() as connection:
        row = connection.execute(
            text('SELECT "TAXMDT", "FXRAT" FROM "V2_TAXDATHOMCCY" WHERE "TAXOKBIDN" = 626766')
        ).one()
    assert str(row.TAXMDT) == "2025-10-27"
    assert Decimal(str(row.FXRAT)) == Decimal("1.1640")
    assert len(inspect(sync_engine).get_columns("V2_TAXDATHOMCCY")) == 138
    sync_engine.dispose()
