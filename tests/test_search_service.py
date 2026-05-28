from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.db.base import Base
from fondant.db.models import SECMDA, TAXRPT
from fondant.search import has_available_fund_data, search_available_funds


@pytest.fixture
async def search_session() -> AsyncSession:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
async def test_search_available_funds_matches_name_and_aggregates_coverage(
    search_session: AsyncSession,
) -> None:
    search_session.add_all(
        [
            SECMDA(isin="IE00BMTX1Y45", name="Vanguard Example UCITS ETF", waehrung="EUR"),
            SECMDA(isin="LU1681044993", name="Global Bond Fund", waehrung="CHF"),
            TAXRPT(
                isin="IE00BMTX1Y45",
                stm_id=1001,
                versions_nr=1,
                report_year=2024,
                meldg_datum=date(2024, 6, 30),
                waehrung="EUR",
                isin_bez="Vanguard Example UCITS ETF",
            ),
            TAXRPT(
                isin="IE00BMTX1Y45",
                stm_id=1002,
                versions_nr=1,
                report_year=2025,
                meldg_datum=date(2025, 6, 30),
                waehrung="EUR",
                isin_bez="Vanguard Example UCITS ETF",
            ),
        ]
    )
    await search_session.commit()

    assert await has_available_fund_data(search_session) is True

    results = await search_available_funds(search_session, "vanguard")

    assert len(results) == 1
    assert results[0].isin == "IE00BMTX1Y45"
    assert results[0].name == "Vanguard Example UCITS ETF"
    assert results[0].currency == "EUR"
    assert results[0].available_tax_years == (2024, 2025)
    assert results[0].report_count == 2


@pytest.mark.asyncio
async def test_search_available_funds_matches_isin_case_insensitively(
    search_session: AsyncSession,
) -> None:
    search_session.add(
        SECMDA(isin="LU1681044993", name="Global Bond Fund", waehrung="CHF")
    )
    await search_session.commit()

    results = await search_available_funds(search_session, "lu1681044993")

    assert len(results) == 1
    assert results[0].isin == "LU1681044993"
    assert results[0].name == "Global Bond Fund"
    assert results[0].currency == "CHF"
    assert results[0].available_tax_years == ()
    assert results[0].report_count == 0


@pytest.mark.asyncio
async def test_search_available_funds_returns_empty_for_no_match(
    search_session: AsyncSession,
) -> None:
    search_session.add(
        SECMDA(isin="IE00BMTX1Y45", name="Vanguard Example UCITS ETF", waehrung="EUR")
    )
    await search_session.commit()

    assert await search_available_funds(search_session, "missing") == ()


@pytest.mark.asyncio
async def test_search_empty_database_state(search_session: AsyncSession) -> None:
    assert await has_available_fund_data(search_session) is False
    assert await search_available_funds(search_session, "IE00BMTX1Y45") == ()
