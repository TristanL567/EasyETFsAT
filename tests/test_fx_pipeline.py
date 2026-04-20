from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.db.base import Base
from fondant.db.models import REFEXC
from fondant.ecb.models import ECBRatePoint
from fondant.ingestion import fx_pipeline


class FakeECBClient:
    def __init__(self, points: list[ECBRatePoint]) -> None:
        self._points = points

    async def __aenter__(self) -> FakeECBClient:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    async def get_reference_rates(
        self,
        *,
        currency_codes: list[str],
        start_date: date,
        end_date: date,
    ) -> list[ECBRatePoint]:
        _ = (currency_codes, start_date, end_date)
        return self._points


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


@pytest.mark.asyncio
async def test_backfill_ecb_rates_upserts(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", sqlite_session_factory)

    points = [
        ECBRatePoint(rate_date=date(2026, 4, 1), currency_code="USD", rate=Decimal("1.1782")),
        ECBRatePoint(rate_date=date(2026, 4, 1), currency_code="CHF", rate=Decimal("0.9191")),
    ]
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: FakeECBClient(points))

    result = await fx_pipeline.backfill_ecb_rates(
        start_date=date(2026, 4, 1),
        end_date=date(2026, 4, 1),
        currency_codes=["USD", "CHF"],
    )

    assert result.rates_seen == 2
    assert result.rates_written == 2

    async with sqlite_session_factory() as session:
        rows = (await session.execute(select(REFEXC).order_by(REFEXC.currency_code))).scalars().all()

    assert len(rows) == 2
    assert rows[0].currency_code == "CHF"
    assert rows[0].rate == Decimal("0.9191")
    assert rows[1].currency_code == "USD"


@pytest.mark.asyncio
async def test_fetch_latest_ecb_rates_picks_latest_per_currency(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", sqlite_session_factory)

    points = [
        ECBRatePoint(rate_date=date(2026, 4, 10), currency_code="USD", rate=Decimal("1.1000")),
        ECBRatePoint(rate_date=date(2026, 4, 11), currency_code="USD", rate=Decimal("1.2000")),
        ECBRatePoint(rate_date=date(2026, 4, 11), currency_code="CHF", rate=Decimal("0.9500")),
    ]
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: FakeECBClient(points))

    result = await fx_pipeline.fetch_latest_ecb_rates(currency_codes=["USD", "CHF"], as_of=date(2026, 4, 12))

    assert result.rates_seen == 2
    assert result.rates_written == 2

    async with sqlite_session_factory() as session:
        usd = await session.scalar(select(REFEXC).where(REFEXC.currency_code == "USD"))
        chf = await session.scalar(select(REFEXC).where(REFEXC.currency_code == "CHF"))

    assert usd is not None and usd.rate_date == date(2026, 4, 11)
    assert usd.rate == Decimal("1.2000")
    assert chf is not None and chf.rate_date == date(2026, 4, 11)
