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


class RecordingECBClient(FakeECBClient):
    def __init__(self, points: list[ECBRatePoint]) -> None:
        super().__init__(points)
        self.requests: list[tuple[list[str], date, date]] = []

    async def get_reference_rates(
        self,
        *,
        currency_codes: list[str],
        start_date: date,
        end_date: date,
    ) -> list[ECBRatePoint]:
        self.requests.append((currency_codes, start_date, end_date))
        return self._points


async def _seed_rates(
    session_factory: async_sessionmaker[AsyncSession],
    rates: list[tuple[date, str, str]],
) -> None:
    async with session_factory() as session:
        session.add_all(
            REFEXC(rate_date=rate_date, currency_code=currency, rate=Decimal(rate))
            for rate_date, currency, rate in rates
        )
        await session.commit()


@pytest.mark.asyncio
async def test_top_up_ecb_rates_starts_at_earliest_latest_stored_date(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", sqlite_session_factory)
    await _seed_rates(
        sqlite_session_factory,
        [
            (date(2026, 5, 27), "USD", "1.1500"),
            (date(2026, 5, 28), "USD", "1.1600"),
            (date(2026, 5, 27), "CHF", "0.9400"),
            (date(2026, 5, 28), "GBP", "0.8500"),
        ],
    )
    points = [
        ECBRatePoint(rate_date=date(2026, 5, 28), currency_code="USD", rate=Decimal("1.1600")),
        ECBRatePoint(rate_date=date(2026, 7, 27), currency_code="USD", rate=Decimal("1.1700")),
        ECBRatePoint(rate_date=date(2026, 7, 27), currency_code="CHF", rate=Decimal("0.9300")),
    ]
    client = RecordingECBClient(points)
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: client)

    result = await fx_pipeline.top_up_ecb_rates(end_date=date(2026, 10, 9))

    assert client.requests == [(["CHF", "GBP", "USD"], date(2026, 5, 27), date(2026, 10, 9))]
    assert result.start_date == date(2026, 5, 27)
    assert result.end_date == date(2026, 10, 9)
    assert result.rates_seen == 3
    assert result.rates_written == 3
    assert result.latest_rate_date == date(2026, 7, 27)

    async with sqlite_session_factory() as session:
        usd = await session.scalar(
            select(REFEXC).where(REFEXC.currency_code == "USD", REFEXC.rate_date == date(2026, 7, 27))
        )
        count = len((await session.scalars(select(REFEXC))).all())

    assert usd is not None and usd.rate == Decimal("1.1700")
    assert count == 6


@pytest.mark.asyncio
async def test_top_up_ecb_rates_backfills_currency_without_stored_rates(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", sqlite_session_factory)
    await _seed_rates(sqlite_session_factory, [(date(2026, 5, 28), "USD", "1.1600")])
    client = RecordingECBClient([])
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: client)

    result = await fx_pipeline.top_up_ecb_rates(currency_codes=["USD", "CHF"], end_date=date(2026, 10, 9))

    assert client.requests == [(["CHF", "USD"], date(2010, 1, 1), date(2026, 10, 9))]
    assert result.rates_written == 0
    assert result.latest_rate_date is None


@pytest.mark.asyncio
async def test_top_up_ecb_rates_dry_run_writes_nothing(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", sqlite_session_factory)
    await _seed_rates(sqlite_session_factory, [(date(2026, 5, 28), "USD", "1.1600")])
    points = [ECBRatePoint(rate_date=date(2026, 7, 27), currency_code="USD", rate=Decimal("1.1700"))]
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: RecordingECBClient(points))

    result = await fx_pipeline.top_up_ecb_rates(
        currency_codes=["USD"], end_date=date(2026, 10, 9), apply=False
    )

    assert result.rates_seen == 1
    assert result.rates_written == 0
    assert result.latest_rate_date == date(2026, 7, 27)

    async with sqlite_session_factory() as session:
        rows = (await session.scalars(select(REFEXC))).all()

    assert [(row.rate_date, row.currency_code) for row in rows] == [(date(2026, 5, 28), "USD")]


@pytest.mark.asyncio
async def test_top_up_ecb_rates_skips_request_when_start_is_after_end(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", sqlite_session_factory)
    await _seed_rates(sqlite_session_factory, [(date(2026, 10, 9), "USD", "1.1600")])
    client = RecordingECBClient([])
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: client)

    result = await fx_pipeline.top_up_ecb_rates(currency_codes=["USD"], end_date=date(2026, 10, 8))

    assert client.requests == []
    assert result.rates_seen == 0
    assert result.rates_written == 0
