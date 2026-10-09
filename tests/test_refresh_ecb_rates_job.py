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
from fondant.jobs import refresh_ecb_rates


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
async def session_factory(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    monkeypatch.setattr(fx_pipeline, "AsyncSessionFactory", factory)

    async with factory() as session:
        session.add_all(
            REFEXC(rate_date=date(2026, 5, 28), currency_code=currency, rate=Decimal(rate))
            for currency, rate in [("USD", "1.1600"), ("GBP", "0.8500"), ("CHF", "0.9400")]
        )
        await session.commit()

    points = [
        ECBRatePoint(rate_date=date(2026, 5, 28), currency_code="USD", rate=Decimal("1.1600")),
        ECBRatePoint(rate_date=date(2026, 7, 27), currency_code="USD", rate=Decimal("1.1700")),
    ]
    monkeypatch.setattr(fx_pipeline, "ECBClient", lambda: FakeECBClient(points))
    yield factory

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


async def _stored_rates(factory: async_sessionmaker[AsyncSession]) -> list[tuple[date, str]]:
    async with factory() as session:
        rows = (await session.scalars(select(REFEXC).order_by(REFEXC.rate_date, REFEXC.currency_code))).all()
    return [(row.rate_date, row.currency_code) for row in rows]


@pytest.mark.asyncio
async def test_dry_run_reports_range_and_writes_nothing(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = await refresh_ecb_rates.run_job(apply=False)

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "mode=dry-run currencies=CHF,GBP,USD start=2026-05-28 " in out
    assert f"end={date.today().isoformat()} " in out
    assert "rates_seen=2 latest_rate_date=2026-07-27 rates_written=0 applied=false" in out
    assert (date(2026, 7, 27), "USD") not in await _stored_rates(session_factory)


@pytest.mark.asyncio
async def test_apply_writes_fetched_rates(
    session_factory: async_sessionmaker[AsyncSession],
    capsys: pytest.CaptureFixture[str],
) -> None:
    exit_code = await refresh_ecb_rates.run_job(apply=True)

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "mode=apply " in out
    assert "rates_seen=2 latest_rate_date=2026-07-27 rates_written=2 applied=true" in out
    assert (date(2026, 7, 27), "USD") in await _stored_rates(session_factory)


def test_parser_defaults_to_dry_run() -> None:
    parser = refresh_ecb_rates._build_parser()

    assert parser.parse_args([]).apply is False
    assert parser.parse_args(["--apply"]).apply is True
