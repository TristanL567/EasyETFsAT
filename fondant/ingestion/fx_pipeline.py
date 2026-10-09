from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from fondant.db.models import REFEXC
from fondant.db.session import AsyncSessionFactory
from fondant.ecb.client import ECBClient
from fondant.ecb.models import ECBRatePoint

DEFAULT_ECB_CURRENCIES = ["USD", "GBP", "CHF"]
DEFAULT_BACKFILL_START = date(2010, 1, 1)


@dataclass(slots=True)
class FXIngestionResult:
    rates_seen: int
    rates_written: int
    start_date: date
    end_date: date
    currencies: list[str]
    latest_rate_date: date | None = None


async def backfill_ecb_rates(
    *,
    start_date: date = DEFAULT_BACKFILL_START,
    end_date: date | None = None,
    currency_codes: list[str] | None = None,
) -> FXIngestionResult:
    end = end_date or date.today()
    currencies = _normalize_currencies(currency_codes)

    async with ECBClient() as client:
        points = await client.get_reference_rates(
            currency_codes=currencies,
            start_date=start_date,
            end_date=end,
        )

    written = await _upsert_points(points)
    return FXIngestionResult(
        rates_seen=len(points),
        rates_written=written,
        start_date=start_date,
        end_date=end,
        currencies=currencies,
    )


async def fetch_latest_ecb_rates(
    *,
    currency_codes: list[str] | None = None,
    as_of: date | None = None,
    lookback_days: int = 7,
) -> FXIngestionResult:
    # ECB can miss weekends/holidays, so we request a short lookback window
    # and keep the latest available observation per currency.
    end = as_of or (date.today() - timedelta(days=1))
    start = end - timedelta(days=max(1, lookback_days))
    currencies = _normalize_currencies(currency_codes)

    async with ECBClient() as client:
        points = await client.get_reference_rates(
            currency_codes=currencies,
            start_date=start,
            end_date=end,
        )

    latest_by_currency: dict[str, ECBRatePoint] = {}
    for point in points:
        prev = latest_by_currency.get(point.currency_code)
        if prev is None or point.rate_date > prev.rate_date:
            latest_by_currency[point.currency_code] = point

    latest_points = list(latest_by_currency.values())
    written = await _upsert_points(latest_points)

    return FXIngestionResult(
        rates_seen=len(latest_points),
        rates_written=written,
        start_date=start,
        end_date=end,
        currencies=currencies,
    )


async def top_up_ecb_rates(
    *,
    currency_codes: list[str] | None = None,
    end_date: date | None = None,
    apply: bool = True,
) -> FXIngestionResult:
    # Start at the earliest per-currency latest stored date, inclusive: missed
    # runs leave no gaps, and the request always covers a published ECB day.
    end = end_date or date.today()
    currencies = _normalize_currencies(currency_codes)
    start = await _top_up_start_date(currencies)
    if start > end:
        return FXIngestionResult(
            rates_seen=0,
            rates_written=0,
            start_date=start,
            end_date=end,
            currencies=currencies,
        )

    async with ECBClient() as client:
        points = await client.get_reference_rates(
            currency_codes=currencies,
            start_date=start,
            end_date=end,
        )

    written = await _upsert_points(points) if apply else 0
    return FXIngestionResult(
        rates_seen=len(points),
        rates_written=written,
        start_date=start,
        end_date=end,
        currencies=currencies,
        latest_rate_date=max((point.rate_date for point in points), default=None),
    )


async def _top_up_start_date(currencies: list[str]) -> date:
    async with AsyncSessionFactory() as session:
        rows = (
            await session.execute(
                select(REFEXC.currency_code, func.max(REFEXC.rate_date))
                .where(REFEXC.currency_code.in_(currencies))
                .group_by(REFEXC.currency_code)
            )
        ).all()

    latest_by_currency = {currency: latest for currency, latest in rows}
    if any(latest_by_currency.get(currency) is None for currency in currencies):
        return DEFAULT_BACKFILL_START
    return min(latest_by_currency.values())


def _normalize_currencies(currency_codes: list[str] | None) -> list[str]:
    base = currency_codes or DEFAULT_ECB_CURRENCIES
    return sorted({code.upper() for code in base if code})


async def _upsert_points(points: list[ECBRatePoint]) -> int:
    if not points:
        return 0

    rows = [
        {
            "rate_date": point.rate_date,
            "currency_code": point.currency_code,
            "rate": point.rate,
        }
        for point in points
    ]

    async with AsyncSessionFactory() as session:
        bind = session.get_bind()
        if bind is None:
            raise RuntimeError("Session is not bound to an engine.")

        dialect = bind.dialect.name
        insert_fn = pg_insert if dialect == "postgresql" else sqlite_insert

        chunk_size = 1000
        for idx in range(0, len(rows), chunk_size):
            chunk = rows[idx : idx + chunk_size]
            stmt = insert_fn(REFEXC).values(chunk)
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                index_elements=[REFEXC.rate_date, REFEXC.currency_code],
                set_={
                    "REFRAT": excluded.REFRAT,
                    "REFUPDDTS": func.now(),
                },
            )
            await session.execute(stmt)
        await session.commit()

    return len(rows)
