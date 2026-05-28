from __future__ import annotations

from dataclasses import dataclass, field

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.db.models import SECMDA, TAXRPT

DEFAULT_SEARCH_LIMIT = 50
LIKE_ESCAPE = "\\"


@dataclass(frozen=True)
class FundSearchResult:
    isin: str
    name: str
    currency: str | None
    available_tax_years: tuple[int, ...] = field(default_factory=tuple)
    report_count: int = 0


async def has_available_fund_data(session: AsyncSession) -> bool:
    result = await session.execute(sa.select(SECMDA.isin).limit(1))
    return result.scalar_one_or_none() is not None


async def search_available_funds(
    session: AsyncSession,
    query: str,
    *,
    limit: int = DEFAULT_SEARCH_LIMIT,
) -> tuple[FundSearchResult, ...]:
    normalized_query = query.strip()
    if not normalized_query:
        return ()

    normalized_limit = max(1, min(limit, DEFAULT_SEARCH_LIMIT))
    matching_isins = await _matching_isins(session, normalized_query, normalized_limit)
    if not matching_isins:
        return ()

    rows = (
        await session.execute(
            sa.select(
                SECMDA.isin.label("isin"),
                SECMDA.name.label("name"),
                SECMDA.waehrung.label("security_currency"),
                TAXRPT.report_year.label("tax_year"),
                TAXRPT.stm_id.label("report_id"),
                TAXRPT.waehrung.label("report_currency"),
            )
            .select_from(SECMDA)
            .outerjoin(TAXRPT, TAXRPT.isin == SECMDA.isin)
            .where(SECMDA.isin.in_(matching_isins))
            .order_by(SECMDA.isin, TAXRPT.report_year, TAXRPT.stm_id)
        )
    ).mappings().all()

    return _search_results_from_rows(rows, matching_isins)


async def _matching_isins(
    session: AsyncSession,
    query: str,
    limit: int,
) -> tuple[str, ...]:
    pattern = f"%{_escape_like(query)}%"
    result = await session.execute(
        sa.select(SECMDA.isin)
        .select_from(SECMDA)
        .outerjoin(TAXRPT, TAXRPT.isin == SECMDA.isin)
        .where(
            sa.or_(
                SECMDA.isin.ilike(pattern, escape=LIKE_ESCAPE),
                SECMDA.name.ilike(pattern, escape=LIKE_ESCAPE),
                TAXRPT.isin_bez.ilike(pattern, escape=LIKE_ESCAPE),
            )
        )
        .distinct()
        .order_by(SECMDA.isin)
        .limit(limit)
    )
    return tuple(result.scalars().all())


def _search_results_from_rows(
    rows: list[sa.RowMapping],
    matching_isins: tuple[str, ...],
) -> tuple[FundSearchResult, ...]:
    by_isin: dict[str, dict[str, object]] = {
        isin: {
            "isin": isin,
            "name": "",
            "currency": None,
            "years": set(),
            "report_ids": set(),
        }
        for isin in matching_isins
    }

    for row in rows:
        bucket = by_isin[str(row["isin"])]
        bucket["name"] = row["name"] or bucket["name"]
        bucket["currency"] = row["security_currency"] or bucket["currency"] or row["report_currency"]
        if row["tax_year"] is not None:
            years = bucket["years"]
            assert isinstance(years, set)
            years.add(row["tax_year"])
        if row["report_id"] is not None:
            report_ids = bucket["report_ids"]
            assert isinstance(report_ids, set)
            report_ids.add(row["report_id"])

    results: list[FundSearchResult] = []
    for isin in matching_isins:
        bucket = by_isin[isin]
        years = bucket["years"]
        report_ids = bucket["report_ids"]
        assert isinstance(years, set)
        assert isinstance(report_ids, set)
        results.append(
            FundSearchResult(
                isin=isin,
                name=str(bucket["name"]),
                currency=bucket["currency"] if isinstance(bucket["currency"], str) else None,
                available_tax_years=tuple(sorted(years)),
                report_count=len(report_ids),
            )
        )
    return tuple(results)


def _escape_like(value: str) -> str:
    return (
        value.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2)
        .replace("%", f"{LIKE_ESCAPE}%")
        .replace("_", f"{LIKE_ESCAPE}_")
    )
