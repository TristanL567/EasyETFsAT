from collections import defaultdict
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.db.models import TAXCAT, TAXDAT, TAXLIN, TAXRPT
from fondant.db.session import get_session

router = APIRouter(prefix="/etf", tags=["etf"])


@router.get("/{isin}/tax")
async def get_etf_tax(
    isin: str,
    session: Annotated[AsyncSession, Depends(get_session)],
    year: int = Query(..., ge=1900, le=3000),
) -> dict[str, Any]:
    normalized_isin = isin.upper()

    report_rows = (
        await session.execute(
            select(TAXRPT)
            .where(TAXRPT.isin == normalized_isin, TAXRPT.report_year == year)
            .order_by(TAXRPT.meldg_datum.desc())
        )
    ).scalars().all()

    used_null_year_fallback = False
    if not report_rows:
        report_rows = (
            await session.execute(
                select(TAXRPT)
                .where(TAXRPT.isin == normalized_isin, TAXRPT.report_year.is_(None))
                .order_by(TAXRPT.meldg_datum.desc())
            )
        ).scalars().all()
        used_null_year_fallback = bool(report_rows)

    if not report_rows:
        raise HTTPException(status_code=404, detail="No tax data found for ISIN/year")

    report_ids = [row.id for row in report_rows]
    tax_points = (
        await session.execute(
            select(TAXDAT, TAXLIN, TAXCAT)
            .join(TAXLIN, TAXDAT.taxlin_id == TAXLIN.id)
            .join(TAXCAT, TAXDAT.taxcat_id == TAXCAT.id)
            .where(TAXDAT.taxrpt_id.in_(report_ids))
        )
    ).all()

    by_report: dict[int, dict[str, dict[str, float | None]]] = defaultdict(dict)
    for taxdat, taxlin, taxcat in tax_points:
        metric_bucket = by_report[taxdat.taxrpt_id].setdefault(taxlin.metric_key, {})
        metric_bucket[taxcat.category_key] = float(taxdat.amount)

    reports: list[dict[str, Any]] = []
    for taxrpt in report_rows:
        reports.append(
            {
                "stm_id": taxrpt.stm_id,
                "versions_nr": taxrpt.versions_nr,
                "status_code": taxrpt.status_code,
                "waehrung": taxrpt.waehrung,
                "meldg_datum": taxrpt.meldg_datum,
                "tax_fields": by_report.get(taxrpt.id, {}),
            }
        )

    return {
        "isin": normalized_isin,
        "year": year,
        "year_fallback_null_used": used_null_year_fallback,
        "count": len(reports),
        "reports": reports,
    }
