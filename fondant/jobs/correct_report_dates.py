"""Date stored OeKB reports by their stored Zufluss (one-time correction).

OeKB's Meldedatum is the steuerlicher Zufluss. Reports ingested before the
parser preferred `zufluss` may be dated by `eintragezeit` instead. TAXRPT is only
re-curated when a report's payload changes, so this job corrects stored rows.

Dry run by default: lists every row it would move and every moved non-EUR
report without an ECB rate on its corrected date. Writes only with --apply.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import date

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.db.models import REFEXC, SECDIV, SOURCEAGE, SOURCERPT, TAXRPT
from fondant.db.session import AsyncSessionFactory


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Date stored OeKB reports by their stored Zufluss. Dry run unless --apply is given."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write the corrections. Without it, only list what would change.",
    )
    return parser


def _needs_move(meldg_datum: date | None, report_year: int | None, zufluss: date) -> bool:
    return meldg_datum != zufluss or report_year != zufluss.year


def _format_date_year(meldg_datum: date | None, report_year: int | None) -> str:
    return f"{meldg_datum.isoformat() if meldg_datum else None}/{report_year}"


async def _correct_reports(session: AsyncSession, model: type, *, apply: bool) -> list:
    rows = (
        await session.scalars(
            select(model).where(model.zufluss.is_not(None)).order_by(model.isin, model.stm_id)
        )
    ).all()
    moved = [row for row in rows if _needs_move(row.meldg_datum, row.report_year, row.zufluss)]
    for row in moved:
        print(
            f"{model.__tablename__} {row.isin} {row.stm_id}: "
            f"{_format_date_year(row.meldg_datum, row.report_year)} -> "
            f"{_format_date_year(row.zufluss, row.zufluss.year)}"
        )
        if apply:
            row.meldg_datum = row.zufluss
            row.report_year = row.zufluss.year
    return moved


async def _correct_year_copies(
    session: AsyncSession,
    model: type,
    report_model: type,
    report_id_column,
    *,
    apply: bool,
) -> int:
    pairs = (
        await session.execute(
            select(model, report_model.zufluss)
            .join(
                report_model,
                and_(model.isin == report_model.isin, report_id_column == report_model.stm_id),
            )
            .where(report_model.zufluss.is_not(None))
            .order_by(model.isin, report_id_column)
        )
    ).all()
    moved = 0
    for row, zufluss in pairs:
        if row.report_year == zufluss.year:
            continue
        moved += 1
        report_id = getattr(row, report_id_column.key)
        print(f"{model.__tablename__} {row.isin} {report_id}: {row.report_year} -> {zufluss.year}")
        if apply:
            row.report_year = zufluss.year
    return moved


async def _report_missing_rates(session: AsyncSession, tax_reports: list[TAXRPT]) -> int:
    missing = 0
    for report in tax_reports:
        if report.waehrung is None or report.waehrung == "EUR":
            continue
        rate = await session.scalar(
            select(REFEXC.id).where(
                REFEXC.currency_code == report.waehrung,
                REFEXC.rate_date == report.zufluss,
            )
        )
        if rate is None:
            missing += 1
            print(
                f"missing_fx TAXRPT {report.isin} {report.stm_id} "
                f"{report.waehrung} {report.zufluss.isoformat()}"
            )
    return missing


async def run_job(*, apply: bool) -> int:
    print(f"mode={'apply' if apply else 'dry-run'}")
    async with AsyncSessionFactory() as session:
        sourcerpt_moved = await _correct_reports(session, SOURCERPT, apply=apply)
        taxrpt_moved = await _correct_reports(session, TAXRPT, apply=apply)
        sourceage_moved = await _correct_year_copies(
            session, SOURCEAGE, SOURCERPT, SOURCEAGE.stm_id, apply=apply
        )
        secdiv_moved = await _correct_year_copies(session, SECDIV, TAXRPT, SECDIV.okb_id, apply=apply)
        missing_fx = await _report_missing_rates(session, taxrpt_moved)
        if apply:
            await session.commit()

    print(
        f"sourcerpt_moved={len(sourcerpt_moved)} taxrpt_moved={len(taxrpt_moved)} "
        f"sourceage_year_moved={sourceage_moved} secdiv_year_moved={secdiv_moved} "
        f"missing_fx={missing_fx} applied={str(apply).lower()}"
    )
    return 0


def main() -> None:
    args = _build_parser().parse_args()
    raise SystemExit(asyncio.run(run_job(apply=args.apply)))


if __name__ == "__main__":
    main()
