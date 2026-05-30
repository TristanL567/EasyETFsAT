from __future__ import annotations

import asyncio

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.db.models import TAXLIN
from fondant.db.session import AsyncSessionFactory
from fondant.ingestion.pipeline import _ensure_tax_dictionaries


async def _load_taxlin_summary(session: AsyncSession) -> tuple[int, bool]:
    taxlin_rows = await session.scalar(select(func.count()).select_from(TAXLIN))
    incomplete_rows = await session.scalar(
        select(func.count())
        .select_from(TAXLIN)
        .where(
            TAXLIN.is_active.is_(True),
            or_(
                TAXLIN.description.is_(None)
                | (func.trim(TAXLIN.description) == ""),
                TAXLIN.usage_note.is_(None)
                | (func.trim(TAXLIN.usage_note) == ""),
                TAXLIN.source_label.is_(None)
                | (func.trim(TAXLIN.source_label) == ""),
            ),
        )
    )
    return int(taxlin_rows or 0), (incomplete_rows or 0) == 0


async def run_job() -> int:
    async with AsyncSessionFactory() as session:
        await _ensure_tax_dictionaries(session=session)
        await session.commit()
        taxlin_rows, metadata_complete = await _load_taxlin_summary(session)

    print(f"taxlin_rows={taxlin_rows} metadata_complete={str(metadata_complete).lower()}")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(run_job()))


if __name__ == "__main__":
    main()
