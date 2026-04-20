from __future__ import annotations

from collections.abc import AsyncIterator
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.db.base import Base
from fondant.db.models import (
    IMPLOG,
    SECMDA,
    SOURCEAGE,
    SOURCERAW,
    SOURCERPT,
    TAXADJ,
    TAXCAT,
    TAXDAT,
    TAXLIN,
    TAXRPT,
)
from fondant.ingestion import pipeline
from fondant.oekb.models import OeKBReportDetailResponse, OeKBReportListItem


class FakeOeKBClient:
    def __init__(self, version: int = 1, age_value: str = "1.2500") -> None:
        self.version = version
        self.age_value = age_value

    async def get_report_list(self, isin: str) -> list[OeKBReportListItem]:
        return [
            OeKBReportListItem(
                stmId=1001,
                isin=isin,
                statusCode="FIN",
                versionsNr=self.version,
                isinBez=f"{isin} Fund",
                waehrung="EUR",
                meldeDatum="28.07.2024",
                eintragezeit="2025-09-30T09:16:24.563",
                gjBeginn="2024-04-01T00:00:00.000",
                gjEnde="2025-03-31T00:00:00.000",
                jahresmeldung="JA",
                ausschuettungsmeldung="NEIN",
                selbstnachweis="NEIN",
            ),
            OeKBReportListItem(
                stmId=1002,
                isin=isin,
                statusCode="DRAFT",
                versionsNr=self.version,
                meldeDatum="29.07.2025",
                eintragezeit="2025-09-30T09:16:24.563",
                jahresmeldung="JA",
                ausschuettungsmeldung="NEIN",
                selbstnachweis="NEIN",
            ),
        ]

    async def get_report_detail(self, stm_id: int) -> OeKBReportDetailResponse:
        payload = {
            "stmId": stm_id,
            "statusCode": "FIN",
            "versionsNr": self.version,
            "waehrung": "EUR",
            "werte": [
                {"steuerName": "StB_E1KV_AGErtraege", "pvMitOption4": self.age_value},
                {"steuerName": "StB_KESt", "pvMitOption4": "0.3300"},
            ],
        }
        return OeKBReportDetailResponse(
            stmId=stm_id,
            statusCode="FIN",
            versionsNr=self.version,
            waehrung="EUR",
            payload=payload,
        )


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
async def test_ingest_isin_is_idempotent_and_logs_runs(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "AsyncSessionFactory", sqlite_session_factory)

    isin = "IE00BMTX1Y45"
    first = await pipeline.ingest_isin(isin, client=FakeOeKBClient(version=1))
    second = await pipeline.ingest_isin(isin, client=FakeOeKBClient(version=1))

    assert first.status == "SUCCESS"
    assert first.records_seen == 1
    assert first.records_written == 1
    assert second.status == "SUCCESS"
    assert second.records_seen == 1
    assert second.records_written == 0

    async with sqlite_session_factory() as session:
        secmda_count = await session.scalar(select(func.count()).select_from(SECMDA))
        sourcerpt_count = await session.scalar(select(func.count()).select_from(SOURCERPT))
        sourceage_count = await session.scalar(select(func.count()).select_from(SOURCEAGE))
        sourceraw_count = await session.scalar(select(func.count()).select_from(SOURCERAW))
        taxrpt_count = await session.scalar(select(func.count()).select_from(TAXRPT))
        taxdat_count = await session.scalar(select(func.count()).select_from(TAXDAT))
        taxadj_count = await session.scalar(select(func.count()).select_from(TAXADJ))
        taxlin_count = await session.scalar(select(func.count()).select_from(TAXLIN))
        taxcat_count = await session.scalar(select(func.count()).select_from(TAXCAT))
        implog_count = await session.scalar(select(func.count()).select_from(IMPLOG))
        sec_row = await session.scalar(select(SECMDA).where(SECMDA.isin == isin))
        sourcerpt_row = await session.scalar(select(SOURCERPT).where(SOURCERPT.isin == isin, SOURCERPT.stm_id == 1001))

    assert secmda_count == 1
    assert sec_row is not None
    assert sec_row.name == f"{isin} Fund"
    assert sourcerpt_count == 2
    assert sourceage_count == 1
    assert sourceraw_count == 1
    assert taxrpt_count == 1
    assert taxdat_count == 2
    assert taxadj_count == 0
    assert taxlin_count == 11
    assert taxcat_count == 6
    assert implog_count == 2
    assert sourcerpt_row is not None
    assert sourcerpt_row.report_year == 2025
    assert sourcerpt_row.meldg_datum.isoformat() == "2024-07-28"
    assert sourcerpt_row.gj_beginn.isoformat() == "2024-04-01"
    assert sourcerpt_row.gj_ende.isoformat() == "2025-03-31"
    assert sourcerpt_row.jahresmeldung is True


@pytest.mark.asyncio
async def test_ingest_isin_updates_same_version_when_payload_changes(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "AsyncSessionFactory", sqlite_session_factory)

    isin = "LU0380865021"
    first = await pipeline.ingest_isin(isin, client=FakeOeKBClient(version=2, age_value="1.2500"))
    second = await pipeline.ingest_isin(isin, client=FakeOeKBClient(version=2, age_value="2.5000"))

    assert first.status == "SUCCESS"
    assert first.records_written == 1
    assert second.status == "SUCCESS"
    assert second.records_written == 1

    async with sqlite_session_factory() as session:
        sourceage_row = await session.scalar(select(SOURCEAGE).where(SOURCEAGE.isin == isin, SOURCEAGE.stm_id == 1001))
        k11 = await session.scalar(select(TAXLIN).where(TAXLIN.line_code == "K11"))
        pvm = await session.scalar(select(TAXCAT).where(TAXCAT.category_code == "PVM"))
        assert sourceage_row is not None
        assert k11 is not None
        assert pvm is not None

        taxdat_row = await session.scalar(
            select(TAXDAT).where(
                TAXDAT.taxlin_id == k11.id,
                TAXDAT.taxcat_id == pvm.id,
            )
        )

    assert sourceage_row.ag_ertraege_pv_mit == Decimal("2.5000")
    assert taxdat_row is not None
    assert taxdat_row.amount == Decimal("2.5000")
