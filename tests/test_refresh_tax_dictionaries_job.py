from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.db.base import Base
from fondant.db.models import TAXLIN
from fondant.ingestion import pipeline
from fondant.jobs import refresh_tax_dictionaries


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
async def test_refresh_tax_dictionaries_refreshes_stale_null_metadata(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(refresh_tax_dictionaries, "AsyncSessionFactory", sqlite_session_factory)
    expected_k11 = next(line for line in pipeline.LINE_DICTIONARY if line["line_code"] == "K11")

    async with sqlite_session_factory() as session:
        session.add(
            TAXLIN(
                line_code="K11",
                metric_key=expected_k11["metric_key"],
                name_de="stale",
                name_en=None,
                line_order=999,
                description=None,
                usage_note="stale",
                source_label=None,
                is_active=True,
            )
        )
        await session.commit()

    exit_code = await refresh_tax_dictionaries.run_job()

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "taxlin_rows=11" in output
    assert "metadata_complete=true" in output

    async with sqlite_session_factory() as session:
        k11 = await session.scalar(select(TAXLIN).where(TAXLIN.line_code == "K11"))
        incomplete_count = await session.scalar(
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

    assert k11 is not None
    assert k11.description == expected_k11["description"]
    assert k11.usage_note == expected_k11["usage_note"]
    assert k11.source_label == expected_k11["source_label"]
    assert incomplete_count == 0


@pytest.mark.asyncio
async def test_refresh_tax_dictionaries_is_idempotent_and_does_not_duplicate_taxlin_rows(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_tax_dictionaries, "AsyncSessionFactory", sqlite_session_factory)

    first_exit_code = await refresh_tax_dictionaries.run_job()
    second_exit_code = await refresh_tax_dictionaries.run_job()

    async with sqlite_session_factory() as session:
        taxlin_count = await session.scalar(select(func.count()).select_from(TAXLIN))
        distinct_line_codes = await session.scalar(select(func.count(func.distinct(TAXLIN.line_code))))

    assert first_exit_code == 0
    assert second_exit_code == 0
    assert taxlin_count == 11
    assert distinct_line_codes == 11


@pytest.mark.asyncio
async def test_refresh_tax_dictionaries_does_not_require_oekb_network_calls(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(refresh_tax_dictionaries, "AsyncSessionFactory", sqlite_session_factory)

    class FailingOeKBClient:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("OeKB client should not be constructed")

    monkeypatch.setattr(pipeline, "OeKBClient", FailingOeKBClient)

    exit_code = await refresh_tax_dictionaries.run_job()

    async with sqlite_session_factory() as session:
        taxlin_count = await session.scalar(select(func.count()).select_from(TAXLIN))

    assert exit_code == 0
    assert taxlin_count == 11
