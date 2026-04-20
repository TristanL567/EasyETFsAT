from __future__ import annotations

from collections.abc import AsyncIterator
from types import SimpleNamespace

import pytest
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.db.base import Base
from fondant.db.models import SOURCERPT
from fondant.jobs import fetch_missing_isins, isin_storage, refresh_existing_isins


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
async def test_fetch_missing_dry_run_only_targets_missing(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(fetch_missing_isins, "AsyncSessionFactory", sqlite_session_factory)

    async with sqlite_session_factory() as session:
        session.add(SOURCERPT(isin="IE00BMTX1Y45", stm_id=111, versions_nr=1, status_code="FIN"))
        await session.commit()

    storage_path = tmp_path / "isin_storage.csv"
    storage_path.write_text("ISIN\nIE00BMTX1Y45\nLU0380865021\n", encoding="utf-8")

    args = SimpleNamespace(
        storage=storage_path,
        isin=[],
        persist_input=False,
        limit=None,
        force=False,
        dry_run=True,
        show_isins=False,
    )

    exit_code = await fetch_missing_isins.run_job(args)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Universe size: 2" in output
    assert "Already in SOURCERPT: 1" in output
    assert "Candidate fetch count: 1" in output


@pytest.mark.asyncio
async def test_fetch_missing_persist_input_updates_storage(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage_path = tmp_path / "isin_storage.csv"
    storage_path.write_text("ISIN\n", encoding="utf-8")

    async def _fake_load_existing() -> set[str]:
        return set()

    async def _fake_ingest_many(isins: list[str]) -> list[object]:
        return []

    monkeypatch.setattr(fetch_missing_isins, "_load_existing_source_isins", _fake_load_existing)
    monkeypatch.setattr(fetch_missing_isins, "ingest_many", _fake_ingest_many)

    args = SimpleNamespace(
        storage=storage_path,
        isin=["LU0380865021"],
        persist_input=True,
        limit=None,
        force=False,
        dry_run=True,
        show_isins=False,
    )

    exit_code = await fetch_missing_isins.run_job(args)

    assert exit_code == 0
    assert isin_storage.load_storage_isins(storage_path) == ["LU0380865021"]


@pytest.mark.asyncio
async def test_refresh_existing_dry_run_filters_requested_isins(
    sqlite_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(refresh_existing_isins, "AsyncSessionFactory", sqlite_session_factory)

    async with sqlite_session_factory() as session:
        session.add(SOURCERPT(isin="IE00BMTX1Y45", stm_id=111, versions_nr=1, status_code="FIN"))
        session.add(SOURCERPT(isin="LU0380865021", stm_id=222, versions_nr=1, status_code="FIN"))
        await session.commit()

    args = SimpleNamespace(
        isin=["IE00BMTX1Y45", "LU9999999999"],
        limit=None,
        dry_run=True,
        show_isins=False,
    )

    exit_code = await refresh_existing_isins.run_job(args)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Requested ISINs not found in SOURCERPT:" in output
    assert "Candidate refresh count: 1" in output

