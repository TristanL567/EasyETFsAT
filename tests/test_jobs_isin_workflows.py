from __future__ import annotations

import uuid
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
from fondant.ingestion.pipeline import IngestionResult
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
async def test_fetch_missing_summary_reports_all_success(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    storage_path = tmp_path / "isin_storage.csv"
    storage_path.write_text("ISIN\nIE00BMTX1Y45\nLU0380865021\n", encoding="utf-8")

    async def _fake_load_existing() -> set[str]:
        return set()

    async def _fake_ingest_many(isins: list[str]) -> list[IngestionResult]:
        return [
            _ingestion_result(isin=isin, status="SUCCESS", records_seen=1, records_written=1)
            for isin in isins
        ]

    monkeypatch.setattr(fetch_missing_isins, "_load_existing_source_isins", _fake_load_existing)
    monkeypatch.setattr(fetch_missing_isins, "ingest_many", _fake_ingest_many)

    args = SimpleNamespace(
        storage=storage_path,
        isin=[],
        persist_input=False,
        limit=None,
        force=False,
        dry_run=False,
        show_isins=False,
    )

    exit_code = await fetch_missing_isins.run_job(args)

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Fetched ISINs: 2" in output
    assert "Success: 2 | Failed: 0" in output
    assert "Batch outcome: all_success" in output
    assert "Failure category: none" in output
    assert "Failed ISINs:" not in output


@pytest.mark.asyncio
async def test_refresh_existing_summary_reports_mixed_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def _fake_load_existing() -> list[str]:
        return ["IE00BMTX1Y45", "LU0380865021"]

    async def _fake_ingest_many(isins: list[str]) -> list[IngestionResult]:
        assert isins == ["IE00BMTX1Y45", "LU0380865021"]
        return [
            _ingestion_result(isin="IE00BMTX1Y45", status="SUCCESS", records_seen=1, records_written=0),
            _ingestion_result(
                isin="LU0380865021",
                status="FAILED",
                records_seen=0,
                records_written=0,
                message="upstream timeout",
            ),
        ]

    monkeypatch.setattr(refresh_existing_isins, "_load_existing_source_isins", _fake_load_existing)
    monkeypatch.setattr(refresh_existing_isins, "ingest_many", _fake_ingest_many)

    args = SimpleNamespace(
        isin=[],
        limit=None,
        dry_run=False,
        show_isins=False,
    )

    exit_code = await refresh_existing_isins.run_job(args)

    output = capsys.readouterr().out
    assert exit_code == 2
    assert "Refreshed ISINs: 2" in output
    assert "Success: 1 | Failed: 1" in output
    assert "Batch outcome: mixed_failure" in output
    assert "Failure category: isolated_isin_failures" in output
    assert "Unchanged ISINs: 1" in output
    assert "- LU0380865021: upstream timeout" in output


@pytest.mark.asyncio
async def test_fetch_missing_summary_reports_all_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    storage_path = tmp_path / "isin_storage.csv"
    storage_path.write_text("ISIN\nIE00BMTX1Y45\nLU0380865021\n", encoding="utf-8")

    async def _fake_load_existing() -> set[str]:
        return set()

    async def _fake_ingest_many(isins: list[str]) -> list[IngestionResult]:
        return [
            _ingestion_result(
                isin=isin,
                status="FAILED",
                records_seen=0,
                records_written=0,
                message="service unavailable",
            )
            for isin in isins
        ]

    monkeypatch.setattr(fetch_missing_isins, "_load_existing_source_isins", _fake_load_existing)
    monkeypatch.setattr(fetch_missing_isins, "ingest_many", _fake_ingest_many)

    args = SimpleNamespace(
        storage=storage_path,
        isin=[],
        persist_input=False,
        limit=None,
        force=False,
        dry_run=False,
        show_isins=False,
    )

    exit_code = await fetch_missing_isins.run_job(args)

    output = capsys.readouterr().out
    assert exit_code == 2
    assert "Fetched ISINs: 2" in output
    assert "Success: 0 | Failed: 2" in output
    assert "Batch outcome: all_failure" in output
    assert "Failure category: systemic_batch_failure" in output
    assert output.count(": service unavailable") == 2


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


def _ingestion_result(
    *,
    isin: str,
    status: str,
    records_seen: int,
    records_written: int,
    message: str | None = None,
) -> IngestionResult:
    return IngestionResult(
        isin=isin,
        status=status,
        records_seen=records_seen,
        records_written=records_written,
        run_id=uuid.uuid4(),
        message=message,
    )
