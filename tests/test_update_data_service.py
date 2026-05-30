from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest

from fondant import update_data
from fondant.api.main import create_app


@dataclass(frozen=True)
class FakeIngestionResult:
    isin: str
    status: str
    records_seen: int
    records_written: int
    message: str | None = None


@pytest.mark.asyncio
async def test_update_single_isin_returns_success_when_ingestion_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_ingest_isin(isin: str) -> FakeIngestionResult:
        calls.append(isin)
        return FakeIngestionResult(
            isin=isin,
            status="SUCCESS",
            records_seen=3,
            records_written=2,
            message="Processed 3 FIN reports; wrote 2; skipped 1 unchanged/older reports.",
        )

    monkeypatch.setattr(update_data, "ingest_isin", fake_ingest_isin)

    result = await update_data.update_single_isin("IE00BMTX1Y45")

    assert calls == ["IE00BMTX1Y45"]
    assert result == update_data.UpdateDataResult(
        isin="IE00BMTX1Y45",
        status="success",
        records_seen=3,
        records_written=2,
        message="Processed 3 FIN reports; wrote 2; skipped 1 unchanged/older reports.",
        error=None,
    )


@pytest.mark.asyncio
async def test_update_single_isin_returns_failed_when_ingestion_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_ingest_isin(isin: str) -> FakeIngestionResult:
        raise RuntimeError(f"OeKB unavailable for {isin}")

    monkeypatch.setattr(update_data, "ingest_isin", fake_ingest_isin)

    result = await update_data.update_single_isin("IE00BMTX1Y45")

    assert result == update_data.UpdateDataResult(
        isin="IE00BMTX1Y45",
        status="failed",
        records_seen=0,
        records_written=0,
        message="Ingestion failed for IE00BMTX1Y45: OeKB unavailable for IE00BMTX1Y45",
        error="OeKB unavailable for IE00BMTX1Y45",
    )


@pytest.mark.asyncio
async def test_update_data_route_does_not_call_update_service_yet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_if_called(isin: str) -> update_data.UpdateDataResult:
        raise AssertionError(f"update service should not be called for {isin}")

    monkeypatch.setattr(update_data, "update_single_isin", fail_if_called)
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        login_response = await client.post(
            "/login",
            data={"username": "admin", "password": "password"},
        )
        assert login_response.status_code == 303

        response = await client.post("/app/update-data", data={"isins": "IE00BMTX1Y45"})

    assert response.status_code == 200
    assert "<h2>Normalized ISIN preview</h2>" in response.text
    assert "<li>IE00BMTX1Y45</li>" in response.text
