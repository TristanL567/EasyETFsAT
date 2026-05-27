from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.api.main import create_app
from fondant.db.base import Base
from fondant.db.models import TAXCAT, TAXDAT, TAXLIN, TAXRPT
from fondant.db.session import get_session


@pytest.fixture
async def api_client() -> httpx.AsyncClient:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as session:
        k11 = TAXLIN(
            line_code="K11",
            metric_key="ag_ertraege",
            name_de="AGErtraege",
            name_en="income",
            line_order=10,
            is_active=True,
        )
        k81 = TAXLIN(
            line_code="K81",
            metric_key="kest_total",
            name_de="KESt",
            name_en="withholding_tax",
            line_order=20,
            is_active=True,
        )
        pvm = TAXCAT(
            category_code="PVM",
            category_key="pv_mit",
            name_de="Privat mit",
            name_en="private with",
            category_order=10,
        )
        stiftung = TAXCAT(
            category_code="STF",
            category_key="stiftung",
            name_de="Stiftung",
            name_en="foundation",
            category_order=60,
        )
        session.add_all([k11, k81, pvm, stiftung])
        await session.flush()

        rpt_older = TAXRPT(
            isin="IE00BMTX1Y45",
            stm_id=111,
            versions_nr=1,
            status_code="FIN",
            report_year=2025,
            meldg_datum=date(2025, 5, 1),
            waehrung="EUR",
        )
        rpt_newer = TAXRPT(
            isin="IE00BMTX1Y45",
            stm_id=112,
            versions_nr=2,
            status_code="FIN",
            report_year=2025,
            meldg_datum=date(2025, 6, 15),
            waehrung="EUR",
        )
        rpt_tied = TAXRPT(
            isin="IE00BMTX1Y45",
            stm_id=113,
            versions_nr=3,
            status_code="COR",
            report_year=2025,
            meldg_datum=date(2025, 6, 15),
            waehrung="EUR",
        )
        rpt_null_date = TAXRPT(
            isin="IE00BMTX1Y45",
            stm_id=114,
            versions_nr=4,
            status_code=None,
            report_year=2025,
            meldg_datum=None,
            waehrung=None,
        )
        rpt_null_year = TAXRPT(
            isin="LU1681044993",
            stm_id=222,
            versions_nr=1,
            status_code="FIN",
            report_year=None,
            meldg_datum=None,
            waehrung="CHF",
        )
        session.add_all([rpt_older, rpt_newer, rpt_tied, rpt_null_date, rpt_null_year])
        await session.flush()

        session.add_all(
            [
                TAXDAT(
                    taxrpt_id=rpt_older.id,
                    okb_id=rpt_older.stm_id,
                    taxlin_id=k11.id,
                    taxcat_id=pvm.id,
                    amount=Decimal("1.2000000000"),
                    waehrung="EUR",
                ),
                TAXDAT(
                    taxrpt_id=rpt_older.id,
                    okb_id=rpt_older.stm_id,
                    taxlin_id=k81.id,
                    taxcat_id=pvm.id,
                    amount=Decimal("0.3000000000"),
                    waehrung="EUR",
                ),
                TAXDAT(
                    taxrpt_id=rpt_newer.id,
                    okb_id=rpt_newer.stm_id,
                    taxlin_id=k11.id,
                    taxcat_id=pvm.id,
                    amount=Decimal("1234.5678901234"),
                    waehrung="EUR",
                ),
                TAXDAT(
                    taxrpt_id=rpt_tied.id,
                    okb_id=rpt_tied.stm_id,
                    taxlin_id=k11.id,
                    taxcat_id=stiftung.id,
                    amount=Decimal("0.1000000001"),
                    waehrung="EUR",
                ),
                TAXDAT(
                    taxrpt_id=rpt_null_year.id,
                    okb_id=rpt_null_year.stm_id,
                    taxlin_id=k11.id,
                    taxcat_id=pvm.id,
                    amount=Decimal("0.5000000000"),
                    waehrung="CHF",
                ),
                TAXDAT(
                    taxrpt_id=rpt_null_year.id,
                    okb_id=rpt_null_year.stm_id,
                    taxlin_id=k81.id,
                    taxcat_id=pvm.id,
                    amount=Decimal("0.1000000000"),
                    waehrung="CHF",
                ),
            ]
        )
        await session.commit()

    app = create_app()

    async def _override_session() -> AsyncSession:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    yield client

    await client.aclose()
    app.dependency_overrides.clear()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


def _report_by_stm_id(payload: dict[str, Any], stm_id: int) -> dict[str, Any]:
    return next(report for report in payload["reports"] if report["stm_id"] == stm_id)


@pytest.mark.asyncio
async def test_health_contract_shape(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_get_etf_tax_returns_rows(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/etf/ie00bmtx1y45/tax", params={"year": 2025})
    assert response.status_code == 200

    payload = response.json()
    assert payload["isin"] == "IE00BMTX1Y45"
    assert payload["year"] == 2025
    assert payload["year_fallback_null_used"] is False
    assert payload["count"] == 4

    older_report = _report_by_stm_id(payload, 111)
    assert older_report == {
        "stm_id": 111,
        "versions_nr": 1,
        "status_code": "FIN",
        "waehrung": "EUR",
        "meldg_datum": "2025-05-01",
        "tax_fields": {
            "ag_ertraege": {"pv_mit": 1.2},
            "kest_total": {"pv_mit": 0.3},
        },
    }


@pytest.mark.asyncio
async def test_get_etf_tax_orders_multiple_reports_by_descending_report_date(
    api_client: httpx.AsyncClient,
) -> None:
    response = await api_client.get("/etf/IE00BMTX1Y45/tax", params={"year": 2025})
    assert response.status_code == 200

    reports = response.json()["reports"]
    assert {reports[0]["stm_id"], reports[1]["stm_id"]} == {112, 113}
    assert reports[0]["meldg_datum"] == "2025-06-15"
    assert reports[1]["meldg_datum"] == "2025-06-15"
    assert reports[2]["stm_id"] == 111
    assert reports[2]["meldg_datum"] == "2025-05-01"


@pytest.mark.asyncio
async def test_get_etf_tax_preserves_tied_and_null_date_reports(
    api_client: httpx.AsyncClient,
) -> None:
    response = await api_client.get("/etf/IE00BMTX1Y45/tax", params={"year": 2025})
    assert response.status_code == 200

    payload = response.json()
    tied_report = _report_by_stm_id(payload, 113)
    null_date_report = _report_by_stm_id(payload, 114)
    assert tied_report["meldg_datum"] == "2025-06-15"
    assert tied_report["tax_fields"] == {"ag_ertraege": {"stiftung": 0.1000000001}}
    assert null_date_report == {
        "stm_id": 114,
        "versions_nr": 4,
        "status_code": None,
        "waehrung": None,
        "meldg_datum": None,
        "tax_fields": {},
    }


@pytest.mark.asyncio
async def test_get_etf_tax_returns_404_for_no_data_isin(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/etf/AT0000000000/tax", params={"year": 2025})
    assert response.status_code == 404
    assert response.json() == {"detail": "No tax data found for ISIN/year"}


@pytest.mark.asyncio
async def test_get_etf_tax_returns_404_for_syntactically_invalid_isin(
    api_client: httpx.AsyncClient,
) -> None:
    response = await api_client.get("/etf/not-an-isin/tax", params={"year": 2025})
    assert response.status_code == 404
    assert response.json() == {"detail": "No tax data found for ISIN/year"}


@pytest.mark.asyncio
async def test_get_etf_tax_falls_back_to_null_year_rows(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/etf/LU1681044993/tax", params={"year": 2025})
    assert response.status_code == 200
    payload = response.json()
    assert payload["isin"] == "LU1681044993"
    assert payload["year"] == 2025
    assert payload["year_fallback_null_used"] is True
    assert payload["count"] == 1


@pytest.mark.asyncio
async def test_get_etf_tax_numeric_values_are_json_numbers_with_float_precision(
    api_client: httpx.AsyncClient,
) -> None:
    response = await api_client.get("/etf/IE00BMTX1Y45/tax", params={"year": 2025})
    assert response.status_code == 200

    report = _report_by_stm_id(response.json(), 112)
    amount = report["tax_fields"]["ag_ertraege"]["pv_mit"]
    assert isinstance(amount, float)
    assert amount == 1234.5678901234


@pytest.mark.asyncio
async def test_openapi_contract_exposes_current_public_routes(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/openapi.json")
    assert response.status_code == 200

    schema = response.json()
    assert schema["info"] == {"title": "EasyETFsAT API", "version": "0.1.0"}

    health_get = schema["paths"]["/health"]["get"]
    assert health_get["tags"] == ["health"]
    assert health_get["responses"]["200"]["content"]["application/json"]["schema"] == {
        "additionalProperties": {"type": "string"},
        "type": "object",
        "title": "Response Health Health Get",
    }

    tax_get = schema["paths"]["/etf/{isin}/tax"]["get"]
    assert tax_get["tags"] == ["etf"]
    assert tax_get["parameters"] == [
        {
            "name": "isin",
            "in": "path",
            "required": True,
            "schema": {"type": "string", "title": "Isin"},
        },
        {
            "name": "year",
            "in": "query",
            "required": True,
            "schema": {
                "type": "integer",
                "maximum": 3000,
                "minimum": 1900,
                "title": "Year",
            },
        },
    ]
    assert tax_get["responses"]["200"]["content"]["application/json"]["schema"] == {
        "type": "object",
        "additionalProperties": True,
        "title": "Response Get Etf Tax Etf  Isin  Tax Get",
    }
