from __future__ import annotations

from datetime import date

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
        k11 = TAXLIN(line_code="K11", metric_key="ag_ertraege", name_de="AGErtraege", name_en="income", line_order=10, is_active=True)
        k81 = TAXLIN(line_code="K81", metric_key="kest_total", name_de="KESt", name_en="withholding_tax", line_order=20, is_active=True)
        pvm = TAXCAT(category_code="PVM", category_key="pv_mit", name_de="Privat mit", name_en="private with", category_order=10)
        session.add_all([k11, k81, pvm])
        await session.flush()

        rpt_2025 = TAXRPT(
            isin="IE00BMTX1Y45",
            stm_id=111,
            versions_nr=1,
            status_code="FIN",
            report_year=2025,
            meldg_datum=date(2025, 5, 1),
            waehrung="EUR",
        )
        rpt_null = TAXRPT(
            isin="LU1681044993",
            stm_id=222,
            versions_nr=1,
            status_code="FIN",
            report_year=None,
            meldg_datum=None,
            waehrung="CHF",
        )
        session.add_all([rpt_2025, rpt_null])
        await session.flush()

        session.add_all(
            [
                TAXDAT(
                    taxrpt_id=rpt_2025.id,
                    okb_id=rpt_2025.stm_id,
                    taxlin_id=k11.id,
                    taxcat_id=pvm.id,
                    amount=1.2,
                    waehrung="EUR",
                ),
                TAXDAT(
                    taxrpt_id=rpt_2025.id,
                    okb_id=rpt_2025.stm_id,
                    taxlin_id=k81.id,
                    taxcat_id=pvm.id,
                    amount=0.3,
                    waehrung="EUR",
                ),
                TAXDAT(
                    taxrpt_id=rpt_null.id,
                    okb_id=rpt_null.stm_id,
                    taxlin_id=k11.id,
                    taxcat_id=pvm.id,
                    amount=0.5,
                    waehrung="CHF",
                ),
                TAXDAT(
                    taxrpt_id=rpt_null.id,
                    okb_id=rpt_null.stm_id,
                    taxlin_id=k81.id,
                    taxcat_id=pvm.id,
                    amount=0.1,
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


@pytest.mark.asyncio
async def test_get_etf_tax_returns_rows(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/etf/ie00bmtx1y45/tax", params={"year": 2025})
    assert response.status_code == 200

    payload = response.json()
    assert payload["isin"] == "IE00BMTX1Y45"
    assert payload["year"] == 2025
    assert payload["count"] == 1
    assert payload["reports"][0]["stm_id"] == 111
    assert payload["reports"][0]["tax_fields"]["ag_ertraege"]["pv_mit"] == 1.2


@pytest.mark.asyncio
async def test_get_etf_tax_returns_404_for_unknown_year(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/etf/IE00BMTX1Y45/tax", params={"year": 2024})
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_get_etf_tax_falls_back_to_null_year_rows(api_client: httpx.AsyncClient) -> None:
    response = await api_client.get("/etf/LU1681044993/tax", params={"year": 2025})
    assert response.status_code == 200
    payload = response.json()
    assert payload["year_fallback_null_used"] is True
    assert payload["count"] == 1
