from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from fondant.api.main import create_app
from fondant.api.routes import web as web_routes
from fondant.business_query import BusinessQueryInput, BusinessQueryResult, BusinessQueryResultRow
from fondant.db.base import Base
from fondant.db.models import INGJOB
from fondant.db.session import get_session
from fondant.search import FundSearchResult
from fondant.tax_registry import TAX_LINES


@pytest.fixture
async def web_client() -> httpx.AsyncClient:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    yield client
    await client.aclose()


@pytest.fixture
async def update_data_job_client() -> AsyncIterator[
    tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]]
]:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    app = create_app()

    async def _override_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    yield client, session_factory

    await client.aclose()
    app.dependency_overrides.clear()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
async def test_root_renders_template_html(web_client: httpx.AsyncClient) -> None:
    response = await web_client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "<title>EasyETFsAT</title>" in response.text
    assert 'href="http://test/static/css/site.css"' in response.text
    assert "<h1" in response.text
    assert "EasyETFsAT" in response.text
    normalized_html = " ".join(response.text.split())
    assert (
        "Austrian ETF tax reporting and inspection using OeKB public reports "
        "and ECB FX data."
    ) in normalized_html
    assert 'href="/login"' in response.text


@pytest.mark.asyncio
async def test_root_does_not_expose_internal_operations(web_client: httpx.AsyncClient) -> None:
    response = await web_client.get("/")

    assert response.status_code == 200
    public_html = response.text.lower()
    internal_terms = [
        "diagnostic",
        "ingestion",
        "raw sql",
        "migration",
        "database url",
        "environment",
        "stack trace",
    ]
    assert all(term not in public_html for term in internal_terms)


@pytest.mark.asyncio
async def test_static_css_is_served(web_client: httpx.AsyncClient) -> None:
    response = await web_client.get("/static/css/site.css")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/css")
    assert ".page-shell" in response.text
    assert "@media" in response.text


def test_app_startup_registers_static_web_and_api_routes_together() -> None:
    app = create_app()

    route_paths = {getattr(route, "path", "") for route in app.routes}

    assert "/static" in route_paths
    assert "/" in route_paths
    assert "/login" in route_paths
    assert "/app" in route_paths
    assert "/app/business-query" in route_paths
    assert "/app/search" in route_paths
    assert "/app/update-data" in route_paths
    assert "/app/documentation" in route_paths
    assert "/health" in route_paths
    assert "/etf/{isin}/tax" in route_paths


@pytest.mark.asyncio
async def test_login_form_renders(web_client: httpx.AsyncClient) -> None:
    response = await web_client.get("/login")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "<title>Log in - EasyETFsAT</title>" in response.text
    assert '<form class="login-form" method="post" action="/login">' in response.text
    assert 'name="username"' in response.text
    assert 'name="password"' in response.text


@pytest.mark.asyncio
async def test_login_sets_http_only_signed_session_cookie(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/app"
    set_cookie = response.headers["set-cookie"]
    assert "easyetfsat_session=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "SameSite=lax" in set_cookie

    cookie_value = response.cookies["easyetfsat_session"]
    assert "." in cookie_value
    payload, signature = cookie_value.rsplit(".", 1)
    assert payload
    assert len(signature) == 64


@pytest.mark.asyncio
async def test_login_fails_generically_without_session_cookie(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "wrong"},
    )

    assert response.status_code == 401
    assert "The username or password is incorrect." in response.text
    assert "unknown username" not in response.text.lower()
    assert "wrong password" not in response.text.lower()
    assert "easyetfsat_session=" not in response.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_app_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    for path in [
        "/app",
        "/app/business-query",
        "/app/search",
        "/app/update-data",
        "/app/documentation",
    ]:
        response = await web_client.get(path)

        assert response.status_code == 303
        assert response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_app_renders_for_authenticated_users(web_client: httpx.AsyncClient) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app")

    assert response.status_code == 200
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert 'aria-label="Primary sections"' in response.text
    assert response.text.count('class="portal-nav-link') == 4
    assert ">BusinessQuery<" in response.text
    assert ">Search<" in response.text
    assert ">Update Data<" in response.text
    assert ">Documentation<" in response.text
    assert 'href="/app/business-query"' in response.text
    assert 'href="/app/search"' in response.text
    assert 'href="/app/update-data"' in response.text
    assert 'href="/app/documentation"' in response.text
    normalized_html = " ".join(response.text.split())
    assert (
        ">BusinessQuery</a> <a class=\"portal-nav-link\" href=\"/app/search\" >Search</a> "
        "<a class=\"portal-nav-link\" href=\"/app/update-data\" >Update Data</a> "
        "<a class=\"portal-nav-link\" href=\"/app/documentation\" >Documentation</a>"
    ) in normalized_html
    assert "Signed in as <strong>admin</strong>" in response.text
    assert "<h1 id=\"app-title\">BusinessQuery</h1>" in response.text
    assert "Submit structured inputs to calculate Austrian ETF tax values." in response.text
    assert '<form method="post" action="/logout">' in response.text


@pytest.mark.asyncio
async def test_business_query_form_renders_for_authenticated_users(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    for path in ["/app", "/app/business-query"]:
        response = await web_client.get(path)

        assert response.status_code == 200
        assert (
            '<form class="business-query-form" method="post" '
            'action="/app/business-query" aria-label="BusinessQuery setup" novalidate>'
        ) in response.text
        assert '<label for="query-name">Custom query name</label>' in response.text
        assert 'name="query_name"' in response.text
        assert '<label for="isins">ISIN input area</label>' in response.text
        assert 'name="isins"' in response.text
        assert '<label for="legal-entity-type">Legal entity type</label>' in response.text
        assert '<label for="amount">Amount</label>' in response.text
        assert 'name="amount"' in response.text
        assert "Submit structured inputs to calculate Austrian ETF tax values." in response.text
        assert 'type="submit"' in response.text

        normalized_html = " ".join(response.text.split())
        assert (
            '<option value="natural person" selected>natural person</option> '
            '<option value="business">business</option> '
            '<option value="Stiftung">Stiftung</option>'
        ) in normalized_html
        assert normalized_html.count("<option") == 3


@pytest.mark.asyncio
async def test_business_query_get_prefills_selected_isin_from_search_link(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/business-query", params={"isins": "ie00bmtx1y45"})

    assert response.status_code == 200
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert ">IE00BMTX1Y45</textarea>" in response.text


@pytest.mark.asyncio
async def test_business_query_post_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "Monthly review",
            "isins": "ie00bmtx1y45",
            "legal_entity_type": "business",
            "amount": "1000",
        },
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_business_query_export_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post(
        "/app/business-query/export",
        data={
            "query_name": "Monthly review",
            "isins": "ie00bmtx1y45",
            "legal_entity_type": "business",
            "amount": "1000",
        },
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_business_query_valid_post_calls_service_and_renders_result_rows(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_calls = []

    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        service_calls.append((session, query))
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Monthly review",
                    isin="IE00BMTX1Y45",
                    tax_year=2025,
                    oekb_report_id=1001,
                    fund_currency="EUR",
                    report_date=date(2025, 6, 15),
                    fx_rate=Decimal("1.0000000000"),
                    legal_entity_category="BVM",
                    tax_field_code="K40",
                    tax_field_label="Taxable income",
                    base_eur_value=Decimal("10.0000000000"),
                    amount_multiplier=Decimal("1000.50"),
                    calculated_eur_value=Decimal("10005.000000000000"),
                ),
            ),
        )

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "  Monthly review  ",
            "isins": "ie00bmtx1y45\n lu1681044993 ",
            "legal_entity_type": "business",
            "amount": "1000.50",
        },
    )

    assert response.status_code == 200
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert 'id="query-name"' in response.text
    assert 'value="Monthly review"' in response.text
    assert ">IE00BMTX1Y45\nLU1681044993</textarea>" in response.text
    assert '<option value="business" selected>business</option>' in response.text
    assert 'value="1000.50"' in response.text
    assert "<h2>Query results</h2>" in response.text
    assert "<dd>IE00BMTX1Y45, LU1681044993</dd>" in response.text
    assert "<th scope=\"col\">ISIN</th>" in response.text
    assert "<th scope=\"col\">Tax year</th>" in response.text
    assert "<th scope=\"col\">Tax field</th>" in response.text
    assert "<th scope=\"col\">Legal entity category</th>" in response.text
    assert "<th scope=\"col\">Base value</th>" in response.text
    assert "<th scope=\"col\">Multiplier</th>" in response.text
    assert "<th scope=\"col\">Calculated value</th>" in response.text
    assert "<td>IE00BMTX1Y45</td>" in response.text
    assert "<td>2025</td>" in response.text
    assert "<td>K40 - Taxable income</td>" in response.text
    assert "<td>BVM</td>" in response.text
    assert "<td>10.0000000000</td>" in response.text
    assert "<td>1000.50</td>" in response.text
    assert "<td>10005.000000000000</td>" in response.text
    assert 'formaction="/app/business-query/export"' in response.text
    assert "Export CSV" in response.text
    assert "field-error" not in response.text
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.query_name == "Monthly review"
    assert query.isins == ("IE00BMTX1Y45", "LU1681044993")
    assert query.legal_entity_type == "business"
    assert query.amount_multiplier == Decimal("1000.50")


@pytest.mark.asyncio
async def test_business_query_valid_post_with_no_rows_renders_empty_state(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_calls = []

    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        service_calls.append((session, query))
        return BusinessQueryResult(query=query, rows=())

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "No rows",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert "No tax rows matched the submitted ISINs." in response.text
    assert 'value="No rows"' in response.text
    assert ">IE00BMTX1Y45</textarea>" in response.text
    assert len(service_calls) == 1


@pytest.mark.asyncio
async def test_business_query_valid_export_returns_csv_with_expected_rows(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_calls = []

    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        service_calls.append((session, query))
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Monthly review",
                    isin="IE00BMTX1Y45",
                    tax_year=2025,
                    oekb_report_id=1001,
                    fund_currency="EUR",
                    report_date=date(2025, 6, 15),
                    fx_rate=Decimal("1.0000000000"),
                    legal_entity_category="BVM",
                    tax_field_code="K40",
                    tax_field_label="Taxable income",
                    base_eur_value=Decimal("10.0000000000"),
                    amount_multiplier=Decimal("1000.50"),
                    calculated_eur_value=Decimal("10005.000000000000"),
                ),
            ),
        )

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/export",
        data={
            "query_name": "  Monthly review  ",
            "isins": "ie00bmtx1y45\n lu1681044993 ",
            "legal_entity_type": "business",
            "amount": "1000.50",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.text == (
        "query_name,isin,tax_year,tax_field_code,tax_field_label,"
        "legal_entity_category,base_eur_value,amount_multiplier,calculated_eur_value\n"
        "Monthly review,IE00BMTX1Y45,2025,K40,Taxable income,BVM,"
        "10.0000000000,1000.50,10005.000000000000\n"
    )
    assert 'filename="business-query.csv"' in response.headers["content-disposition"]
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.query_name == "Monthly review"
    assert query.isins == ("IE00BMTX1Y45", "LU1681044993")
    assert query.legal_entity_type == "business"
    assert query.amount_multiplier == Decimal("1000.50")


@pytest.mark.asyncio
async def test_business_query_empty_export_returns_csv_headers_only(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service_calls = []

    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        service_calls.append((session, query))
        return BusinessQueryResult(query=query, rows=())

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/export",
        data={
            "query_name": "No rows",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.text == (
        "query_name,isin,tax_year,tax_field_code,tax_field_label,"
        "legal_entity_category,base_eur_value,amount_multiplier,calculated_eur_value\n"
    )
    assert len(service_calls) == 1


@pytest.mark.asyncio
async def test_business_query_invalid_post_preserves_values_and_shows_errors(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_if_called(session: object, query: BusinessQueryInput) -> BusinessQueryResult:
        raise AssertionError("invalid BusinessQuery POST must not call the service")

    monkeypatch.setattr(web_routes, "execute_business_query", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": " ",
            "isins": "not-an-isin",
            "legal_entity_type": "foundation",
            "amount": "-1",
        },
    )

    assert response.status_code == 200
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert ">not-an-isin</textarea>" in response.text
    assert "Enter a custom query name." in response.text
    assert "Enter ISIN-like values such as IE00BMTX1Y45." in response.text
    assert "Choose a supported legal entity type." in response.text
    assert "Enter a positive amount." in response.text
    assert "<h2>Query results</h2>" not in response.text


@pytest.mark.asyncio
async def test_business_query_invalid_export_preserves_values_and_shows_errors(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_if_called(session: object, query: BusinessQueryInput) -> BusinessQueryResult:
        raise AssertionError("invalid BusinessQuery export must not call the service")

    monkeypatch.setattr(web_routes, "execute_business_query", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/export",
        data={
            "query_name": " ",
            "isins": "not-an-isin",
            "legal_entity_type": "foundation",
            "amount": "-1",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert ">not-an-isin</textarea>" in response.text
    assert "Enter a custom query name." in response.text
    assert "Enter ISIN-like values such as IE00BMTX1Y45." in response.text
    assert "Choose a supported legal entity type." in response.text
    assert "Enter a positive amount." in response.text
    assert "<h2>Query results</h2>" not in response.text


@pytest.mark.asyncio
async def test_search_form_renders_for_authenticated_users(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_has_available_fund_data(session: object) -> bool:
        return True

    async def fail_if_called(session: object, query: str) -> tuple[FundSearchResult, ...]:
        raise AssertionError("initial search page must not search without a query")

    monkeypatch.setattr(web_routes, "has_available_fund_data", fake_has_available_fund_data)
    monkeypatch.setattr(web_routes, "search_available_funds", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/search")

    assert response.status_code == 200
    assert "<title>Search - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Search</h1>' in response.text
    assert "Discover available fund tax data by ISIN or security name." in response.text
    assert '<form class="search-form" method="get" action="/app/search"' in response.text
    assert 'name="q"' in response.text
    assert "Enter an ISIN or fund/security name to search available records." in response.text


@pytest.mark.asyncio
async def test_authenticated_search_with_mocked_data_renders_matching_rows(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    search_calls = []

    async def fake_has_available_fund_data(session: object) -> bool:
        return True

    async def fake_search_available_funds(
        session: object,
        query: str,
    ) -> tuple[FundSearchResult, ...]:
        search_calls.append((session, query))
        return (
            FundSearchResult(
                isin="IE00BMTX1Y45",
                name="Vanguard Example UCITS ETF",
                currency="EUR",
                available_tax_years=(2024, 2025),
                report_count=3,
            ),
        )

    async def fail_ingestion(*args: object, **kwargs: object) -> object:
        raise AssertionError("search must not trigger ingestion")

    from fondant.ingestion import pipeline

    monkeypatch.setattr(web_routes, "has_available_fund_data", fake_has_available_fund_data)
    monkeypatch.setattr(web_routes, "search_available_funds", fake_search_available_funds)
    monkeypatch.setattr(pipeline, "ingest_isin", fail_ingestion)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/search", params={"q": "vanguard"})

    assert response.status_code == 200
    assert 'value="vanguard"' in response.text
    assert "<th scope=\"col\">ISIN</th>" in response.text
    assert "<th scope=\"col\">Fund/security name</th>" in response.text
    assert "<th scope=\"col\">Currency</th>" in response.text
    assert "<th scope=\"col\">Available tax years</th>" in response.text
    assert "<th scope=\"col\">Report count</th>" in response.text
    assert "<td>IE00BMTX1Y45</td>" in response.text
    assert "<td>Vanguard Example UCITS ETF</td>" in response.text
    assert "<td>EUR</td>" in response.text
    assert "<td>2024, 2025</td>" in response.text
    assert "<td>3</td>" in response.text
    assert 'href="/app/business-query?isins=IE00BMTX1Y45"' in response.text
    assert len(search_calls) == 1
    assert search_calls[0][1] == "vanguard"


@pytest.mark.asyncio
async def test_authenticated_search_renders_no_results_state(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_has_available_fund_data(session: object) -> bool:
        return True

    async def fake_search_available_funds(
        session: object,
        query: str,
    ) -> tuple[FundSearchResult, ...]:
        return ()

    monkeypatch.setattr(web_routes, "has_available_fund_data", fake_has_available_fund_data)
    monkeypatch.setattr(web_routes, "search_available_funds", fake_search_available_funds)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/search", params={"q": "missing"})

    assert response.status_code == 200
    assert "No matching fund data found." in response.text
    assert 'value="missing"' in response.text


@pytest.mark.asyncio
async def test_search_renders_empty_database_state_without_searching(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_has_available_fund_data(session: object) -> bool:
        return False

    async def fail_if_called(session: object, query: str) -> tuple[FundSearchResult, ...]:
        raise AssertionError("empty database state must not run a search")

    monkeypatch.setattr(web_routes, "has_available_fund_data", fake_has_available_fund_data)
    monkeypatch.setattr(web_routes, "search_available_funds", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/search", params={"q": "IE00BMTX1Y45"})

    assert response.status_code == 200
    assert "No fund data is available in the database yet." in response.text


@pytest.mark.asyncio
async def test_update_data_page_renders_authenticated_input_form(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, _session_factory = update_data_job_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/update-data")

    assert response.status_code == 200
    assert "<title>Update Data - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Update Data</h1>' in response.text
    assert "Prepare future authenticated fund data refresh workflows." in response.text
    assert (
        '<form class="update-data-form" method="post" action="/app/update-data" '
        'aria-label="Update Data ISIN entry" novalidate>'
    ) in response.text
    assert '<label for="update-isins">ISIN input area</label>' in response.text
    assert 'id="update-isins"' in response.text
    assert 'name="isins"' in response.text
    assert '<textarea' in response.text
    assert "disabled" not in response.text
    assert "Update ISIN" in response.text
    assert '<button class="primary-action" type="submit">' in response.text
    assert "New ISINs will eventually fetch OeKB data" in response.text
    assert "Existing ISINs will eventually check OeKB for newer data" in response.text
    assert "This version validates submitted ISINs only. It does not run ingestion yet." in response.text
    assert "<h2>Recent update jobs</h2>" in response.text
    assert "No update jobs have been queued yet." in response.text
    assert '<form class="business-query-form"' not in response.text
    assert '<form class="search-form"' not in response.text


@pytest.mark.asyncio
async def test_update_data_page_renders_recent_job_history(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = update_data_job_client
    async with session_factory() as session:
        session.add_all(
            [
                INGJOB(
                    isin="IE00BMTX1Y45",
                    requested_user="first-user",
                    status="success",
                    message="Finished.",
                    created_at=datetime(2026, 5, 1, 12, 0, 0),
                    started_at=datetime(2026, 5, 1, 12, 1, 0),
                    finished_at=datetime(2026, 5, 1, 12, 2, 0),
                ),
                INGJOB(
                    isin="LU1681044993",
                    requested_user="second-user",
                    status="failed",
                    message="Failed.",
                    error_detail="Remote report was unavailable.",
                    created_at=datetime(2026, 5, 2, 12, 0, 0),
                    started_at=datetime(2026, 5, 2, 12, 1, 0),
                    finished_at=datetime(2026, 5, 2, 12, 2, 0),
                ),
            ]
        )
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/update-data")

    assert response.status_code == 200
    assert "<h2>Recent update jobs</h2>" in response.text
    assert "No update jobs have been queued yet." not in response.text
    assert response.text.index("<td>LU1681044993</td>") < response.text.index(
        "<td>IE00BMTX1Y45</td>"
    )
    assert '<span class="status-pill status-failed">failed</span>' in response.text
    assert '<span class="status-pill status-success">success</span>' in response.text
    assert "<td>second-user</td>" in response.text
    assert "<td>2026-05-02 12:00:00</td>" in response.text
    assert "<td>2026-05-02 12:01:00</td>" in response.text
    assert "<td>2026-05-02 12:02:00</td>" in response.text
    assert "<td>Failed.</td>" in response.text
    assert "<td>Remote report was unavailable.</td>" in response.text


def test_update_data_status_styles_cover_all_job_statuses() -> None:
    stylesheet = Path("fondant/api/static/css/site.css").read_text()

    for status in ("queued", "running", "success", "failed", "skipped", "cancelled"):
        assert f".status-{status}" in stylesheet


@pytest.mark.asyncio
async def test_update_data_post_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post("/app/update-data", data={"isins": "IE00BMTX1Y45"})

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_update_data_blank_post_shows_validation_error(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, _session_factory = update_data_job_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post("/app/update-data", data={"isins": " \n\t "})

    assert response.status_code == 200
    assert '<h1 id="app-title">Update Data</h1>' in response.text
    assert "Enter at least one ISIN." in response.text
    assert "Normalized ISIN preview" not in response.text


@pytest.mark.asyncio
async def test_update_data_malformed_post_preserves_input_and_shows_error(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, _session_factory = update_data_job_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post("/app/update-data", data={"isins": "not-an-isin"})

    assert response.status_code == 200
    assert "Enter valid ISINs. Invalid values: NOT-AN-ISIN." in response.text
    assert "not-an-isin" in response.text
    assert "Normalized ISIN preview" not in response.text


@pytest.mark.asyncio
async def test_update_data_post_rejects_invalid_isin_checksum(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, _session_factory = update_data_job_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post("/app/update-data", data={"isins": "IE00BMTX1Y44"})

    assert response.status_code == 200
    assert "Enter valid ISINs. Invalid values: IE00BMTX1Y44." in response.text
    assert "Normalized ISIN preview" not in response.text


@pytest.mark.asyncio
async def test_update_data_invalid_post_creates_no_jobs(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = update_data_job_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post("/app/update-data", data={"isins": "IE00BMTX1Y44"})

    assert response.status_code == 200
    assert "Enter valid ISINs. Invalid values: IE00BMTX1Y44." in response.text
    assert "Update job status" not in response.text

    async with session_factory() as session:
        job_count = len((await session.scalars(select(INGJOB))).all())

    assert job_count == 0


@pytest.mark.asyncio
async def test_update_data_valid_post_creates_queued_jobs_and_shows_status(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = update_data_job_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/update-data",
        data={"isins": "ie00bmtx1y45, LU1681044993; IE00BMTX1Y45\tUS0378331005"},
    )

    assert response.status_code == 200
    assert "<h2>Update job status</h2>" in response.text
    assert "<td>IE00BMTX1Y45</td>" in response.text
    assert "<td>LU1681044993</td>" in response.text
    assert "<td>US0378331005</td>" in response.text
    assert response.text.index("<td>IE00BMTX1Y45</td>") < response.text.index(
        "<td>LU1681044993</td>"
    )
    assert response.text.count("<td>IE00BMTX1Y45</td>") == 2
    assert response.text.count('<span class="status-pill status-queued">queued</span>') == 6
    assert response.text.count("Queued for update.") >= 3
    assert "<h2>Recent update jobs</h2>" in response.text
    assert response.text.count("<td>admin</td>") == 3
    assert "field-error" not in response.text

    async with session_factory() as session:
        jobs = (
            await session.scalars(select(INGJOB).order_by(INGJOB.id))
        ).all()

    assert [job.isin for job in jobs] == ["IE00BMTX1Y45", "LU1681044993", "US0378331005"]
    assert [job.requested_user for job in jobs] == ["admin", "admin", "admin"]
    assert [job.status for job in jobs] == ["queued", "queued", "queued"]
    assert [job.message for job in jobs] == [
        "Queued for update.",
        "Queued for update.",
        "Queued for update.",
    ]


@pytest.mark.asyncio
async def test_update_data_valid_post_skips_duplicate_active_jobs(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = update_data_job_client
    async with session_factory() as session:
        session.add(
            INGJOB(
                isin="LU1681044993",
                requested_user="prior-user",
                status="running",
                message="Already running.",
            )
        )
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/update-data",
        data={"isins": "IE00BMTX1Y45 LU1681044993 US0378331005"},
    )

    assert response.status_code == 200
    assert "<h2>Update job status</h2>" in response.text
    assert '<span class="status-pill status-queued">queued</span>' in response.text
    assert '<span class="status-pill status-skipped">skipped</span>' in response.text
    assert "Skipped: active update job already exists." in response.text

    async with session_factory() as session:
        jobs = (
            await session.scalars(select(INGJOB).order_by(INGJOB.id))
        ).all()

    assert [job.isin for job in jobs] == ["LU1681044993", "IE00BMTX1Y45", "US0378331005"]
    assert [job.status for job in jobs] == ["running", "queued", "queued"]
    assert [job.requested_user for job in jobs] == ["prior-user", "admin", "admin"]


@pytest.mark.asyncio
async def test_documentation_page_renders_authenticated_help_content(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/documentation")

    assert response.status_code == 200
    assert "<title>Documentation - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Documentation</h1>' in response.text
    assert "Quick reference for authenticated BusinessQuery and Search use." in response.text
    assert "BusinessQuery queries V2_TAXDATEUR only." in response.text
    assert "K40" in response.text
    assert "K61" in response.text
    assert "K62" in response.text
    assert "<th scope=\"col\">Field</th>" in response.text
    assert "<th scope=\"col\">German label</th>" in response.text
    assert "<th scope=\"col\">Description</th>" in response.text
    assert "<th scope=\"col\">Usage</th>" in response.text
    assert "Descriptions are based on OeKB tax field documentation." in response.text
    for tax_line in TAX_LINES:
        assert f"<td>{tax_line.line_code}</td>" in response.text
        assert f"<td>{tax_line.name_de}</td>" in response.text
        assert f"<td>{tax_line.description}</td>" in response.text
        assert f"<td>{tax_line.usage_note}</td>" in response.text
    assert "PVM" in response.text
    assert "BVO" in response.text
    assert "STI" in response.text
    assert "amount multiplier" in response.text
    assert "CSV exports include" in response.text
    assert "Search helps find available fund tax data" in response.text


@pytest.mark.asyncio
async def test_logout_clears_session_and_protects_app(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    logout_response = await web_client.post("/logout")

    assert logout_response.status_code == 303
    assert logout_response.headers["location"] == "/login"
    assert "easyetfsat_session=" in logout_response.headers["set-cookie"]
    assert "Max-Age=0" in logout_response.headers["set-cookie"]

    app_response = await web_client.get("/app")
    assert app_response.status_code == 303
    assert app_response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_web_routes_preserve_existing_health_contract(web_client: httpx.AsyncClient) -> None:
    response = await web_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
