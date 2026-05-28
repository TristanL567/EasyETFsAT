from __future__ import annotations

from datetime import date
from decimal import Decimal

import httpx
import pytest

from fondant.api.main import create_app
from fondant.api.routes import web as web_routes
from fondant.business_query import BusinessQueryInput, BusinessQueryResult, BusinessQueryResultRow


@pytest.fixture
async def web_client() -> httpx.AsyncClient:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    client = httpx.AsyncClient(transport=transport, base_url="http://test")
    yield client
    await client.aclose()


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
    for path in ["/app", "/app/business-query", "/app/search", "/app/documentation"]:
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
    assert response.text.count('class="portal-nav-link') == 3
    assert ">BusinessQuery<" in response.text
    assert ">Search<" in response.text
    assert ">Documentation<" in response.text
    assert 'href="/app/business-query"' in response.text
    assert 'href="/app/search"' in response.text
    assert 'href="/app/documentation"' in response.text
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
async def test_non_business_query_placeholder_pages_render_for_authenticated_users(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    cases = [
        (
            "/app/search",
            "Search",
            "Placeholder workspace for future portfolio and report search.",
        ),
        (
            "/app/documentation",
            "Documentation",
            "Placeholder workspace for future user documentation.",
        ),
    ]
    for path, title, summary in cases:
        response = await web_client.get(path)

        assert response.status_code == 200
        assert f"<title>{title} - EasyETFsAT</title>" in response.text
        assert f'<h1 id="app-title">{title}</h1>' in response.text
        assert summary in response.text


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
