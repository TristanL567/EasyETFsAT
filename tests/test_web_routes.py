from __future__ import annotations

import csv
import logging
from collections.abc import AsyncIterator
from datetime import date, datetime
from decimal import Decimal
from io import StringIO
from pathlib import Path
from urllib.parse import urlencode

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
from fondant.db.models import BQGROUP, BQSAVED, INGJOB
from fondant.db.session import get_session
from fondant.search import FundSearchResult
from fondant.tax_registry import TAX_LINES


@pytest.fixture
async def web_client() -> AsyncIterator[httpx.AsyncClient]:
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
    yield client
    await client.aclose()
    app.dependency_overrides.clear()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


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


@pytest.fixture
async def saved_query_client() -> AsyncIterator[
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
    assert "/app/business-query/new" in route_paths
    assert "/app/business-query/queries" in route_paths
    assert "/app/business-query/queries/{saved_query_id}/edit" in route_paths
    assert "/app/business-query/groups" in route_paths
    assert "/app/business-query/save" in route_paths
    assert "/app/business-query/saved/{saved_query_id}/load" in route_paths
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
        "/app/business-query/new",
        "/app/business-query/queries",
        "/app/business-query/queries/1/edit",
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
    assert response.text.count('class="portal-nav-link') == 7
    assert ">BusinessQuery<" in response.text
    assert ">Add New Query<" in response.text
    assert ">Queries<" in response.text
    assert ">Group BusinessQuery<" in response.text
    assert ">Search<" in response.text
    assert ">Update Data<" in response.text
    assert ">Documentation<" in response.text
    assert 'href="/app/business-query"' in response.text
    assert 'href="/app/business-query/new"' in response.text
    assert 'href="/app/business-query/queries"' in response.text
    assert 'href="/app/business-query/groups"' in response.text
    assert 'href="/app/search"' in response.text
    assert 'href="/app/update-data"' in response.text
    assert 'href="/app/documentation"' in response.text
    normalized_html = " ".join(response.text.split())
    assert (
        ">BusinessQuery</a> <div class=\"portal-subnav\" aria-label=\"BusinessQuery pages\"> "
        "<a class=\"portal-nav-link portal-subnav-link active\" href=\"/app/business-query/new\" "
        "aria-current=\"page\" >Add New Query</a> "
        "<a class=\"portal-nav-link portal-subnav-link\" href=\"/app/business-query/queries\" "
        ">Queries</a> "
        "<a class=\"portal-nav-link portal-subnav-link\" href=\"/app/business-query/groups\" "
        ">Group BusinessQuery</a> </div> </div> <a class=\"portal-nav-link\" href=\"/app/search\" >Search</a> "
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

    for path in ["/app", "/app/business-query", "/app/business-query/new"]:
        response = await web_client.get(path)

        assert response.status_code == 200
        assert (
            '<form class="business-query-form" method="post" '
            'action="/app/business-query/new" aria-label="BusinessQuery setup" novalidate>'
        ) in response.text
        assert '<label for="query-name">Custom query name</label>' in response.text
        assert 'name="query_name"' in response.text
        assert '<label for="isins">ISIN input area</label>' in response.text
        assert 'name="isins"' in response.text
        assert '<label for="legal-entity-type">Legal entity type</label>' in response.text
        assert '<label for="subcategory-key">Subcategory</label>' in response.text
        assert 'name="subcategory_key"' in response.text
        assert '<label for="tax-year-filter">Tax year</label>' in response.text
        assert 'name="tax_year_filter"' in response.text
        assert "<legend>Tax fields</legend>" in response.text
        for tax_line in TAX_LINES:
            assert f'id="tax-field-{tax_line.line_code}"' in response.text
            assert f'value="{tax_line.line_code}"' in response.text
            assert f"{tax_line.line_code} - {tax_line.name_de}" in response.text
        assert '<label for="amount">Amount</label>' in response.text
        assert 'name="amount"' in response.text
        assert '<label for="query-note">Note</label>' in response.text
        assert 'name="note"' in response.text
        assert 'formaction="/app/business-query/save"' in response.text
        assert "Save query" in response.text
        assert "<h2>Saved queries</h2>" not in response.text
        assert "No saved queries yet." not in response.text
        assert "Submit structured inputs to calculate Austrian ETF tax values." in response.text
        assert "How to read and reuse a query" in response.text
        assert "PA mit Option means private assets with option" in response.text
        assert "PA ohne Option means private assets without option" in response.text
        assert "BV mit Option means business assets with option" in response.text
        assert "BV ohne Option means business assets without option" in response.text
        assert "BV jur. Person means business assets for a legal person" in response.text
        assert "Stiftung means foundation assets" in response.text
        assert (
            "All available years runs the query without a tax-year filter."
            in response.text
        )
        assert "The amount multiplies each base tax value" in response.text
        assert (
            "Load one later, replace or paste the ISINs you want to review, then rerun"
            in response.text
        )
        assert 'type="submit"' in response.text

        normalized_html = " ".join(response.text.split())
        assert (
            '<option value="natural person" selected>natural person</option> '
            '<option value="business">business</option> '
            '<option value="Stiftung">Stiftung</option>'
        ) in normalized_html
        assert '<option value="natural_person_all" selected>All private investor categories</option>' in normalized_html
        assert '<option value="natural_person_pa_with_option">PA mit Option</option>' in normalized_html
        assert '<option value="natural_person_pa_without_option">PA ohne Option</option>' in normalized_html
        assert '<option value="business_all">All business categories</option>' in normalized_html
        assert '<option value="business_bv_with_option">BV mit Option</option>' in normalized_html
        assert '<option value="business_bv_without_option">BV ohne Option</option>' in normalized_html
        assert '<option value="business_bv_legal_person">BV jur. Person</option>' in normalized_html
        assert '<option value="stiftung">Stiftung</option>' in normalized_html
        assert '<option value="all_available_years" selected>All available years</option>' in normalized_html
        # BQ-004 uses a conservative server-provided rolling list instead of
        # adding a database dependency to authenticated GET rendering.
        assert f'<option value="{date.today().year}">{date.today().year}</option>' in normalized_html


@pytest.mark.asyncio
async def test_business_query_queries_renders_saved_query_management(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/business-query/queries")

    assert response.status_code == 200
    assert "<title>Queries - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Queries</h1>' in response.text
    assert "Manage saved BusinessQuery rules for authenticated review." in response.text
    assert 'aria-label="Create BusinessQuery group"' not in response.text
    assert '<label for="group-name">Group name</label>' not in response.text
    assert '<label for="group-description">Description</label>' not in response.text
    assert "Create group" not in response.text
    assert "<h2>Saved queries</h2>" in response.text
    assert (
        "Review existing saved queries. Use Run to execute saved default ISINs, "
        "Load to adjust a rule, or Edit to update it."
    ) in response.text
    assert 'aria-label="Filter saved queries by group"' in response.text
    assert "Filter by group" in response.text
    assert (
        "No saved queries yet. Create one from Add New Query after entering a BusinessQuery rule."
        in response.text
    )
    assert "No groups yet." not in response.text
    assert '<form class="business-query-form"' not in response.text
    assert ">BusinessQuery<" in response.text
    assert ">Add New Query<" in response.text
    assert ">Queries<" in response.text
    assert ">Group BusinessQuery<" in response.text


@pytest.mark.asyncio
async def test_business_query_group_page_renders_group_management(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/business-query/groups")

    assert response.status_code == 200
    assert "<title>Group BusinessQuery - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Group BusinessQuery</h1>' in response.text
    assert "Create and review saved-query groups for BusinessQuery." in response.text
    assert 'aria-label="Create BusinessQuery group"' in response.text
    assert '<label for="group-name">Group name</label>' in response.text
    assert '<label for="group-description">Description</label>' in response.text
    assert "Create group" in response.text
    assert '<div class="result-panel business-query-group-list" aria-label="BusinessQuery groups">' in response.text
    assert "<h2>Groups</h2>" in response.text
    assert "No groups yet." in response.text
    assert "<h2>Saved queries</h2>" not in response.text


@pytest.mark.asyncio
async def test_business_query_get_prefills_selected_isin_from_search_link(
    web_client: httpx.AsyncClient,
) -> None:
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/business-query/new", params={"isins": "ie00bmtx1y45"})

    assert response.status_code == 200
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert ">IE00BMTX1Y45</textarea>" in response.text


@pytest.mark.asyncio
async def test_business_query_post_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    for path in ["/app/business-query", "/app/business-query/new"]:
        response = await web_client.post(
            path,
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
async def test_business_query_save_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post(
        "/app/business-query/save",
        data={
            "query_name": "Monthly review",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "all_available_years",
            "amount": "1000",
        },
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_business_query_edit_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    get_response = await web_client.get("/app/business-query/queries/1/edit")
    assert get_response.status_code == 303
    assert get_response.headers["location"] == "/login"

    post_response = await web_client.post(
        "/app/business-query/queries/1/edit",
        data={
            "query_name": "Monthly review",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "all_available_years",
            "amount": "1000",
        },
    )
    assert post_response.status_code == 303
    assert post_response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_business_query_group_create_redirects_unauthenticated_users_to_login(
    web_client: httpx.AsyncClient,
) -> None:
    response = await web_client.post(
        "/app/business-query/groups",
        data={"group_name": "Quarterly reviews", "description": "Quarterly rules"},
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/login"


@pytest.mark.asyncio
async def test_authenticated_user_can_save_business_query_with_structured_fields(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/save",
        data={
            "query_name": "  Monthly review  ",
            "isins": "ie00bmtx1y45\n lu1681044993 ",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_legal_person",
            "tax_year_filter": "2025",
            "amount": "250.75",
            "note": " Run for model portfolio ",
        },
    )

    assert response.status_code == 200
    assert "Saved query created." in response.text
    assert "<h2>Query results</h2>" not in response.text
    assert "<h2>Saved queries</h2>" not in response.text
    assert 'value="Monthly review"' in response.text
    assert ">IE00BMTX1Y45\nLU1681044993</textarea>" in response.text
    assert ">Run for model portfolio</textarea>" in response.text

    async with session_factory() as session:
        saved_query = await session.scalar(select(BQSAVED))

    assert saved_query is not None
    assert saved_query.owner_username == "admin"
    assert saved_query.query_name == "Monthly review"
    assert saved_query.legal_entity_type == "business"
    assert saved_query.subcategory_key == "business_bv_legal_person"
    assert saved_query.tax_year_filter == "2025"
    assert saved_query.amount == Decimal("250.7500000000")
    assert saved_query.note == "Run for model portfolio"
    assert saved_query.default_isins == ["IE00BMTX1Y45", "LU1681044993"]
    assert saved_query.selected_tax_fields == ["K40", "K61", "K62"]


@pytest.mark.asyncio
async def test_authenticated_user_can_save_business_query_with_selected_tax_fields(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/save",
        content=urlencode(
            [
                ("query_name", "Selected fields rule"),
                ("isins", "IE00BMTX1Y45"),
                ("legal_entity_type", "natural person"),
                ("subcategory_key", "natural_person_pa_with_option"),
                ("tax_year_filter", "2025"),
                ("tax_fields_present", "1"),
                ("tax_fields", "K11"),
                ("tax_fields", "K61"),
                ("amount", "100"),
                ("note", "Field-specific saved rule"),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 200
    assert "Saved query created." in response.text

    async with session_factory() as session:
        saved_query = await session.scalar(select(BQSAVED))

    assert saved_query is not None
    assert saved_query.selected_tax_fields == ["K11", "K61"]


@pytest.mark.asyncio
async def test_business_query_save_allows_optional_default_isins(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/save",
        data={
            "query_name": "No defaults",
            "isins": " ",
            "legal_entity_type": "natural person",
            "subcategory_key": "natural_person_all",
            "tax_year_filter": "all_available_years",
            "amount": "100",
            "note": "",
        },
    )

    assert response.status_code == 200
    assert "Saved query created." in response.text

    async with session_factory() as session:
        saved_query = await session.scalar(select(BQSAVED))

    assert saved_query is not None
    assert saved_query.default_isins is None
    assert saved_query.note is None
    assert saved_query.selected_tax_fields == ["K40", "K61", "K62"]


@pytest.mark.asyncio
async def test_business_query_save_duplicate_name_for_same_user_shows_validation_error(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    payload = {
        "query_name": "Monthly review",
        "legal_entity_type": "business",
        "subcategory_key": "business_all",
        "tax_year_filter": "all_available_years",
        "amount": "1000",
    }
    first_response = await web_client.post("/app/business-query/save", data=payload)
    assert first_response.status_code == 200

    duplicate_response = await web_client.post("/app/business-query/save", data=payload)

    assert duplicate_response.status_code == 200
    assert "A saved query with this name already exists." in duplicate_response.text
    assert "IntegrityError" not in duplicate_response.text
    assert "UNIQUE constraint" not in duplicate_response.text
    async with session_factory() as session:
        saved_queries = (await session.scalars(select(BQSAVED))).all()

    assert len(saved_queries) == 1


@pytest.mark.asyncio
async def test_business_query_save_allows_same_name_for_different_users(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        session.add(
            BQSAVED(
                owner_username="alice",
                query_name="Quarterly review",
                legal_entity_type="natural person",
                subcategory_key="natural_person_all",
                tax_year_filter="all_available_years",
                amount=Decimal("100.00"),
            )
        )
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/save",
        data={
            "query_name": "Quarterly review",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "2025",
            "amount": "250",
        },
    )

    assert response.status_code == 200
    assert "Saved query created." in response.text
    async with session_factory() as session:
        saved_queries = (
            await session.scalars(select(BQSAVED).order_by(BQSAVED.owner_username))
        ).all()

    assert [(query.owner_username, query.query_name) for query in saved_queries] == [
        ("admin", "Quarterly review"),
        ("alice", "Quarterly review"),
    ]


@pytest.mark.asyncio
async def test_business_query_saved_list_shows_only_current_user_queries(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        session.add_all(
            [
                BQSAVED(
                    owner_username="admin",
                    query_name="Admin saved rule",
                    legal_entity_type="business",
                    subcategory_key="business_bv_with_option",
                    tax_year_filter="2025",
                    amount=Decimal("1000.00"),
                    updated_at=datetime(2026, 1, 2, 3, 4, 5, 678901),
                ),
                BQSAVED(
                    owner_username="other-user",
                    query_name="Other user rule",
                    legal_entity_type="Stiftung",
                    subcategory_key="stiftung",
                    tax_year_filter="all_available_years",
                    amount=Decimal("500.00"),
                    updated_at=datetime(2026, 1, 3, 4, 5, 6, 789012),
                ),
            ]
        )
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/business-query/queries")

    assert response.status_code == 200
    assert "<title>Queries - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Queries</h1>' in response.text
    assert "<h2>Saved queries</h2>" in response.text
    assert (
        "Review existing saved queries. Use Run to execute saved default ISINs, "
        "Load to adjust a rule, or Edit to update it."
    ) in response.text
    assert "Filter by group" in response.text
    assert '<form class="business-query-form"' not in response.text
    assert 'action="/app/business-query/groups"' not in response.text
    assert "Create group" not in response.text
    assert "<td>Admin saved rule</td>" in response.text
    assert "<td>business</td>" in response.text
    assert "<td>BV mit Option</td>" in response.text
    assert "<td>2025</td>" in response.text
    assert "<td>1000.00</td>" in response.text
    assert "<td>2026-01-02 03:04</td>" in response.text
    assert "2026-01-02 03:04:05" not in response.text
    assert '<th scope="col">Actions</th>' in response.text
    assert '<h3 id="saved-query-group-ungrouped">Ungrouped</h3>' in response.text
    assert "/app/business-query/queries/" in response.text
    assert ">Run</button>" in response.text
    assert ">Load</button>" in response.text
    assert ">Edit</a>" in response.text
    assert "Other user rule" not in response.text
    assert "other-user" not in response.text


@pytest.mark.asyncio
async def test_business_query_queries_groups_saved_queries_by_group(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        group = BQGROUP(
            owner_username="admin",
            group_name="Quarterly reviews",
            description="Quarterly model rules",
        )
        session.add(group)
        await session.flush()
        session.add_all(
            [
                BQSAVED(
                    owner_username="admin",
                    group_id=group.id,
                    query_name="Grouped rule",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("100.00"),
                    updated_at=datetime(2026, 2, 3, 4, 5, 6, 123456),
                ),
                BQSAVED(
                    owner_username="admin",
                    query_name="Loose rule",
                    legal_entity_type="natural person",
                    subcategory_key="natural_person_all",
                    tax_year_filter="all_available_years",
                    amount=Decimal("200.00"),
                    updated_at=datetime(2026, 2, 4, 5, 6, 7, 123456),
                ),
            ]
        )
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get("/app/business-query/queries")

    assert response.status_code == 200
    assert ">Quarterly reviews</h3>" in response.text
    assert "Quarterly model rules" in response.text
    assert '<h3 id="saved-query-group-ungrouped">Ungrouped</h3>' in response.text
    assert response.text.index(">Quarterly reviews</h3>") < response.text.index(
        "<td>Grouped rule</td>"
    )
    assert response.text.index('<h3 id="saved-query-group-ungrouped">Ungrouped</h3>') < (
        response.text.index("<td>Loose rule</td>")
    )
    assert "<td>100.00</td>" in response.text
    assert "<td>200.00</td>" in response.text
    assert "<td>2026-02-03 04:05</td>" in response.text
    assert "<td>2026-02-04 05:06</td>" in response.text


@pytest.mark.asyncio
async def test_business_query_group_filter_limits_saved_queries_to_owned_group(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        shown_group = BQGROUP(owner_username="admin", group_name="Shown group")
        hidden_group = BQGROUP(owner_username="admin", group_name="Hidden group")
        other_user_group = BQGROUP(owner_username="other-user", group_name="Other group")
        session.add_all([shown_group, hidden_group, other_user_group])
        await session.flush()
        session.add_all(
            [
                BQSAVED(
                    owner_username="admin",
                    group_id=shown_group.id,
                    query_name="Shown rule",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("100.00"),
                ),
                BQSAVED(
                    owner_username="admin",
                    group_id=hidden_group.id,
                    query_name="Hidden rule",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("200.00"),
                ),
                BQSAVED(
                    owner_username="other-user",
                    group_id=other_user_group.id,
                    query_name="Other user rule",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("300.00"),
                ),
            ]
        )
        await session.commit()
        shown_group_id = shown_group.id
        other_user_group_id = other_user_group.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    filtered_response = await web_client.get(
        "/app/business-query/queries",
        params={"group_id": str(shown_group_id)},
    )

    assert filtered_response.status_code == 200
    assert ">Shown group</h3>" in filtered_response.text
    assert "<td>Shown rule</td>" in filtered_response.text
    assert "Hidden rule" not in filtered_response.text
    assert "Other user rule" not in filtered_response.text
    assert "Other group" not in filtered_response.text

    other_filter_response = await web_client.get(
        "/app/business-query/queries",
        params={"group_id": str(other_user_group_id)},
    )

    assert other_filter_response.status_code == 200
    assert "Other user rule" not in other_filter_response.text
    assert "Other group" not in other_filter_response.text


@pytest.mark.asyncio
async def test_business_query_group_filter_shows_selected_group_empty_state(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        group = BQGROUP(owner_username="admin", group_name="Empty group")
        session.add(group)
        await session.commit()
        group_id = group.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get(
        "/app/business-query/queries",
        params={"group_id": str(group_id)},
    )

    assert response.status_code == 200
    assert ">Empty group</h3>" in response.text
    assert "Selected group has no saved queries." in response.text
    assert "No saved queries yet." not in response.text


@pytest.mark.asyncio
async def test_authenticated_user_can_create_business_query_group(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/groups",
        data={
            "group_name": " Quarterly reviews ",
            "description": " Model portfolio rules ",
        },
    )

    assert response.status_code == 200
    assert "Group created." in response.text
    assert '<h1 id="app-title">Group BusinessQuery</h1>' in response.text
    assert "<h2>Groups</h2>" in response.text
    assert "<td>Quarterly reviews</td>" in response.text
    assert "<td>Model portfolio rules</td>" in response.text
    assert "<h2>Saved queries</h2>" not in response.text
    async with session_factory() as session:
        group = await session.scalar(select(BQGROUP))

    assert group is not None
    assert group.owner_username == "admin"
    assert group.group_name == "Quarterly reviews"
    assert group.description == "Model portfolio rules"


@pytest.mark.asyncio
async def test_business_query_group_duplicate_name_for_same_user_shows_validation_error(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        session.add(BQGROUP(owner_username="admin", group_name="Quarterly reviews"))
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/groups",
        data={"group_name": "Quarterly reviews", "description": ""},
    )

    assert response.status_code == 200
    assert '<h1 id="app-title">Group BusinessQuery</h1>' in response.text
    assert "A group with this name already exists." in response.text
    assert "IntegrityError" not in response.text
    assert "UNIQUE constraint" not in response.text
    async with session_factory() as session:
        groups = (await session.scalars(select(BQGROUP))).all()

    assert len(groups) == 1


@pytest.mark.asyncio
async def test_business_query_group_allows_same_name_for_different_users(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        session.add(BQGROUP(owner_username="other-user", group_name="Quarterly reviews"))
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query/groups",
        data={"group_name": "Quarterly reviews", "description": ""},
    )

    assert response.status_code == 200
    assert "Group created." in response.text
    assert '<h1 id="app-title">Group BusinessQuery</h1>' in response.text
    assert "<td>Quarterly reviews</td>" in response.text
    async with session_factory() as session:
        groups = (await session.scalars(select(BQGROUP).order_by(BQGROUP.owner_username))).all()

    assert [(group.owner_username, group.group_name) for group in groups] == [
        ("admin", "Quarterly reviews"),
        ("other-user", "Quarterly reviews"),
    ]


@pytest.mark.asyncio
async def test_current_user_can_load_saved_business_query_with_structured_fields(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Loaded monthly rule",
            legal_entity_type="business",
            subcategory_key="business_bv_legal_person",
            tax_year_filter="2025",
            amount=Decimal("250.75"),
            note="Stored model portfolio note",
            default_isins=["IE00BMTX1Y45", "LU1681044993"],
            selected_tax_fields=["K11", "K61"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get(f"/app/business-query/saved/{saved_query_id}/load")

    assert response.status_code == 200
    assert "Saved query loaded." in response.text
    assert 'value="Loaded monthly rule"' in response.text
    assert ">IE00BMTX1Y45\nLU1681044993</textarea>" in response.text
    assert '<option value="business" selected>business</option>' in response.text
    assert '<option value="business_bv_legal_person" selected>BV jur. Person</option>' in response.text
    assert '<option value="2025" selected>2025</option>' in response.text
    assert 'value="250.7500000000"' in response.text
    assert ">Stored model portfolio note</textarea>" in response.text
    normalized_html = " ".join(response.text.split())
    assert '<input id="tax-field-K11" name="tax_fields" type="checkbox" value="K11" checked >' in normalized_html
    assert '<input id="tax-field-K61" name="tax_fields" type="checkbox" value="K61" checked >' in normalized_html
    assert '<input id="tax-field-K40" name="tax_fields" type="checkbox" value="K40" >' in normalized_html
    assert "<h2>Saved queries</h2>" not in response.text


@pytest.mark.asyncio
async def test_loaded_saved_business_query_can_replace_isins_and_rerun_existing_post_flow(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Rerun saved rule",
            legal_entity_type="business",
            subcategory_key="business_bv_without_option",
            tax_year_filter="2025",
            amount=Decimal("1000.50"),
            note="Rerun note",
            default_isins=["IE00BMTX1Y45"],
            selected_tax_fields=["K11"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

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

    load_response = await web_client.get(f"/app/business-query/saved/{saved_query_id}/load")
    assert load_response.status_code == 200

    rerun_response = await web_client.post(
        "/app/business-query",
        content=urlencode(
            [
                ("query_name", "Rerun saved rule"),
                ("isins", "LU1681044993"),
                ("legal_entity_type", "business"),
                ("subcategory_key", "business_bv_without_option"),
                ("tax_year_filter", "2025"),
                ("tax_fields_present", "1"),
                ("tax_fields", "K11"),
                ("amount", "1000.5000000000"),
                ("note", "Rerun note"),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert rerun_response.status_code == 200
    assert "<h2>Query results</h2>" in rerun_response.text
    assert ">LU1681044993</textarea>" in rerun_response.text
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.query_name == "Rerun saved rule"
    assert query.isins == ("LU1681044993",)
    assert query.legal_entity_type == "business"
    assert query.subcategory_key == "business_bv_without_option"
    assert query.tax_year_filter == "2025"
    assert query.tax_fields == ("K11",)
    assert query.amount_multiplier == Decimal("1000.5000000000")


@pytest.mark.asyncio
async def test_another_users_saved_business_query_cannot_be_loaded_or_displayed(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="other-user",
            query_name="Other private rule",
            legal_entity_type="Stiftung",
            subcategory_key="stiftung",
            tax_year_filter="all_available_years",
            amount=Decimal("500.00"),
            note="Other user note",
            default_isins=["US0378331005"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    list_response = await web_client.get("/app/business-query/queries")
    assert list_response.status_code == 200
    assert "Other private rule" not in list_response.text
    assert "US0378331005" not in list_response.text
    assert f"/app/business-query/saved/{saved_query_id}/load" not in list_response.text
    assert f"/app/business-query/queries/{saved_query_id}/edit" not in list_response.text

    load_response = await web_client.get(f"/app/business-query/saved/{saved_query_id}/load")
    assert load_response.status_code == 404
    assert "Other private rule" not in load_response.text
    assert "US0378331005" not in load_response.text
    assert "other-user" not in load_response.text


@pytest.mark.asyncio
async def test_owner_can_open_saved_business_query_edit_form(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Editable rule",
            legal_entity_type="business",
            subcategory_key="business_bv_with_option",
            tax_year_filter="2025",
            amount=Decimal("1000.00"),
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get(f"/app/business-query/queries/{saved_query_id}/edit")

    assert response.status_code == 200
    assert "<title>Edit Query - EasyETFsAT</title>" in response.text
    assert '<h1 id="app-title">Edit Query</h1>' in response.text
    assert 'aria-label="Edit saved BusinessQuery rule"' in response.text
    assert f'action="/app/business-query/queries/{saved_query_id}/edit"' in response.text
    assert 'value="Editable rule"' in response.text
    assert "Save changes" in response.text


@pytest.mark.asyncio
async def test_saved_business_query_edit_form_prefills_all_saved_fields(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        owned_group = BQGROUP(owner_username="admin", group_name="Quarterly reviews")
        other_group = BQGROUP(owner_username="other-user", group_name="Other user group")
        session.add_all([owned_group, other_group])
        await session.flush()
        saved_query = BQSAVED(
            owner_username="admin",
            group_id=owned_group.id,
            query_name="Loaded edit rule",
            legal_entity_type="business",
            subcategory_key="business_bv_legal_person",
            tax_year_filter="2025",
            amount=Decimal("250.75"),
            note="Stored edit note",
            default_isins=["IE00BMTX1Y45", "LU1681044993"],
            selected_tax_fields=["K11", "K62"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id
        owned_group_id = owned_group.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.get(f"/app/business-query/queries/{saved_query_id}/edit")

    assert response.status_code == 200
    assert 'value="Loaded edit rule"' in response.text
    assert ">IE00BMTX1Y45\nLU1681044993</textarea>" in response.text
    assert '<option value="business" selected>business</option>' in response.text
    assert '<option value="business_bv_legal_person" selected>BV jur. Person</option>' in response.text
    assert '<option value="2025" selected>2025</option>' in response.text
    assert 'value="250.7500000000"' in response.text
    assert ">Stored edit note</textarea>" in response.text
    assert f'<option value="{owned_group_id}" selected>Quarterly reviews</option>' in response.text
    normalized_html = " ".join(response.text.split())
    assert '<input id="edit-tax-field-K11" name="tax_fields" type="checkbox" value="K11" checked >' in normalized_html
    assert '<input id="edit-tax-field-K62" name="tax_fields" type="checkbox" value="K62" checked >' in normalized_html
    assert '<input id="edit-tax-field-K40" name="tax_fields" type="checkbox" value="K40" >' in normalized_html
    assert "Other user group" not in response.text


@pytest.mark.asyncio
async def test_owner_can_update_saved_business_query_fields(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Original edit rule",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter="all_available_years",
            amount=Decimal("100.00"),
            note="Original note",
            default_isins=["IE00BMTX1Y45"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        f"/app/business-query/queries/{saved_query_id}/edit",
        content=urlencode(
            [
                ("query_name", " Updated edit rule "),
                ("isins", "lu1681044993 us0378331005"),
                ("legal_entity_type", "business"),
                ("subcategory_key", "business_bv_without_option"),
                ("tax_year_filter", "2025"),
                ("tax_fields_present", "1"),
                ("tax_fields", "K11"),
                ("tax_fields", "K62"),
                ("amount", "777.25"),
                ("note", " Updated note "),
                ("group_id", ""),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 200
    assert "Saved query updated." in response.text
    assert 'value="Updated edit rule"' in response.text
    assert ">LU1681044993\nUS0378331005</textarea>" in response.text

    async with session_factory() as session:
        saved_query = await session.scalar(select(BQSAVED).where(BQSAVED.id == saved_query_id))

    assert saved_query is not None
    assert saved_query.query_name == "Updated edit rule"
    assert saved_query.legal_entity_type == "business"
    assert saved_query.subcategory_key == "business_bv_without_option"
    assert saved_query.tax_year_filter == "2025"
    assert saved_query.amount == Decimal("777.2500000000")
    assert saved_query.note == "Updated note"
    assert saved_query.default_isins == ["LU1681044993", "US0378331005"]
    assert saved_query.selected_tax_fields == ["K11", "K62"]


@pytest.mark.asyncio
async def test_owner_can_assign_and_remove_saved_business_query_group(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        group = BQGROUP(owner_username="admin", group_name="Tax review")
        other_group = BQGROUP(owner_username="other-user", group_name="Hidden group")
        session.add_all([group, other_group])
        await session.flush()
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Group edit rule",
            legal_entity_type="business",
            subcategory_key="business_all",
            tax_year_filter="2025",
            amount=Decimal("100.00"),
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id
        group_id = group.id
        other_group_id = other_group.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    assign_response = await web_client.post(
        f"/app/business-query/queries/{saved_query_id}/edit",
        data={
            "query_name": "Group edit rule",
            "isins": "",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "2025",
            "amount": "100",
            "note": "",
            "group_id": str(group_id),
        },
    )
    assert assign_response.status_code == 200
    assert "Saved query updated." in assign_response.text
    assert f'<option value="{group_id}" selected>Tax review</option>' in assign_response.text

    invalid_group_response = await web_client.post(
        f"/app/business-query/queries/{saved_query_id}/edit",
        data={
            "query_name": "Group edit rule",
            "isins": "",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "2025",
            "amount": "100",
            "note": "",
            "group_id": str(other_group_id),
        },
    )
    assert invalid_group_response.status_code == 200
    assert "Choose one of your saved query groups." in invalid_group_response.text
    assert "Hidden group" not in invalid_group_response.text

    remove_response = await web_client.post(
        f"/app/business-query/queries/{saved_query_id}/edit",
        data={
            "query_name": "Group edit rule",
            "isins": "",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "2025",
            "amount": "100",
            "note": "",
            "group_id": "",
        },
    )
    assert remove_response.status_code == 200
    assert "Saved query updated." in remove_response.text
    assert '<option value="" selected>Ungrouped</option>' in remove_response.text

    async with session_factory() as session:
        saved_query = await session.scalar(select(BQSAVED).where(BQSAVED.id == saved_query_id))

    assert saved_query is not None
    assert saved_query.group_id is None


@pytest.mark.asyncio
async def test_another_users_saved_business_query_cannot_be_viewed_or_edited_by_id(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="other-user",
            query_name="Other edit rule",
            legal_entity_type="Stiftung",
            subcategory_key="stiftung",
            tax_year_filter="all_available_years",
            amount=Decimal("500.00"),
            note="Other edit note",
            default_isins=["US0378331005"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    get_response = await web_client.get(f"/app/business-query/queries/{saved_query_id}/edit")
    assert get_response.status_code == 404
    assert "Other edit rule" not in get_response.text
    assert "US0378331005" not in get_response.text
    assert "other-user" not in get_response.text

    post_response = await web_client.post(
        f"/app/business-query/queries/{saved_query_id}/edit",
        data={
            "query_name": "Hijacked rule",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "2025",
            "amount": "1000",
            "note": "changed",
            "group_id": "",
        },
    )
    assert post_response.status_code == 404
    assert "Hijacked rule" not in post_response.text

    async with session_factory() as session:
        saved_query = await session.scalar(select(BQSAVED).where(BQSAVED.id == saved_query_id))

    assert saved_query is not None
    assert saved_query.query_name == "Other edit rule"
    assert saved_query.note == "Other edit note"
    assert saved_query.default_isins == ["US0378331005"]


@pytest.mark.asyncio
async def test_saved_business_query_edit_duplicate_name_for_same_user_shows_validation_error(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        session.add_all(
            [
                BQSAVED(
                    owner_username="admin",
                    query_name="Existing rule",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("100.00"),
                ),
                BQSAVED(
                    owner_username="admin",
                    query_name="Second rule",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("200.00"),
                ),
            ]
        )
        await session.commit()
        second_query = await session.scalar(
            select(BQSAVED).where(BQSAVED.query_name == "Second rule")
        )
        assert second_query is not None
        second_query_id = second_query.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        f"/app/business-query/queries/{second_query_id}/edit",
        data={
            "query_name": "Existing rule",
            "isins": "",
            "legal_entity_type": "business",
            "subcategory_key": "business_all",
            "tax_year_filter": "2025",
            "amount": "200",
            "note": "",
            "group_id": "",
        },
    )

    assert response.status_code == 200
    assert "A saved query with this name already exists." in response.text
    assert "IntegrityError" not in response.text
    assert "UNIQUE constraint" not in response.text

    async with session_factory() as session:
        saved_queries = (
            await session.scalars(select(BQSAVED).order_by(BQSAVED.query_name))
        ).all()

    assert [query.query_name for query in saved_queries] == ["Existing rule", "Second rule"]


@pytest.mark.asyncio
async def test_saved_business_query_listing_reflects_edited_values(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        group = BQGROUP(owner_username="admin", group_name="Edited group")
        session.add(group)
        await session.flush()
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Before list edit",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter="all_available_years",
            amount=Decimal("100.00"),
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id
        group_id = group.id

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    edit_response = await web_client.post(
        f"/app/business-query/queries/{saved_query_id}/edit",
        data={
            "query_name": "After list edit",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_without_option",
            "tax_year_filter": "2025",
            "amount": "300.50",
            "note": "",
            "group_id": str(group_id),
        },
    )
    assert edit_response.status_code == 200

    list_response = await web_client.get("/app/business-query/queries")

    assert list_response.status_code == 200
    assert "<td>After list edit</td>" in list_response.text
    assert "<td>business</td>" in list_response.text
    assert "<td>BV ohne Option</td>" in list_response.text
    assert "<td>2025</td>" in list_response.text
    assert "<td>300.50</td>" in list_response.text
    assert "<td>300.5000000000</td>" not in list_response.text
    assert '<h3 id="saved-query-group-' in list_response.text
    assert ">Edited group</h3>" in list_response.text
    assert "Before list edit" not in list_response.text


@pytest.mark.asyncio
async def test_owner_can_run_saved_business_query_from_queries_page(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Direct run rule",
            legal_entity_type="business",
            subcategory_key="business_bv_legal_person",
            tax_year_filter="2025",
            amount=Decimal("42.50"),
            default_isins=["LU1681044993"],
            selected_tax_fields=["K11", "K62"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    service_calls: list[BusinessQueryInput] = []

    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        service_calls.append(query)
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name=query.query_name,
                    isin="LU1681044993",
                    tax_year=2025,
                    oekb_report_id=1001,
                    fund_currency="EUR",
                    report_date=date(2025, 6, 15),
                    fx_rate=Decimal("1.0000000000"),
                    legal_entity_category="BVJ",
                    tax_field_code="K11",
                    tax_field_label="AG Ertraege",
                    base_eur_value=Decimal("1.2500000000"),
                    amount_multiplier=query.amount_multiplier,
                    calculated_eur_value=Decimal("53.1250000000"),
                    original_currency_code="EUR",
                    home_currency_code="EUR",
                    fx_date=date(2025, 6, 15),
                    base_home_currency_value=Decimal("1.2500000000"),
                    calculated_home_currency_value=Decimal("53.1250000000"),
                ),
            ),
        )

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(f"/app/business-query/queries/{saved_query_id}/run")

    assert response.status_code == 200
    assert "Saved query ran using its default ISINs." in response.text
    assert "<h2>Query results</h2>" in response.text
    assert "<dd>Direct run rule</dd>" in response.text
    assert "<dd>LU1681044993</dd>" in response.text
    assert "<dd>BV jur. Person</dd>" in response.text
    normalized_html = " ".join(response.text.split())
    assert "<dt>OeKB release date</dt>" in response.text
    assert "<dd> 2025-06-15 </dd>" in normalized_html
    assert "K11 - AG Ertraege" in response.text
    assert "<td>LU1681044993</td>" in response.text
    assert "<td>BVJ</td>" in response.text
    assert ">Run</button>" in response.text
    assert ">Load</button>" in response.text
    assert ">Edit</a>" in response.text
    assert len(service_calls) == 1
    query = service_calls[0]
    assert query.query_name == "Direct run rule"
    assert query.isins == ("LU1681044993",)
    assert query.legal_entity_type == "business"
    assert query.subcategory_key == "business_bv_legal_person"
    assert query.tax_year_filter == "2025"
    assert query.tax_fields == ("K11", "K62")
    assert query.amount_multiplier == Decimal("42.50")


@pytest.mark.asyncio
async def test_non_owner_cannot_run_saved_business_query_by_id(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="other-user",
            query_name="Other direct run rule",
            legal_entity_type="business",
            subcategory_key="business_all",
            tax_year_filter="2025",
            amount=Decimal("100.00"),
            default_isins=["LU1681044993"],
            selected_tax_fields=["K40"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    async def fail_if_called(session: object, query: BusinessQueryInput) -> BusinessQueryResult:
        raise AssertionError("non-owner saved query must not run")

    monkeypatch.setattr(web_routes, "execute_business_query", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(f"/app/business-query/queries/{saved_query_id}/run")

    assert response.status_code == 404
    assert "Other direct run rule" not in response.text
    assert "LU1681044993" not in response.text


@pytest.mark.asyncio
async def test_saved_business_query_run_without_default_isins_shows_friendly_error(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="No default ISIN rule",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter="all_available_years",
            amount=Decimal("100.00"),
            default_isins=None,
            selected_tax_fields=["K40"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

    async def fail_if_called(session: object, query: BusinessQueryInput) -> BusinessQueryResult:
        raise AssertionError("saved query without default ISINs must not run")

    monkeypatch.setattr(web_routes, "execute_business_query", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(f"/app/business-query/queries/{saved_query_id}/run")

    assert response.status_code == 200
    assert (
        "This saved query has no default ISINs. Load or edit it and add ISINs before running."
        in response.text
    )
    assert "<td>No default ISIN rule</td>" in response.text
    assert "<h2>Query results</h2>" not in response.text
    assert "No tax rows matched the saved query default ISINs." not in response.text


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
                    original_currency_code="USD",
                    home_currency_code="USD",
                    fx_date=date(2025, 6, 15),
                    base_home_currency_value=Decimal("11.0000000000"),
                    calculated_home_currency_value=Decimal("11005.500000000000"),
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
            "subcategory_key": "business_bv_without_option",
            "tax_year_filter": "2025",
            "amount": "1000.50",
        },
    )

    assert response.status_code == 200
    assert "<title>BusinessQuery - EasyETFsAT</title>" in response.text
    assert 'id="query-name"' in response.text
    assert 'value="Monthly review"' in response.text
    assert ">IE00BMTX1Y45\nLU1681044993</textarea>" in response.text
    assert '<option value="business" selected>business</option>' in response.text
    assert '<option value="business_bv_without_option" selected>BV ohne Option</option>' in response.text
    assert '<option value="2025" selected>2025</option>' in response.text
    assert 'value="1000.50"' in response.text
    assert "<h2>Query results</h2>" in response.text
    assert "<dd>IE00BMTX1Y45, LU1681044993</dd>" in response.text
    assert "<dt>Subcategory</dt>" in response.text
    assert "<dd>BV ohne Option</dd>" in response.text
    assert "<dt>Selected tax year</dt>" in response.text
    assert "<dd>2025</dd>" in response.text
    normalized_html = " ".join(response.text.split())
    assert "<dt>OeKB release date</dt>" in response.text
    assert "<dd> 2025-06-15 </dd>" in normalized_html
    assert "<dt>Amount</dt>" in response.text
    assert "<dd>1000.500</dd>" in response.text
    assert "<th scope=\"col\">ISIN</th>" in response.text
    assert "<th scope=\"col\">Tax year</th>" in response.text
    assert "<th scope=\"col\">Tax field</th>" in response.text
    assert "<th scope=\"col\">Legal entity category</th>" in response.text
    assert "<th scope=\"col\">Original/home currency</th>" in response.text
    assert "<th scope=\"col\">Original/home base value</th>" in response.text
    assert "<th scope=\"col\">Original/home calculated value</th>" in response.text
    assert "<th scope=\"col\">EUR base value</th>" in response.text
    assert "<th scope=\"col\">Multiplier</th>" in response.text
    assert "<th scope=\"col\">EUR calculated value</th>" in response.text
    assert "<th scope=\"col\">FX rate/date</th>" in response.text
    assert "<td>IE00BMTX1Y45</td>" in response.text
    assert "<td>2025</td>" in response.text
    assert "K40 - Taxable income" in response.text
    assert "<td>BVM</td>" in response.text
    assert "<td>USD</td>" in response.text
    assert "<td>11.000</td>" in response.text
    assert "<td>11005.500</td>" in response.text
    assert "<td>10.000</td>" in response.text
    assert "<td>1000.500</td>" in response.text
    assert "<td>10005.000</td>" in response.text
    assert '<span class="numeric-value">1.0000</span>' in response.text
    assert "1.0000000000" not in response.text
    assert "2025-06-15" in response.text
    assert 'formaction="/app/business-query/export"' in response.text
    assert "Export CSV" in response.text
    assert "field-error" not in response.text
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.query_name == "Monthly review"
    assert query.isins == ("IE00BMTX1Y45", "LU1681044993")
    assert query.legal_entity_type == "business"
    assert query.subcategory_key == "business_bv_without_option"
    assert query.tax_year_filter == "2025"
    assert query.tax_fields == ("K40", "K61", "K62")
    assert query.amount_multiplier == Decimal("1000.50")


@pytest.mark.asyncio
async def test_business_query_result_summary_renders_multiple_oekb_release_dates(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Release review",
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
                    amount_multiplier=Decimal("1"),
                    calculated_eur_value=Decimal("10.0000000000"),
                ),
                BusinessQueryResultRow(
                    query_name="Release review",
                    isin="LU1681044993",
                    tax_year=2025,
                    oekb_report_id=1002,
                    fund_currency="EUR",
                    report_date=date(2025, 7, 1),
                    fx_rate=Decimal("1.0000000000"),
                    legal_entity_category="BVM",
                    tax_field_code="K40",
                    tax_field_label="Taxable income",
                    base_eur_value=Decimal("20.0000000000"),
                    amount_multiplier=Decimal("1"),
                    calculated_eur_value=Decimal("20.0000000000"),
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
            "query_name": "Release review",
            "isins": "IE00BMTX1Y45\nLU1681044993",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_without_option",
            "tax_year_filter": "2025",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert "<dt>OeKB release date</dt>" in response.text
    assert "2 dates: 2025-06-15, 2025-07-01" in response.text


@pytest.mark.asyncio
async def test_business_query_post_passes_selected_tax_fields_to_service(
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
        content=urlencode(
            [
                ("query_name", "Selected field review"),
                ("isins", "IE00BMTX1Y45"),
                ("legal_entity_type", "natural person"),
                ("subcategory_key", "natural_person_pa_with_option"),
                ("tax_year_filter", "2025"),
                ("tax_fields", "K11"),
                ("tax_fields", "K61"),
                ("amount", "100"),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 200
    normalized_html = " ".join(response.text.split())
    assert '<input id="tax-field-K11" name="tax_fields" type="checkbox" value="K11" checked >' in normalized_html
    assert '<input id="tax-field-K61" name="tax_fields" type="checkbox" value="K61" checked >' in normalized_html
    assert "<dt>Tax fields</dt>" in response.text
    assert "K11 - AGErtraege" in response.text
    assert "K61 - Korrekturbetrag Anschaffungskosten" in response.text
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.tax_fields == ("K11", "K61")


@pytest.mark.asyncio
async def test_business_query_result_rows_expose_tax_field_metadata(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = tuple(
        BusinessQueryResultRow(
            query_name="Tax metadata",
            isin="IE00BMTX1Y45",
            tax_year=2025,
            oekb_report_id=1001,
            fund_currency="EUR",
            report_date=date(2025, 6, 15),
            fx_rate=Decimal("1.0000000000"),
            legal_entity_category="BVM",
            tax_field_code=tax_line.line_code,
            tax_field_label=tax_line.name_en,
            base_eur_value=Decimal("10.0000000000"),
            amount_multiplier=Decimal("1"),
            calculated_eur_value=Decimal("10.0000000000"),
        )
        for tax_line in TAX_LINES
        if tax_line.line_code in {"K40", "K61", "K62"}
    )

    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(query=query, rows=rows)

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "Tax metadata",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert response.text.count("<summary>Field details</summary>") == 3
    for tax_line in TAX_LINES:
        if tax_line.line_code in {"K40", "K61", "K62"}:
            assert f"<dd>{tax_line.line_code}</dd>" in response.text
            assert f"<dd>{tax_line.name_de}</dd>" in response.text
            assert f"<dd>{tax_line.description}</dd>" in response.text
            assert f"<dd>{tax_line.usage_note}</dd>" in response.text


@pytest.mark.asyncio
async def test_business_query_result_rows_render_when_tax_field_metadata_is_missing(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Missing metadata",
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
                    amount_multiplier=Decimal("1"),
                    calculated_eur_value=Decimal("10.0000000000"),
                ),
            ),
        )

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)
    monkeypatch.setattr(web_routes, "BUSINESS_QUERY_TAX_FIELD_METADATA", {})

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "Missing metadata",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert "<h2>Query results</h2>" in response.text
    assert "<td>IE00BMTX1Y45</td>" in response.text
    assert "<td>BVM</td>" in response.text
    assert "K40 - Taxable income" in response.text
    assert "<summary>Field details</summary>" not in response.text


@pytest.mark.asyncio
async def test_business_query_result_page_formats_numeric_values_for_display(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Formatted values",
                    isin="IE00BMTX1Y45",
                    tax_year=2025,
                    oekb_report_id=1001,
                    fund_currency="EUR",
                    report_date=date(2025, 6, 15),
                    fx_rate=Decimal("1.2345678900"),
                    legal_entity_category="BVM",
                    tax_field_code="K40",
                    tax_field_label="Taxable income",
                    base_eur_value=Decimal("10"),
                    amount_multiplier=Decimal("10.1235"),
                    calculated_eur_value=Decimal("10.1235"),
                    original_currency_code="EUR",
                    home_currency_code="EUR",
                    fx_date=date(2025, 6, 15),
                    base_home_currency_value=Decimal("10"),
                    calculated_home_currency_value=Decimal("10.1235"),
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
            "query_name": "Formatted values",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "amount": "10.1235",
        },
    )

    assert response.status_code == 200
    assert "<dd>10.124</dd>" in response.text
    assert response.text.count("<td>10.000</td>") == 2
    assert response.text.count("<td>10.124</td>") == 3
    assert '<span class="numeric-value">1.2346</span>' in response.text
    assert "1.2345678900" not in response.text


@pytest.mark.asyncio
async def test_business_query_result_page_preserves_missing_numeric_display(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Missing values",
                    isin="IE00BMTX1Y45",
                    tax_year=2025,
                    oekb_report_id=1001,
                    fund_currency="EUR",
                    report_date=date(2025, 6, 15),
                    fx_rate=None,
                    legal_entity_category="BVM",
                    tax_field_code="K40",
                    tax_field_label="Taxable income",
                    base_eur_value=None,
                    amount_multiplier=Decimal("1"),
                    calculated_eur_value=None,
                    original_currency_code="EUR",
                    home_currency_code="EUR",
                    fx_date=date(2025, 6, 15),
                    base_home_currency_value=None,
                    calculated_home_currency_value=None,
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
            "query_name": "Missing values",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert "<td>1.000</td>" in response.text
    assert response.text.count("<td>-</td>") == 4
    assert '<span class="numeric-value">-</span>' in response.text


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
async def test_business_query_missing_year_messages_render_without_hiding_valid_rows(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(
            query=query,
            rows=(
                BusinessQueryResultRow(
                    query_name="Missing year review",
                    isin="IE00BMTX1Y45",
                    tax_year=2025,
                    oekb_report_id=1001,
                    fund_currency="CHF",
                    report_date=date(2025, 6, 15),
                    fx_rate=Decimal("1.0400000000"),
                    legal_entity_category="BVM",
                    tax_field_code="K40",
                    tax_field_label="Taxable income",
                    base_eur_value=Decimal("9.6153846154"),
                    amount_multiplier=Decimal("2"),
                    calculated_eur_value=Decimal("19.2307692308"),
                    original_currency_code="CHF",
                    home_currency_code="CHF",
                    fx_date=date(2025, 6, 15),
                    base_home_currency_value=Decimal("10.0000000000"),
                    calculated_home_currency_value=Decimal("20.0000000000"),
                ),
            ),
            missing_year_isins=("LU1681044993",),
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
            "query_name": "Missing year review",
            "isins": "IE00BMTX1Y45\nLU1681044993",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "amount": "2",
        },
    )

    assert response.status_code == 200
    assert "Data for ISIN LU1681044993 is not available for the selected year." in response.text
    assert "field-error" not in response.text
    assert "<td>IE00BMTX1Y45</td>" in response.text
    assert "<td>CHF</td>" in response.text
    assert "<td>10.000</td>" in response.text
    assert "<td>20.000</td>" in response.text
    assert "<td>9.615</td>" in response.text
    assert "<td>19.231</td>" in response.text
    assert '<span class="numeric-value">1.0400</span>' in response.text
    assert "1.0400000000" not in response.text
    assert "2025-06-15" in response.text


@pytest.mark.asyncio
async def test_business_query_missing_year_messages_render_with_empty_results(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute_business_query(
        session: object,
        query: BusinessQueryInput,
    ) -> BusinessQueryResult:
        return BusinessQueryResult(query=query, rows=(), missing_year_isins=("IE00BMTX1Y45",))

    monkeypatch.setattr(web_routes, "execute_business_query", fake_execute_business_query)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "Only missing",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "amount": "1",
        },
    )

    assert response.status_code == 200
    assert "Data for ISIN IE00BMTX1Y45 is not available for the selected year." in response.text
    assert "No tax rows matched the submitted ISINs." in response.text


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
                    original_currency_code="CHF",
                    home_currency_code="CHF",
                    fx_date=date(2025, 6, 15),
                    base_home_currency_value=Decimal("10.9000000000"),
                    calculated_home_currency_value=Decimal("10905.450000000000"),
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
            "subcategory_key": "business_bv_legal_person",
            "tax_year_filter": "2025",
            "amount": "1000.50",
        },
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    csv_rows = list(csv.DictReader(StringIO(response.text)))
    assert csv_rows == [
        {
            "query_name": "Monthly review",
            "isin": "IE00BMTX1Y45",
            "tax_year": "2025",
            "tax_field_code": "K40",
            "tax_field_label": "Taxable income",
            "legal_entity_category": "BVM",
            "original_home_currency": "CHF",
            "base_home_currency_value": "10.9000000000",
            "calculated_home_currency_value": "10905.450000000000",
            "base_eur_value": "10.0000000000",
            "amount_multiplier": "1000.50",
            "calculated_eur_value": "10005.000000000000",
            "fx_rate": "1.0000000000",
            "fx_date": "2025-06-15",
        }
    ]
    assert csv_rows[0].keys() == {
        "query_name",
        "isin",
        "tax_year",
        "tax_field_code",
        "tax_field_label",
        "legal_entity_category",
        "original_home_currency",
        "base_home_currency_value",
        "calculated_home_currency_value",
        "base_eur_value",
        "amount_multiplier",
        "calculated_eur_value",
        "fx_rate",
        "fx_date",
    }
    assert 'filename="business-query.csv"' in response.headers["content-disposition"]
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.query_name == "Monthly review"
    assert query.isins == ("IE00BMTX1Y45", "LU1681044993")
    assert query.legal_entity_type == "business"
    assert query.subcategory_key == "business_bv_legal_person"
    assert query.tax_year_filter == "2025"
    assert query.amount_multiplier == Decimal("1000.50")


@pytest.mark.asyncio
async def test_business_query_export_uses_current_submitted_fields_after_saved_query_load(
    saved_query_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = saved_query_client
    async with session_factory() as session:
        saved_query = BQSAVED(
            owner_username="admin",
            query_name="Saved export rule",
            legal_entity_type="business",
            subcategory_key="business_bv_with_option",
            tax_year_filter="2025",
            amount=Decimal("100.00"),
            default_isins=["IE00BMTX1Y45"],
            selected_tax_fields=["K40"],
        )
        session.add(saved_query)
        await session.commit()
        saved_query_id = saved_query.id

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

    load_response = await web_client.get(f"/app/business-query/saved/{saved_query_id}/load")
    assert load_response.status_code == 200
    normalized_load_html = " ".join(load_response.text.split())
    assert '<input id="tax-field-K40" name="tax_fields" type="checkbox" value="K40" checked >' in normalized_load_html

    response = await web_client.post(
        "/app/business-query/export",
        content=urlencode(
            [
                ("query_name", "Current export rule"),
                ("isins", "LU1681044993"),
                ("legal_entity_type", "natural person"),
                ("subcategory_key", "natural_person_pa_without_option"),
                ("tax_year_filter", "all_available_years"),
                ("tax_fields_present", "1"),
                ("tax_fields", "K11"),
                ("tax_fields", "K61"),
                ("amount", "777.25"),
            ]
        ),
        headers={"content-type": "application/x-www-form-urlencoded"},
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.text == (
        "query_name,isin,tax_year,tax_field_code,tax_field_label,"
        "legal_entity_category,original_home_currency,base_home_currency_value,"
        "calculated_home_currency_value,base_eur_value,amount_multiplier,"
        "calculated_eur_value,fx_rate,fx_date\n"
    )
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.query_name == "Current export rule"
    assert query.isins == ("LU1681044993",)
    assert query.legal_entity_type == "natural person"
    assert query.subcategory_key == "natural_person_pa_without_option"
    assert query.tax_year_filter == "all_available_years"
    assert query.tax_fields == ("K11", "K61")
    assert query.amount_multiplier == Decimal("777.25")


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
        "legal_entity_category,original_home_currency,base_home_currency_value,"
        "calculated_home_currency_value,base_eur_value,amount_multiplier,"
        "calculated_eur_value,fx_rate,fx_date\n"
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
async def test_business_query_invalid_tax_field_submission_is_rejected_before_service(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_if_called(session: object, query: BusinessQueryInput) -> BusinessQueryResult:
        raise AssertionError("invalid BusinessQuery tax field POST must not call the service")

    monkeypatch.setattr(web_routes, "execute_business_query", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "Invalid tax field",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "tax_fields": "SQL",
            "amount": "100",
        },
    )

    assert response.status_code == 200
    assert "Choose supported tax fields." in response.text
    assert "<h2>Query results</h2>" not in response.text


@pytest.mark.asyncio
async def test_business_query_empty_rendered_tax_field_selection_is_rejected_before_service(
    web_client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail_if_called(session: object, query: BusinessQueryInput) -> BusinessQueryResult:
        raise AssertionError("empty rendered BusinessQuery tax field POST must not call the service")

    monkeypatch.setattr(web_routes, "execute_business_query", fail_if_called)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/business-query",
        data={
            "query_name": "Empty tax fields",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "business_bv_with_option",
            "tax_year_filter": "2025",
            "tax_fields_present": "1",
            "amount": "100",
        },
    )

    assert response.status_code == 200
    assert "Choose at least one tax field." in response.text
    assert "<h2>Query results</h2>" not in response.text


@pytest.mark.asyncio
async def test_business_query_invalid_subcategory_and_tax_year_show_validation_errors(
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
            "query_name": "Invalid filters",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "business",
            "subcategory_key": "natural_person_all",
            "tax_year_filter": "3001",
            "amount": "100",
        },
    )

    assert response.status_code == 200
    assert 'value="Invalid filters"' in response.text
    assert ">IE00BMTX1Y45</textarea>" in response.text
    assert '<option value="business" selected>business</option>' in response.text
    assert '<option value="natural_person_all" selected>All private investor categories</option>' in response.text
    assert "Choose a category that matches the selected legal entity type." in response.text
    assert "Choose All available years or one of the listed tax years." in response.text
    assert "<h2>Query results</h2>" not in response.text


@pytest.mark.asyncio
async def test_business_query_stiftung_uses_fixed_subcategory_workflow(
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
            "query_name": "Stiftung review",
            "isins": "IE00BMTX1Y45",
            "legal_entity_type": "Stiftung",
            "subcategory_key": "stiftung",
            "tax_year_filter": "all_available_years",
            "amount": "100",
        },
    )

    assert response.status_code == 200
    assert '<option value="Stiftung" selected>Stiftung</option>' in response.text
    assert '<input type="hidden" name="subcategory_key" value="stiftung">' in response.text
    assert 'id="subcategory-key"' not in response.text
    assert "All private investor categories" not in response.text
    assert "All business categories" not in response.text
    assert '<option value="business_bv_with_option">BV mit Option</option>' not in response.text
    assert "Stiftung uses the fixed Stiftung category." in response.text
    assert "<dt>Subcategory</dt>" in response.text
    assert "<dd>Stiftung</dd>" in response.text
    assert "<dt>Selected tax year</dt>" in response.text
    assert "<dd>All available years</dd>" in response.text
    assert len(service_calls) == 1
    query = service_calls[0][1]
    assert query.legal_entity_type == "Stiftung"
    assert query.subcategory_key == "stiftung"
    assert query.tax_year_filter == "all_available_years"


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
    assert 'href="/app/business-query/new?isins=IE00BMTX1Y45"' in response.text
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
    assert (
        "Valid submissions are queued and processing is started automatically by the web service."
        in response.text
    )
    assert "The job table is the source of truth for completion" in response.text
    assert (
        "the page does not treat a submitted request as finished until the persisted job status "
        "says success"
    ) in response.text
    assert "<strong>queued</strong> means accepted and waiting for or starting processing." in response.text
    assert "<strong>running</strong> means currently being processed." in response.text
    assert "<strong>success</strong> means finished successfully." in response.text
    assert (
        "<strong>failed</strong> means processing failed and error detail should be reviewed."
        in response.text
    )
    assert "python -m fondant.jobs.run_update_data_jobs --limit 10" in response.text
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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    background_calls: list[int] = []

    async def fake_run_queued_update_jobs(*, limit: int = 10) -> None:
        background_calls.append(limit)

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", fake_run_queued_update_jobs)

    response = await web_client.post("/app/update-data", data={"isins": "IE00BMTX1Y45"})

    assert response.status_code == 303
    assert response.headers["location"] == "/login"
    assert background_calls == []


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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = update_data_job_client
    background_calls: list[int] = []

    async def fake_run_queued_update_jobs(*, limit: int = 10) -> None:
        background_calls.append(limit)

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", fake_run_queued_update_jobs)

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
    assert background_calls == []


@pytest.mark.asyncio
async def test_update_data_background_trigger_failure_is_logged_and_contained(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    web_client, session_factory = update_data_job_client
    background_calls: list[int] = []

    async def failing_run_queued_update_jobs(*, limit: int = 10) -> None:
        background_calls.append(limit)
        raise RuntimeError("runner unavailable")

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", failing_run_queued_update_jobs)

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    with caplog.at_level(logging.ERROR, logger=web_routes.logger.name):
        response = await web_client.post("/app/update-data", data={"isins": "IE00BMTX1Y45"})

    assert response.status_code == 200
    assert "<h2>Update job status</h2>" in response.text
    assert '<span class="status-pill status-queued">queued</span>' in response.text
    assert background_calls == [10]
    assert "Background update-data job execution failed." in caplog.text
    assert "runner unavailable" in caplog.text

    async with session_factory() as session:
        jobs = (await session.scalars(select(INGJOB).order_by(INGJOB.id))).all()

    assert [job.isin for job in jobs] == ["IE00BMTX1Y45"]
    assert [job.status for job in jobs] == ["queued"]
    assert [job.message for job in jobs] == ["Queued for update."]


@pytest.mark.asyncio
async def test_update_data_valid_post_creates_queued_jobs_and_shows_status(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = update_data_job_client
    background_calls: list[int] = []

    async def fake_run_queued_update_jobs(*, limit: int = 10) -> None:
        background_calls.append(limit)

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", fake_run_queued_update_jobs)

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
    assert background_calls == [10]


@pytest.mark.asyncio
async def test_update_data_valid_post_skips_duplicate_active_jobs(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = update_data_job_client
    background_calls: list[int] = []

    async def fake_run_queued_update_jobs(*, limit: int = 10) -> None:
        background_calls.append(limit)

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", fake_run_queued_update_jobs)

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
    assert background_calls == [10]


@pytest.mark.asyncio
async def test_update_data_duplicate_only_valid_post_does_not_schedule_background_trigger(
    update_data_job_client: tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    web_client, session_factory = update_data_job_client
    background_calls: list[int] = []

    async def fake_run_queued_update_jobs(*, limit: int = 10) -> None:
        background_calls.append(limit)

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", fake_run_queued_update_jobs)

    async with session_factory() as session:
        session.add_all(
            [
                INGJOB(
                    isin="IE00BMTX1Y45",
                    requested_user="prior-user",
                    status="queued",
                    message="Already queued.",
                ),
                INGJOB(
                    isin="LU1681044993",
                    requested_user="prior-user",
                    status="running",
                    message="Already running.",
                ),
            ]
        )
        await session.commit()

    login_response = await web_client.post(
        "/login",
        data={"username": "admin", "password": "password"},
    )
    assert login_response.status_code == 303

    response = await web_client.post(
        "/app/update-data",
        data={"isins": "IE00BMTX1Y45 LU1681044993"},
    )

    assert response.status_code == 200
    assert "<h2>Update job status</h2>" in response.text
    assert '<span class="status-pill status-skipped">skipped</span>' in response.text
    assert "Skipped: active update job already exists." in response.text

    async with session_factory() as session:
        jobs = (
            await session.scalars(select(INGJOB).order_by(INGJOB.id))
        ).all()

    assert [job.isin for job in jobs] == ["IE00BMTX1Y45", "LU1681044993"]
    assert [job.status for job in jobs] == ["queued", "running"]
    assert background_calls == []


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
    assert "workspace-panel documentation-workspace" in response.text
    assert "placeholder-panel documentation-panel" in response.text
    assert "result-table-wrap documentation-table-wrap" in response.text
    assert "result-table documentation-tax-field-table" in response.text
    assert "Quick reference for authenticated BusinessQuery and Search use." in response.text
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
    assert "PA mit Option means private assets with option" in response.text
    assert "PA ohne Option means private assets without option" in response.text
    assert "BV mit Option means business assets with option" in response.text
    assert "BV ohne Option means business assets without option" in response.text
    assert "BV jur. Person means business assets for a legal person" in response.text
    assert "Stiftung means foundation assets" in response.text
    assert (
        "Original/home currency is the currency of the fund or source tax data."
        in response.text
    )
    assert "EUR is the converted reporting value." in response.text
    assert (
        "The FX rate and FX date show the conversion used to trace each "
        "original/home currency value into EUR."
        in response.text
    )
    assert "Choose a specific tax year to filter results to that year." in response.text
    assert (
        "Choose All available years to query every available tax year."
        in response.text
    )
    assert (
        "Data for ISIN {ISIN} is not available for the selected year."
        in response.text
    )
    assert "Select one or more tax fields when building a BusinessQuery." in response.text
    assert "K11 - AG Ertraege" in response.text
    assert (
        "Use Add New Query to enter ISINs, select tax fields, run a query, "
        "and save a reusable rule."
    ) in response.text
    assert (
        "Use Queries to run saved queries directly with their default ISINs, "
        "or Load and Edit existing rules."
    ) in response.text
    assert (
        "Use Group BusinessQuery to create named groups, then assign saved "
        "queries to those groups while editing."
    ) in response.text
    assert "amount multiplier" in response.text
    assert "CSV exports include" in response.text
    assert "original/home currency values, EUR values" in response.text
    assert "FX rate, and FX date" in response.text
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
