from __future__ import annotations

import httpx
import pytest

from fondant.api.main import create_app


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
async def test_web_routes_preserve_existing_health_contract(web_client: httpx.AsyncClient) -> None:
    response = await web_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
