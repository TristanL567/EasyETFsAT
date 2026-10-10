from __future__ import annotations

import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone

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
from fondant.config import Settings, get_settings
from fondant.db.base import Base
from fondant.db.models import INGJOB, SOURCERPT
from fondant.db.session import get_session

TOKEN = "test-token-for-update-jobs"
AUTH = {"X-EasyETFsAT-Token": TOKEN}


async def _client(
    token: str | None,
    runner_calls: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]]]:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    async def fake_run_queued_update_jobs(*, limit: int = 10) -> None:
        runner_calls.append(limit)

    monkeypatch.setattr(web_routes, "run_queued_update_jobs", fake_run_queued_update_jobs)

    app = create_app()

    async def _override_session() -> AsyncIterator[AsyncSession]:
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = _override_session
    app.dependency_overrides[get_settings] = lambda: Settings(easyetfsat_api_token=token)
    client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
    try:
        yield client, session_factory
    finally:
        await client.aclose()
        app.dependency_overrides.clear()
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest.fixture
def runner_calls() -> list[int]:
    return []


@pytest.fixture
async def api(
    runner_calls: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]]]:
    async for pair in _client(TOKEN, runner_calls, monkeypatch):
        yield pair


@pytest.fixture
async def api_without_token(
    runner_calls: list[int],
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]]]:
    async for pair in _client(None, runner_calls, monkeypatch):
        yield pair


async def _jobs(session_factory: async_sessionmaker[AsyncSession]) -> list[INGJOB]:
    async with session_factory() as session:
        return list((await session.scalars(select(INGJOB).order_by(INGJOB.id))).all())


@pytest.mark.asyncio
async def test_unconfigured_token_returns_503_on_every_route(api_without_token) -> None:
    client, _ = api_without_token

    assert (
        await client.post("/api/update-jobs", json={"isins": ["IE00BMTX1Y45"]}, headers=AUTH)
    ).status_code == 503
    assert (await client.get("/api/update-jobs?ids=1", headers=AUTH)).status_code == 503
    assert (await client.get("/api/update-jobs/1", headers=AUTH)).status_code == 503


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "headers", [{}, {"X-EasyETFsAT-Token": "wrong"}, {"X-EasyETFsAT-Token": ""}]
)
async def test_missing_or_wrong_token_returns_401_and_queues_nothing(
    api, runner_calls, headers
) -> None:
    client, session_factory = api

    post = await client.post("/api/update-jobs", json={"isins": ["IE00BMTX1Y45"]}, headers=headers)
    assert post.status_code == 401
    assert (await client.get("/api/update-jobs?ids=1", headers=headers)).status_code == 401
    assert (await client.get("/api/update-jobs/1", headers=headers)).status_code == 401
    assert await _jobs(session_factory) == []
    assert runner_calls == []


@pytest.mark.asyncio
async def test_auth_is_checked_before_the_body(api) -> None:
    client, _ = api

    response = await client.post("/api/update-jobs", content=b"not json")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_session_cookie_does_not_authenticate(api) -> None:
    client, _ = api

    login = await client.post("/login", data={"username": "admin", "password": "password"})
    assert login.status_code == 303

    response = await client.post("/api/update-jobs", json={"isins": ["IE00BMTX1Y45"]})

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_post_isins_queues_valid_rejects_invalid_and_starts_runner(api, runner_calls) -> None:
    client, session_factory = api

    response = await client.post(
        "/api/update-jobs",
        json={
            "isins": ["ie00bmtx1y45", "IE00BMTX1Y46", "LU1681044993", "IE00BMTX1Y45", "nonsense"]
        },
        headers=AUTH,
    )

    assert response.status_code == 202
    body = response.json()
    assert body["jobs"] == [
        {"id": 1, "isin": "IE00BMTX1Y45", "status": "queued"},
        {"id": 2, "isin": "LU1681044993", "status": "queued"},
    ]
    assert body["rejected"] == [
        {"value": "IE00BMTX1Y46", "reason": "invalid_isin"},
        {"value": "nonsense", "reason": "invalid_isin"},
    ]
    assert body["skipped"] == []

    jobs = await _jobs(session_factory)
    assert [(job.isin, job.status, job.requested_user) for job in jobs] == [
        ("IE00BMTX1Y45", "queued", "easyrep"),
        ("LU1681044993", "queued", "easyrep"),
    ]
    assert runner_calls == [10]


@pytest.mark.asyncio
async def test_post_skips_isin_with_active_job_and_returns_its_id(api, runner_calls) -> None:
    client, session_factory = api
    async with session_factory() as session:
        session.add(INGJOB(isin="IE00BMTX1Y45", requested_user="admin", status="running"))
        await session.commit()

    response = await client.post("/api/update-jobs", json={"isins": ["IE00BMTX1Y45"]}, headers=AUTH)

    assert response.status_code == 202
    assert response.json() == {
        "jobs": [],
        "rejected": [],
        "skipped": [{"id": 1, "isin": "IE00BMTX1Y45", "reason": "active_job"}],
    }
    assert len(await _jobs(session_factory)) == 1
    assert runner_calls == []


@pytest.mark.asyncio
async def test_post_scope_existing_queues_every_sourcerpt_isin(api, runner_calls) -> None:
    client, session_factory = api
    async with session_factory() as session:
        session.add_all(
            [
                SOURCERPT(isin="LU1681044993", stm_id=1),
                SOURCERPT(isin="IE00BMTX1Y45", stm_id=2),
                SOURCERPT(isin="IE00BMTX1Y45", stm_id=3),
            ]
        )
        await session.commit()

    response = await client.post("/api/update-jobs", json={"scope": "existing"}, headers=AUTH)

    assert response.status_code == 202
    assert [job["isin"] for job in response.json()["jobs"]] == ["IE00BMTX1Y45", "LU1681044993"]
    assert [job.requested_user for job in await _jobs(session_factory)] == ["easyrep", "easyrep"]
    assert runner_calls == [10]


@pytest.mark.asyncio
async def test_runner_limit_covers_every_queued_job(api, runner_calls) -> None:
    client, _ = api
    isins = [f"US{n:09d}" for n in range(200)]
    valid = [isin + str(_check_digit(isin)) for isin in isins][:12]

    response = await client.post("/api/update-jobs", json={"isins": valid}, headers=AUTH)

    assert response.status_code == 202
    assert len(response.json()["jobs"]) == 12
    assert runner_calls == [12]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"isins": [f"X{n}" for n in range(51)]},
        {"isins": []},
        {"isins": "IE00BMTX1Y45"},
        {"isins": [1]},
        {"isins": ["IE00BMTX1Y45"], "scope": "existing"},
        {"scope": "all"},
        {},
        ["IE00BMTX1Y45"],
        {"isins": ["\ud800"]},
        {"isins": ["\udc00x"]},
        {"isins": ["IE00BMTX1Y45", "\ud800"]},
    ],
)
async def test_post_invalid_body_returns_422(api, runner_calls, payload) -> None:
    client, session_factory = api

    # json.dumps escapes non-ASCII and lone surrogates, as a real client sends them.
    response = await client.post(
        "/api/update-jobs",
        content=json.dumps(payload),
        headers={**AUTH, "Content-Type": "application/json"},
    )

    assert response.status_code == 422
    assert await _jobs(session_factory) == []
    assert runner_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [b"isins=IE00BMTX1Y45", b"[" * 100000, b'{"a":' * 50000],
    ids=["form-encoded", "deeply-nested-array", "deeply-nested-object"],
)
async def test_post_non_json_body_returns_422(api, content) -> None:
    client, _ = api

    response = await client.post("/api/update-jobs", content=content, headers=AUTH)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_jobs_by_ids_returns_status_fields_in_iso_utc(api) -> None:
    client, session_factory = api
    async with session_factory() as session:
        session.add_all(
            [
                INGJOB(
                    isin="IE00BMTX1Y45",
                    requested_user="easyrep",
                    status="success",
                    message="Processed 6 FIN reports; wrote 1.",
                    started_at=datetime(2026, 10, 10, 9, 0, 1, tzinfo=timezone.utc),
                    finished_at=datetime(2026, 10, 10, 9, 0, 9, tzinfo=timezone.utc),
                ),
                INGJOB(
                    isin="LU1681044993",
                    requested_user="easyrep",
                    status="failed",
                    message="Ingestion failed.",
                    error_detail="HTTP 503 from OeKB",
                ),
            ]
        )
        await session.commit()

    response = await client.get("/api/update-jobs?ids=2, 1,99,2", headers=AUTH)

    assert response.status_code == 200
    body = response.json()
    assert [job["id"] for job in body["jobs"]] == [2, 1]
    assert body["not_found"] == [99]
    first = body["jobs"][1]
    assert first == {
        "id": 1,
        "isin": "IE00BMTX1Y45",
        "status": "success",
        "message": "Processed 6 FIN reports; wrote 1.",
        "error": None,
        "created": first["created"],
        "started": "2026-10-10T09:00:01Z",
        "finished": "2026-10-10T09:00:09Z",
    }
    assert first["created"].endswith("Z")
    assert datetime.fromisoformat(first["created"].replace("Z", "+00:00")).tzinfo is not None
    second = body["jobs"][0]
    assert (second["status"], second["error"], second["started"], second["finished"]) == (
        "failed",
        "HTTP 503 from OeKB",
        None,
        None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        "?ids=%C2%B2",
        "?ids=0",
        "?ids=2147483648",
        "?ids=99999999999999999999",
        "?ids=" + "1" * 5000,
        "?ids=" + "0" * 5000 + "1",
        "",
        "?ids=",
        "?ids=a,1",
        "?ids=-1",
        "?ids=" + ",".join(str(n) for n in range(101)),
    ],
)
async def test_get_jobs_invalid_ids_returns_422(api, query) -> None:
    client, _ = api

    response = await client.get(f"/api/update-jobs{query}", headers=AUTH)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_single_job_and_404(api) -> None:
    client, _ = api
    created = await client.post("/api/update-jobs", json={"isins": ["IE00BMTX1Y45"]}, headers=AUTH)
    job_id = created.json()["jobs"][0]["id"]

    response = await client.get(f"/api/update-jobs/{job_id}", headers=AUTH)
    missing = await client.get("/api/update-jobs/999", headers=AUTH)

    assert response.status_code == 200
    assert response.json()["isin"] == "IE00BMTX1Y45"
    assert response.json()["status"] == "queued"
    assert missing.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("job_id", ["0", "2147483648", "99999999999999999999", "abc"])
async def test_get_single_job_out_of_range_id_returns_422(api, job_id) -> None:
    client, _ = api

    response = await client.get(f"/api/update-jobs/{job_id}", headers=AUTH)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_single_job_checks_token_before_path(api) -> None:
    client, _ = api

    response = await client.get("/api/update-jobs/abc")

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_web_form_still_queues_with_its_own_user(api, runner_calls) -> None:
    client, session_factory = api
    await client.post("/login", data={"username": "admin", "password": "password"})

    response = await client.post("/app/update-data", data={"isins": "IE00BMTX1Y45"})

    assert response.status_code == 200
    assert [(job.isin, job.requested_user) for job in await _jobs(session_factory)] == [
        ("IE00BMTX1Y45", "admin")
    ]
    assert runner_calls == [10]


def _check_digit(body: str) -> int:
    digits = "".join(str(int(char, 36)) for char in body)
    total = 0
    for index, char in enumerate(reversed(digits)):
        value = int(char)
        if index % 2 == 0:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return (10 - total % 10) % 10
