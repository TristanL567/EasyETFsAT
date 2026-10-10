"""Token-authenticated JSON API for queueing update-data jobs and reading their status.

Used by EasyRep. Auth is the `X-EasyETFsAT-Token` header, compared with the
EASYETFSAT_API_TOKEN setting; there is no session cookie on these routes.
"""

from __future__ import annotations

import hmac
import json
import re
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Path, Request
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.api.routes.web import (
    BACKGROUND_UPDATE_DATA_JOB_LIMIT,
    _has_valid_isin_checksum,
    _queue_update_data_jobs,
    _run_queued_update_jobs_background,
)
from fondant.config import Settings, get_settings
from fondant.db.models import INGJOB, SOURCERPT
from fondant.db.session import get_session

API_TOKEN_HEADER = "X-EasyETFsAT-Token"
API_REQUESTED_USER = "easyrep"
MAX_ISINS_PER_REQUEST = 50
MAX_JOB_IDS_PER_REQUEST = 100
# INGJOB.JOBIDN is a 32-bit integer; larger ids would fail in the database.
MAX_JOB_ID = 2**31 - 1
JOB_ID_PATTERN = re.compile(r"[0-9]{1,10}")


def require_api_token(
    settings: Annotated[Settings, Depends(get_settings)],
    token: Annotated[str | None, Header(alias=API_TOKEN_HEADER)] = None,
) -> None:
    expected = settings.easyetfsat_api_token
    if not expected:
        raise HTTPException(status_code=503, detail="API token is not configured.")
    if token is None or not hmac.compare_digest(token.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Missing or invalid API token.")


router = APIRouter(
    prefix="/api/update-jobs",
    tags=["update-jobs"],
    dependencies=[Depends(require_api_token)],
)


@router.post("", status_code=202)
async def create_update_jobs(
    request: Request,
    background_tasks: BackgroundTasks,
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, list[dict[str, Any]]]:
    # The body is parsed here, after the token check, so a caller without a
    # valid token always gets 401/503 and never a body validation error.
    payload = await _read_json_object(request)
    raw_values = await _requested_values(payload, session)

    rejected: list[dict[str, Any]] = []
    valid_isins: list[str] = []
    for raw in raw_values:
        isin = raw.strip().upper()
        if not _has_valid_isin_checksum(isin):
            rejected.append({"value": raw, "reason": "invalid_isin"})
        elif isin not in valid_isins:
            valid_isins.append(isin)

    jobs: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    if valid_isins:
        results = await _queue_update_data_jobs(session, tuple(valid_isins), API_REQUESTED_USER)
        for result in results:
            if result["status"] == "queued":
                jobs.append({"id": int(result["id"]), "isin": result["isin"], "status": "queued"})
            else:
                skipped.append(
                    {"id": int(result["id"]), "isin": result["isin"], "reason": "active_job"}
                )

    if jobs:
        queued_total = await session.scalar(
            select(func.count()).select_from(INGJOB).where(INGJOB.status == "queued")
        )
        background_tasks.add_task(
            _run_queued_update_jobs_background,
            limit=max(BACKGROUND_UPDATE_DATA_JOB_LIMIT, int(queued_total or 0)),
        )

    return {"jobs": jobs, "rejected": rejected, "skipped": skipped}


@router.get("")
async def list_update_jobs(
    session: Annotated[AsyncSession, Depends(get_session)],
    ids: str | None = None,
) -> dict[str, list[Any]]:
    job_ids = _parse_job_ids(ids)
    found = {
        job.id: job
        for job in (await session.scalars(select(INGJOB).where(INGJOB.id.in_(job_ids)))).all()
    }
    return {
        "jobs": [_job_payload(found[job_id]) for job_id in job_ids if job_id in found],
        "not_found": [job_id for job_id in job_ids if job_id not in found],
    }


@router.get("/{job_id}")
async def get_update_job(
    job_id: Annotated[int, Path(ge=1, le=MAX_JOB_ID)],
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    job = await session.get(INGJOB, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Update job not found.")
    return _job_payload(job)


async def _read_json_object(request: Request) -> dict[str, Any]:
    try:
        payload = json.loads(await request.body())
    except (ValueError, RecursionError):
        raise HTTPException(status_code=422, detail="Body must be a JSON object.") from None
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="Body must be a JSON object.")
    return payload


async def _requested_values(payload: dict[str, Any], session: AsyncSession) -> list[str]:
    if ("isins" in payload) == ("scope" in payload):
        raise HTTPException(status_code=422, detail='Send exactly one of "isins" or "scope".')

    if "scope" in payload:
        if payload["scope"] != "existing":
            raise HTTPException(status_code=422, detail='"scope" must be "existing".')
        rows = await session.scalars(select(SOURCERPT.isin).distinct().order_by(SOURCERPT.isin))
        return [isin for isin in rows.all() if isin]

    isins = payload["isins"]
    if not isinstance(isins, list) or not all(isinstance(value, str) for value in isins):
        raise HTTPException(status_code=422, detail='"isins" must be a list of strings.')
    if not all(_is_utf8_encodable(value) for value in isins):
        # JSON allows lone surrogate escapes, which cannot be echoed back in the response.
        raise HTTPException(status_code=422, detail='"isins" must hold valid Unicode strings.')
    if not isins:
        raise HTTPException(status_code=422, detail='"isins" must not be empty.')
    if len(isins) > MAX_ISINS_PER_REQUEST:
        raise HTTPException(
            status_code=422,
            detail=f'"isins" may hold at most {MAX_ISINS_PER_REQUEST} values.',
        )
    return isins


def _is_utf8_encodable(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _parse_job_ids(raw: str | None) -> list[int]:
    parts = [part.strip() for part in (raw or "").split(",") if part.strip()]
    if not parts:
        raise HTTPException(status_code=422, detail='"ids" must list at least one job id.')
    if not all(JOB_ID_PATTERN.fullmatch(part) and 1 <= int(part) <= MAX_JOB_ID for part in parts):
        raise HTTPException(
            status_code=422,
            detail=f'"ids" must be comma-separated job ids between 1 and {MAX_JOB_ID}.',
        )
    job_ids = list(dict.fromkeys(int(part) for part in parts))
    if len(job_ids) > MAX_JOB_IDS_PER_REQUEST:
        raise HTTPException(
            status_code=422,
            detail=f'"ids" may hold at most {MAX_JOB_IDS_PER_REQUEST} job ids.',
        )
    return job_ids


def _job_payload(job: INGJOB) -> dict[str, Any]:
    return {
        "id": job.id,
        "isin": job.isin,
        "status": job.status,
        "message": job.message,
        "error": job.error_detail,
        "created": _iso_utc(job.created_at),
        "started": _iso_utc(job.started_at),
        "finished": _iso_utc(job.finished_at),
    }


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
