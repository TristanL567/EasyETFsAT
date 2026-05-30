from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.db.models import INGJOB
from fondant.db.session import AsyncSessionFactory
from fondant.ingestion.pipeline import ingest_isin


@dataclass(frozen=True)
class UpdateDataResult:
    isin: str
    status: str
    records_seen: int
    records_written: int
    message: str
    error: str | None = None


@dataclass(frozen=True)
class UpdateJobRunSummary:
    processed: int
    successes: int
    failures: int
    skipped: int


async def update_single_isin(isin: str) -> UpdateDataResult:
    """Run update-data ingestion for one already normalized and validated ISIN."""
    try:
        ingestion_result = await ingest_isin(isin)
    except Exception as exc:
        error = str(exc) or type(exc).__name__
        return UpdateDataResult(
            isin=isin,
            status="failed",
            records_seen=0,
            records_written=0,
            message=f"Ingestion failed for {isin}: {error}",
            error=error,
        )

    status = ingestion_result.status.lower()
    if status == "success":
        return UpdateDataResult(
            isin=ingestion_result.isin,
            status="success",
            records_seen=ingestion_result.records_seen,
            records_written=ingestion_result.records_written,
            message=ingestion_result.message or "Ingestion completed.",
        )

    message = ingestion_result.message or f"Ingestion returned status {ingestion_result.status}."
    return UpdateDataResult(
        isin=ingestion_result.isin,
        status="failed",
        records_seen=ingestion_result.records_seen,
        records_written=ingestion_result.records_written,
        message=message,
        error=message,
    )


async def run_next_update_job(session: AsyncSession) -> INGJOB | None:
    """Process the oldest queued update-data job, if one exists."""
    job = await session.scalar(
        select(INGJOB)
        .where(INGJOB.status == "queued")
        .order_by(INGJOB.id)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    if job is None:
        return None

    job.status = "running"
    job.started_at = datetime.now(timezone.utc)
    job.message = "Running update."
    job.error_detail = None
    job.finished_at = None
    await session.commit()

    try:
        result = await update_single_isin(job.isin)
    except Exception as exc:
        error = str(exc) or type(exc).__name__
        result = UpdateDataResult(
            isin=job.isin,
            status="failed",
            records_seen=0,
            records_written=0,
            message=f"Update job failed for {job.isin}: {error}",
            error=error,
        )

    job.status = "success" if result.status == "success" else "failed"
    job.message = result.message
    job.error_detail = None if job.status == "success" else result.error or result.message
    job.finished_at = datetime.now(timezone.utc)
    await session.commit()
    await session.refresh(job)
    return job


async def run_update_jobs(session: AsyncSession, limit: int = 1) -> UpdateJobRunSummary:
    """Process queued update-data jobs up to the provided non-negative limit."""
    processed = 0
    successes = 0
    failures = 0

    for _ in range(max(limit, 0)):
        job = await run_next_update_job(session)
        if job is None:
            break

        processed += 1
        if job.status == "success":
            successes += 1
        elif job.status == "failed":
            failures += 1

    return UpdateJobRunSummary(
        processed=processed,
        successes=successes,
        failures=failures,
        skipped=0 if processed else 1,
    )


async def run_queued_update_jobs(limit: int = 10) -> UpdateJobRunSummary:
    """Open an app database session and process queued update-data jobs."""
    async with AsyncSessionFactory() as session:
        return await run_update_jobs(session, limit=limit)
