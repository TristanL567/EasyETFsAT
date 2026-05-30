from __future__ import annotations

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from fondant.api.routes.web import _queue_update_data_jobs
from fondant.db.base import Base
from fondant.db.models import ACTIVE_UPDATE_DATA_JOB_STATUSES, INGJOB, UPDATE_DATA_JOB_STATUSES


def test_ingjob_model_shape_imports_cleanly() -> None:
    table = INGJOB.__table__

    assert table.name == "INGJOB"
    assert table.c.JOBIDN.primary_key
    assert table.c.JOBISN.nullable is False
    assert table.c.JOBSTS.nullable is False
    assert table.c.JOBREQUSR.nullable is True
    assert table.c.JOBMSG.nullable is True
    assert table.c.JOBERR.nullable is True
    assert table.c.JOBSTADTS.nullable is True
    assert table.c.JOBFINDTS.nullable is True
    assert UPDATE_DATA_JOB_STATUSES == ("queued", "running", "success", "failed", "skipped", "cancelled")
    assert ACTIVE_UPDATE_DATA_JOB_STATUSES == ("queued", "running")
    assert "ix_ingjob_isin_status" in {index.name for index in table.indexes}


def test_active_for_isin_identifies_duplicate_active_jobs() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    try:
        with Session(engine) as session:
            session.add_all(
                [
                    INGJOB(isin="IE00BMTX1Y45", status="queued", message="queued request"),
                    INGJOB(isin="IE00BMTX1Y45", status="running", message="running request"),
                    INGJOB(isin="IE00BMTX1Y45", status="success", message="finished request"),
                    INGJOB(isin="LU0380865021", status="queued", message="other isin"),
                ]
            )
            session.commit()

            active_jobs = session.scalars(INGJOB.active_for_isin("IE00BMTX1Y45")).all()

        assert [job.status for job in active_jobs] == ["queued", "running"]
        assert all(job.isin == "IE00BMTX1Y45" for job in active_jobs)
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_active_for_isin_helper_can_be_used_as_duplicate_exists_query() -> None:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)

    try:
        with Session(engine) as session:
            session.add(INGJOB(isin="IE00BMTX1Y45", status="cancelled", message="not active"))
            session.add(INGJOB(isin="LU0380865021", status="running", message="active"))
            session.commit()

            duplicate_exists = session.execute(
                select(INGJOB.active_for_isin("LU0380865021").exists())
            ).scalar_one()
            inactive_exists = session.execute(
                select(INGJOB.active_for_isin("IE00BMTX1Y45").exists())
            ).scalar_one()

        assert duplicate_exists is True
        assert inactive_exists is False
    finally:
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.mark.asyncio
async def test_queue_update_data_jobs_creates_queued_rows_and_skips_active_duplicates() -> None:
    engine: AsyncEngine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

    try:
        async with session_factory() as session:
            session.add(
                INGJOB(
                    isin="LU1681044993",
                    requested_user="prior-user",
                    status="queued",
                    message="Already queued.",
                )
            )
            await session.commit()

        async with session_factory() as session:
            results = await _queue_update_data_jobs(
                session,
                ("IE00BMTX1Y45", "LU1681044993", "US0378331005"),
                "admin",
            )

        assert results == (
            {"isin": "IE00BMTX1Y45", "status": "queued", "message": "Queued for update."},
            {
                "isin": "LU1681044993",
                "status": "skipped",
                "message": "Skipped: active update job already exists.",
            },
            {"isin": "US0378331005", "status": "queued", "message": "Queued for update."},
        )

        async with session_factory() as session:
            jobs = (await session.scalars(select(INGJOB).order_by(INGJOB.id))).all()

        assert [job.isin for job in jobs] == ["LU1681044993", "IE00BMTX1Y45", "US0378331005"]
        assert [job.status for job in jobs] == ["queued", "queued", "queued"]
        assert [job.requested_user for job in jobs] == ["prior-user", "admin", "admin"]
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        await engine.dispose()
