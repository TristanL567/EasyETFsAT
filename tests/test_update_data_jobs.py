from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

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
