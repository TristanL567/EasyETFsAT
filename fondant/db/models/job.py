from datetime import datetime

from sqlalchemy import DateTime, Index, String, Text, select
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import Select

from fondant.db.base import Base, IdTimestampMixin

UPDATE_DATA_JOB_STATUSES = ("queued", "running", "success", "failed", "skipped", "cancelled")
ACTIVE_UPDATE_DATA_JOB_STATUSES = ("queued", "running")


class INGJOB(IdTimestampMixin, Base):
    __tablename__ = "INGJOB"
    DOMAIN_PREFIX = "JOB"
    __table_args__ = (Index("ix_ingjob_isin_status", "JOBISN", "JOBSTS"),)

    isin: Mapped[str] = mapped_column("JOBISN", String(12), nullable=False)
    requested_user: Mapped[str | None] = mapped_column("JOBREQUSR", String(255))
    status: Mapped[str] = mapped_column("JOBSTS", String(16), nullable=False)
    message: Mapped[str | None] = mapped_column("JOBMSG", Text)
    error_detail: Mapped[str | None] = mapped_column("JOBERR", Text)
    started_at: Mapped[datetime | None] = mapped_column("JOBSTADTS", DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column("JOBFINDTS", DateTime(timezone=True))

    @classmethod
    def active_for_isin(cls, isin: str) -> Select[tuple["INGJOB"]]:
        return select(cls).where(
            cls.isin == isin,
            cls.status.in_(ACTIVE_UPDATE_DATA_JOB_STATUSES),
        ).order_by(cls.id)
