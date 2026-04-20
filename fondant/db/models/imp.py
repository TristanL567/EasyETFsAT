import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from fondant.db.base import Base, IdTimestampMixin


def _json_type() -> JSONB | JSON:
    return JSON().with_variant(JSONB, "postgresql")


class IMPLOG(IdTimestampMixin, Base):
    __tablename__ = "IMPLOG"

    run_id: Mapped[uuid.UUID] = mapped_column("IMPRUNIDN", Uuid, nullable=False, default=uuid.uuid4)
    isin: Mapped[str] = mapped_column("IMPISN", String(12), nullable=False)
    stm_id: Mapped[int | None] = mapped_column("IMPOKBIDN", Integer)
    status: Mapped[str] = mapped_column("IMPSTS", String(24), nullable=False)
    message: Mapped[str | None] = mapped_column("IMPMSG", Text)
    records_seen: Mapped[int] = mapped_column("IMPRSN", Integer, nullable=False, default=0)
    records_written: Mapped[int] = mapped_column("IMPRSW", Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(
        "IMPSTADTS",
        DateTime(timezone=True),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column("IMPFINDTS", DateTime(timezone=True))


class IMPERR(IdTimestampMixin, Base):
    __tablename__ = "IMPERR"

    run_id: Mapped[uuid.UUID] = mapped_column("IMPRUNIDN", Uuid, nullable=False)
    isin: Mapped[str] = mapped_column("IMPISN", String(12), nullable=False)
    stm_id: Mapped[int | None] = mapped_column("IMPOKBIDN", Integer)
    stage: Mapped[str] = mapped_column("IMPSTG", String(64), nullable=False)
    error_code: Mapped[str | None] = mapped_column("IMPECD", String(64))
    error_message: Mapped[str] = mapped_column("IMPEMS", Text, nullable=False)
    payload: Mapped[dict | None] = mapped_column("IMPPAY", _json_type())
