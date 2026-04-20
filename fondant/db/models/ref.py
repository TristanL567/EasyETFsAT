from datetime import date
from decimal import Decimal

from sqlalchemy import Date, Index, Numeric, SmallInteger, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fondant.db.base import Base, IdTimestampMixin


class REFCCY(IdTimestampMixin, Base):
    __tablename__ = "REFCCY"
    __table_args__ = (UniqueConstraint("REFCOD", name="uq_refccy_code"),)

    code: Mapped[str] = mapped_column("REFCOD", String(3), nullable=False)
    name: Mapped[str] = mapped_column("REFNAM", String(64), nullable=False)
    minor_units: Mapped[int | None] = mapped_column("REFMUN", SmallInteger)


class REFCTR(IdTimestampMixin, Base):
    __tablename__ = "REFCTR"
    __table_args__ = (UniqueConstraint("REFCOD", name="uq_refctr_code"),)

    code: Mapped[str] = mapped_column("REFCOD", String(2), nullable=False)
    name_de: Mapped[str] = mapped_column("REFNDE", String(128), nullable=False)
    name_en: Mapped[str | None] = mapped_column("REFNEN", String(128))


class REFEXC(IdTimestampMixin, Base):
    __tablename__ = "REFEXC"
    __table_args__ = (
        UniqueConstraint("REFDAT", "REFCCY", name="uq_refexc_date_currency"),
        Index("ix_refexc_currency_date", "REFCCY", "REFDAT"),
    )

    rate_date: Mapped[date] = mapped_column("REFDAT", Date, nullable=False)
    currency_code: Mapped[str] = mapped_column("REFCCY", String(3), nullable=False)
    rate: Mapped[Decimal] = mapped_column("REFRAT", Numeric(20, 10), nullable=False)
