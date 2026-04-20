from datetime import date
from decimal import Decimal

from sqlalchemy import BIGINT, Date, ForeignKey, Index, Integer, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fondant.db.base import Base, IdTimestampMixin


class SECMDA(IdTimestampMixin, Base):
    __tablename__ = "SECMDA"
    DOMAIN_PREFIX = "SEC"
    __table_args__ = (UniqueConstraint("SECISN", name="uq_secmda_isin"),)

    isin: Mapped[str] = mapped_column("SECISN", String(12), nullable=False)
    name: Mapped[str] = mapped_column("SECNAM", String(255), nullable=False)
    waehrung: Mapped[str | None] = mapped_column("SECCCY", String(3))
    domicile_ctr: Mapped[str | None] = mapped_column("SECCTR", String(2))
    ertragstyp: Mapped[str | None] = mapped_column("SECERT", String(64))


class SECDIV(IdTimestampMixin, Base):
    __tablename__ = "SECDIV"
    DOMAIN_PREFIX = "SEC"
    __table_args__ = (
        UniqueConstraint("SECISN", "SECFLWDAT", "SECFLWTYP", "SECOKBIDN", name="uq_secdiv_event"),
        Index("ix_secdiv_isin_flowdate", "SECISN", "SECFLWDAT"),
    )

    isin: Mapped[str] = mapped_column(
        "SECISN",
        String(12),
        ForeignKey("SECMDA.SECISN", ondelete="RESTRICT"),
        nullable=False,
    )
    okb_id: Mapped[int | None] = mapped_column("SECOKBIDN", BIGINT)
    flow_type: Mapped[str] = mapped_column("SECFLWTYP", String(24), nullable=False)
    flow_date: Mapped[date] = mapped_column("SECFLWDAT", Date, nullable=False)
    cash_amount: Mapped[Decimal | None] = mapped_column("SECFLWAMT", Numeric(20, 10))
    waehrung: Mapped[str | None] = mapped_column("SECCCY", String(3))
    report_year: Mapped[int | None] = mapped_column("SECYEA", Integer)
    status_code: Mapped[str | None] = mapped_column("SECSTS", String(16))
