from decimal import Decimal

from sqlalchemy import Index, Numeric, String, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from fondant.db.base import Base, IdTimestampMixin

ALL_AVAILABLE_YEARS = "all_available_years"


def _isin_list_json_type() -> JSONB | JSON:
    return JSON().with_variant(JSONB, "postgresql")


class BQSAVED(IdTimestampMixin, Base):
    __tablename__ = "BQSAVED"
    DOMAIN_PREFIX = "BQS"
    __table_args__ = (
        UniqueConstraint("BQSUSR", "BQSNAM", name="uq_bqsaved_user_name"),
        Index("ix_bqsaved_owner", "BQSUSR"),
    )

    owner_username: Mapped[str] = mapped_column("BQSUSR", String(255), nullable=False)
    query_name: Mapped[str] = mapped_column("BQSNAM", String(255), nullable=False)
    legal_entity_type: Mapped[str] = mapped_column("BQSLENTYP", String(64), nullable=False)
    subcategory_key: Mapped[str] = mapped_column("BQSSUBCAT", String(64), nullable=False)
    tax_year_filter: Mapped[str] = mapped_column("BQSTXYR", String(32), nullable=False)
    amount: Mapped[Decimal] = mapped_column("BQSAMT", Numeric(20, 10), nullable=False)
    note: Mapped[str | None] = mapped_column("BQSNOTE", Text)
    default_isins: Mapped[list[str] | None] = mapped_column("BQSISNS", _isin_list_json_type())
