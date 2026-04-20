from datetime import datetime

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column


class Base(DeclarativeBase):
    pass


class IdTimestampMixin:
    @classmethod
    def _domain_prefix(cls) -> str:
        return getattr(cls, "DOMAIN_PREFIX", cls.__tablename__[:3])

    @declared_attr
    def id(cls) -> Mapped[int]:  # noqa: N805
        domain = cls._domain_prefix()
        return mapped_column(f"{domain}IDN", primary_key=True, autoincrement=True)

    @declared_attr
    def created_at(cls) -> Mapped[datetime]:  # noqa: N805
        domain = cls._domain_prefix()
        return mapped_column(
            f"{domain}CRTDTS",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
        )

    @declared_attr
    def updated_at(cls) -> Mapped[datetime]:  # noqa: N805
        domain = cls._domain_prefix()
        return mapped_column(
            f"{domain}UPDDTS",
            DateTime(timezone=True),
            nullable=False,
            server_default=func.now(),
            onupdate=func.now(),
        )
