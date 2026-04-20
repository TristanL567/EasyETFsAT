"""Initial schema for EasyETFsAT with strict enterprise field naming."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260418_0001"
down_revision: str | None = None
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _id_and_timestamps(domain: str) -> list[sa.Column]:
    return [
        sa.Column(f"{domain}IDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column(
            f"{domain}CRTDTS",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            f"{domain}UPDDTS",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    ]


def upgrade() -> None:
    op.create_table(
        "SECMDA",
        *_id_and_timestamps("SEC"),
        sa.Column("SECISN", sa.String(length=12), nullable=False),
        sa.Column("SECNAM", sa.String(length=255), nullable=False),
        sa.Column("SECCCY", sa.String(length=3), nullable=True),
        sa.Column("SECCTR", sa.String(length=2), nullable=True),
        sa.Column("SECERT", sa.String(length=64), nullable=True),
        sa.UniqueConstraint("SECISN", name="uq_secmda_isin"),
    )

    op.create_table(
        "SECXRF",
        *_id_and_timestamps("SEC"),
        sa.Column("SECISN", sa.String(length=12), nullable=False),
        sa.Column("SECXTP", sa.String(length=32), nullable=False),
        sa.Column("SECXVL", sa.String(length=128), nullable=False),
        sa.Column("SECPVD", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(["SECISN"], ["SECMDA.SECISN"], ondelete="RESTRICT"),
        sa.UniqueConstraint("SECISN", "SECXTP", "SECXVL", name="uq_secxrf_type_value"),
    )

    op.create_table(
        "REFCCY",
        *_id_and_timestamps("REF"),
        sa.Column("REFCOD", sa.String(length=3), nullable=False),
        sa.Column("REFNAM", sa.String(length=64), nullable=False),
        sa.Column("REFMUN", sa.SmallInteger(), nullable=True),
        sa.UniqueConstraint("REFCOD", name="uq_refccy_code"),
    )

    op.create_table(
        "REFCTR",
        *_id_and_timestamps("REF"),
        sa.Column("REFCOD", sa.String(length=2), nullable=False),
        sa.Column("REFNDE", sa.String(length=128), nullable=False),
        sa.Column("REFNEN", sa.String(length=128), nullable=True),
        sa.UniqueConstraint("REFCOD", name="uq_refctr_code"),
    )

    op.create_table(
        "TAXRPT",
        *_id_and_timestamps("TAX"),
        sa.Column("TAXISN", sa.String(length=12), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("TAXSTS", sa.String(length=16), nullable=True),
        sa.Column("TAXYER", sa.Integer(), nullable=True),
        sa.Column("TAXMDT", sa.Date(), nullable=True),
        sa.Column("TAXCCY", sa.String(length=3), nullable=True),
        sa.Column("TAXISB", sa.String(length=255), nullable=True),
        sa.Column("TAXGVN", sa.Date(), nullable=True),
        sa.Column("TAXGBS", sa.Date(), nullable=True),
        sa.UniqueConstraint("TAXISN", "TAXOKBIDN", name="uq_taxrpt_isin_stmid"),
    )
    op.create_index("ix_taxrpt_isin_year", "TAXRPT", ["TAXISN", "TAXYER"], unique=False)

    numeric = sa.Numeric(20, 10)
    op.create_table(
        "TAXAGE",
        *_id_and_timestamps("TAX"),
        sa.Column("TAXISN", sa.String(length=12), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("TAXK11PVM", numeric, nullable=True),
        sa.Column("TAXK11PVO", numeric, nullable=True),
        sa.Column("TAXK11BVM", numeric, nullable=True),
        sa.Column("TAXK11BVO", numeric, nullable=True),
        sa.Column("TAXK11BVJ", numeric, nullable=True),
        sa.Column("TAXK11STF", numeric, nullable=True),
        sa.Column("TAXK12PVM", numeric, nullable=True),
        sa.Column("TAXK12PVO", numeric, nullable=True),
        sa.Column("TAXK12BVM", numeric, nullable=True),
        sa.Column("TAXK12BVO", numeric, nullable=True),
        sa.Column("TAXK12BVJ", numeric, nullable=True),
        sa.Column("TAXK12STF", numeric, nullable=True),
        sa.Column("TAXK81PVM", numeric, nullable=True),
        sa.Column("TAXK81PVO", numeric, nullable=True),
        sa.Column("TAXK81BVM", numeric, nullable=True),
        sa.Column("TAXK81BVO", numeric, nullable=True),
        sa.Column("TAXK81BVJ", numeric, nullable=True),
        sa.Column("TAXK81STF", numeric, nullable=True),
        sa.Column("TAXK82PVM", numeric, nullable=True),
        sa.Column("TAXK82PVO", numeric, nullable=True),
        sa.Column("TAXK82BVM", numeric, nullable=True),
        sa.Column("TAXK82BVO", numeric, nullable=True),
        sa.Column("TAXK82BVJ", numeric, nullable=True),
        sa.Column("TAXK82STF", numeric, nullable=True),
        sa.Column("TAXK10PVM", numeric, nullable=True),
        sa.Column("TAXK10PVO", numeric, nullable=True),
        sa.Column("TAXK10BVM", numeric, nullable=True),
        sa.Column("TAXK10BVO", numeric, nullable=True),
        sa.Column("TAXK10BVJ", numeric, nullable=True),
        sa.Column("TAXK10STF", numeric, nullable=True),
        sa.Column("TAXK55PVM", numeric, nullable=True),
        sa.Column("TAXK55PVO", numeric, nullable=True),
        sa.Column("TAXK55BVM", numeric, nullable=True),
        sa.Column("TAXK55BVO", numeric, nullable=True),
        sa.Column("TAXK55BVJ", numeric, nullable=True),
        sa.Column("TAXK55STF", numeric, nullable=True),
        sa.Column("TAXK61PVM", numeric, nullable=True),
        sa.Column("TAXK61PVO", numeric, nullable=True),
        sa.Column("TAXK61BVM", numeric, nullable=True),
        sa.Column("TAXK61BVO", numeric, nullable=True),
        sa.Column("TAXK61BVJ", numeric, nullable=True),
        sa.Column("TAXK61STF", numeric, nullable=True),
        sa.Column("TAXK36PVM", numeric, nullable=True),
        sa.Column("TAXK36PVO", numeric, nullable=True),
        sa.Column("TAXK36BVM", numeric, nullable=True),
        sa.Column("TAXK36BVO", numeric, nullable=True),
        sa.Column("TAXK36BVJ", numeric, nullable=True),
        sa.Column("TAXK36STF", numeric, nullable=True),
        sa.Column("TAXK21PVM", numeric, nullable=True),
        sa.Column("TAXK21PVO", numeric, nullable=True),
        sa.Column("TAXK21BVM", numeric, nullable=True),
        sa.Column("TAXK21BVO", numeric, nullable=True),
        sa.Column("TAXK21BVJ", numeric, nullable=True),
        sa.Column("TAXK21STF", numeric, nullable=True),
        sa.ForeignKeyConstraint(
            ["TAXISN", "TAXOKBIDN"],
            ["TAXRPT.TAXISN", "TAXRPT.TAXOKBIDN"],
            name="fk_taxage_taxrpt",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("TAXISN", "TAXOKBIDN", name="uq_taxage_isin_stmid"),
    )

    op.create_table(
        "TAXRAW",
        *_id_and_timestamps("TAX"),
        sa.Column("TAXISN", sa.String(length=12), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "TAXPAY",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["TAXISN", "TAXOKBIDN"],
            ["TAXRPT.TAXISN", "TAXRPT.TAXOKBIDN"],
            name="fk_taxraw_taxrpt",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("TAXISN", "TAXOKBIDN", name="uq_taxraw_isin_stmid"),
    )

    op.create_table(
        "IMPLOG",
        *_id_and_timestamps("IMP"),
        sa.Column("IMPRUNIDN", sa.Uuid(), nullable=False),
        sa.Column("IMPISN", sa.String(length=12), nullable=False),
        sa.Column("IMPOKBIDN", sa.Integer(), nullable=True),
        sa.Column("IMPSTS", sa.String(length=24), nullable=False),
        sa.Column("IMPMSG", sa.Text(), nullable=True),
        sa.Column("IMPRSN", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("IMPRSW", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("IMPSTADTS", sa.DateTime(timezone=True), nullable=False),
        sa.Column("IMPFINDTS", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "IMPERR",
        *_id_and_timestamps("IMP"),
        sa.Column("IMPRUNIDN", sa.Uuid(), nullable=False),
        sa.Column("IMPISN", sa.String(length=12), nullable=False),
        sa.Column("IMPOKBIDN", sa.Integer(), nullable=True),
        sa.Column("IMPSTG", sa.String(length=64), nullable=False),
        sa.Column("IMPECD", sa.String(length=64), nullable=True),
        sa.Column("IMPEMS", sa.Text(), nullable=False),
        sa.Column(
            "IMPPAY",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_table("IMPERR")
    op.drop_table("IMPLOG")
    op.drop_table("TAXRAW")
    op.drop_table("TAXAGE")
    op.drop_index("ix_taxrpt_isin_year", table_name="TAXRPT")
    op.drop_table("TAXRPT")
    op.drop_table("REFCTR")
    op.drop_table("REFCCY")
    op.drop_table("SECXRF")
    op.drop_table("SECMDA")
