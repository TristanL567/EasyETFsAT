"""Add TAXLST table for OeKB report-list metadata by Melde-ID."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260418_0004"
down_revision: str | None = "20260418_0003"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "TAXLST",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column(
            "TAXCRTDTS",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "TAXUPDDTS",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("TAXISN", sa.String(length=12), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("TAXSTS", sa.String(length=16), nullable=True),
        sa.Column("TAXMDT", sa.Date(), nullable=True),
        sa.Column("TAXGBS", sa.Date(), nullable=True),
        sa.Column("TAXJMS", sa.Boolean(), nullable=True),
        sa.Column("TAXAMS", sa.Boolean(), nullable=True),
        sa.Column("TAXSNW", sa.Boolean(), nullable=True),
        sa.Column("TAXKIDN", sa.BigInteger(), nullable=True),
        sa.Column(
            "TAXPAY",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.UniqueConstraint("TAXISN", "TAXOKBIDN", name="uq_taxlst_isin_stmid"),
    )
    op.create_index("ix_taxlst_isin_mdt", "TAXLST", ["TAXISN", "TAXMDT"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_taxlst_isin_mdt", table_name="TAXLST")
    op.drop_table("TAXLST")
