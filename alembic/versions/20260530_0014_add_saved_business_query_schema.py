"""Add saved BusinessQuery schema."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260530_0014"
down_revision: str | None = "20260530_0013"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "BQSAVED",
        sa.Column("BQSIDN", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "BQSCRTDTS",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "BQSUPDDTS",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("BQSUSR", sa.String(length=255), nullable=False),
        sa.Column("BQSNAM", sa.String(length=255), nullable=False),
        sa.Column("BQSLENTYP", sa.String(length=64), nullable=False),
        sa.Column("BQSSUBCAT", sa.String(length=64), nullable=False),
        sa.Column("BQSTXYR", sa.String(length=32), nullable=False),
        sa.Column("BQSAMT", sa.Numeric(20, 10), nullable=False),
        sa.Column("BQSNOTE", sa.Text(), nullable=True),
        sa.Column(
            "BQSISNS",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("BQSIDN"),
        sa.UniqueConstraint("BQSUSR", "BQSNAM", name="uq_bqsaved_user_name"),
    )
    op.create_index("ix_bqsaved_owner", "BQSAVED", ["BQSUSR"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_bqsaved_owner", table_name="BQSAVED")
    op.drop_table("BQSAVED")
