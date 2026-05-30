"""Add ingestion job tracking table."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260530_0013"
down_revision: str | None = "20260419_0012"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "INGJOB",
        sa.Column("JOBIDN", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "JOBCRTDTS",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "JOBUPDDTS",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("JOBISN", sa.String(length=12), nullable=False),
        sa.Column("JOBREQUSR", sa.String(length=255), nullable=True),
        sa.Column("JOBSTS", sa.String(length=16), nullable=False),
        sa.Column("JOBMSG", sa.Text(), nullable=True),
        sa.Column("JOBERR", sa.Text(), nullable=True),
        sa.Column("JOBSTADTS", sa.DateTime(timezone=True), nullable=True),
        sa.Column("JOBFINDTS", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("JOBIDN"),
    )
    op.create_index("ix_ingjob_isin_status", "INGJOB", ["JOBISN", "JOBSTS"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_ingjob_isin_status", table_name="INGJOB")
    op.drop_table("INGJOB")
