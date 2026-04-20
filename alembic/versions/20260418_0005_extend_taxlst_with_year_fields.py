"""Extend TAXLST with year-driving fields from report list metadata."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260418_0005"
down_revision: str | None = "20260418_0004"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("TAXLST", sa.Column("TAXYER", sa.Integer(), nullable=True))
    op.add_column("TAXLST", sa.Column("TAXGJB", sa.Date(), nullable=True))
    op.add_column("TAXLST", sa.Column("TAXGJE", sa.Date(), nullable=True))
    op.add_column("TAXLST", sa.Column("TAXENTDTS", sa.DateTime(timezone=False), nullable=True))
    op.add_column("TAXLST", sa.Column("TAXZFL", sa.Date(), nullable=True))


def downgrade() -> None:
    op.drop_column("TAXLST", "TAXZFL")
    op.drop_column("TAXLST", "TAXENTDTS")
    op.drop_column("TAXLST", "TAXGJE")
    op.drop_column("TAXLST", "TAXGJB")
    op.drop_column("TAXLST", "TAXYER")
