"""Add TAXLIN description metadata columns."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260419_0012"
down_revision: str | None = "20260419_0011"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("TAXLIN", sa.Column("TAXDSC", sa.String(length=512), nullable=True))
    op.add_column("TAXLIN", sa.Column("TAXUSE", sa.String(length=512), nullable=True))
    op.add_column("TAXLIN", sa.Column("TAXSRC", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("TAXLIN", "TAXSRC")
    op.drop_column("TAXLIN", "TAXUSE")
    op.drop_column("TAXLIN", "TAXDSC")
