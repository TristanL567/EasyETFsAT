"""Add saved BusinessQuery tax field selections."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260531_0018"
down_revision: str | None = "20260531_0017"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _tax_field_list_type() -> sa.JSON:
    return sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql")


def upgrade() -> None:
    with op.batch_alter_table("BQSAVED") as batch_op:
        batch_op.add_column(sa.Column("BQSTXFLDS", _tax_field_list_type(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("BQSAVED") as batch_op:
        batch_op.drop_column("BQSTXFLDS")
