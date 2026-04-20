"""Add TAXAGE report year column for direct year-based querying."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260418_0003"
down_revision: str | None = "20260418_0002"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("TAXAGE", sa.Column("TAXYER", sa.Integer(), nullable=True))
    op.create_index("ix_taxage_isin_year", "TAXAGE", ["TAXISN", "TAXYER"], unique=False)
    op.execute(
        sa.text(
            """
            UPDATE "TAXAGE" AS a
            SET "TAXYER" = (
                SELECT r."TAXYER"
                FROM "TAXRPT" AS r
                WHERE r."TAXISN" = a."TAXISN"
                  AND r."TAXOKBIDN" = a."TAXOKBIDN"
            )
            WHERE a."TAXYER" IS NULL
            """
        )
    )


def downgrade() -> None:
    op.drop_index("ix_taxage_isin_year", table_name="TAXAGE")
    op.drop_column("TAXAGE", "TAXYER")
