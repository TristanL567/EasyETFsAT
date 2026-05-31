"""Add saved BusinessQuery group schema."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260531_0016"
down_revision: str | None = "20260530_0015"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "BQGROUP",
        sa.Column("BQGIDN", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "BQGCRTDTS",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "BQGUPDDTS",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("BQGUSR", sa.String(length=255), nullable=False),
        sa.Column("BQGNAM", sa.String(length=255), nullable=False),
        sa.Column("BQGDSC", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("BQGIDN"),
        sa.UniqueConstraint("BQGUSR", "BQGNAM", name="uq_bqgroup_user_name"),
        sa.UniqueConstraint("BQGIDN", "BQGUSR", name="uq_bqgroup_id_user"),
    )
    op.create_index("ix_bqgroup_owner", "BQGROUP", ["BQGUSR"], unique=False)

    with op.batch_alter_table("BQSAVED") as batch_op:
        batch_op.add_column(sa.Column("BQSGRPIDN", sa.Integer(), nullable=True))
        batch_op.create_foreign_key(
            "fk_bqsaved_group_owner",
            "BQGROUP",
            ["BQSGRPIDN", "BQSUSR"],
            ["BQGIDN", "BQGUSR"],
            ondelete="RESTRICT",
        )
        batch_op.create_index("ix_bqsaved_group", ["BQSGRPIDN"], unique=False)


def downgrade() -> None:
    with op.batch_alter_table("BQSAVED") as batch_op:
        batch_op.drop_index("ix_bqsaved_group")
        batch_op.drop_constraint("fk_bqsaved_group_owner", type_="foreignkey")
        batch_op.drop_column("BQSGRPIDN")

    op.drop_index("ix_bqgroup_owner", table_name="BQGROUP")
    op.drop_table("BQGROUP")
