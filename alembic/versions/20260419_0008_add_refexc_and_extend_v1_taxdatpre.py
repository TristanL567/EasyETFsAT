"""Add REFEXC table and extend V1_TAXDATPRE with FNDCCY."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260419_0008"
down_revision: str | None = "20260419_0007"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


VIEW_SQL = """
CREATE VIEW "V1_TAXDATPRE" AS
SELECT
    r."TAXISN" AS "TAXISN",
    r."TAXOKBIDN" AS "TAXOKBIDN",
    r."TAXYEA" AS "TAXYEA",
    r."TAXCCY" AS "FNDCCY",
    MAX(CASE
        WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'pv_mit'
        THEN d."TAXAMT"
    END) AS "AGEPVM",
    MAX(CASE
        WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'pv_ohne'
        THEN d."TAXAMT"
    END) AS "AGEPVO",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'pv_mit'
        THEN d."TAXAMT"
    END) AS "CORAMTPVM",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'pv_ohne'
        THEN d."TAXAMT"
    END) AS "CORAMTPVO"
FROM "TAXRPT" AS r
LEFT JOIN "TAXDAT" AS d
    ON d."TAXRPTIDN" = r."TAXIDN"
LEFT JOIN "TAXLIN" AS l
    ON l."TAXIDN" = d."TAXLINIDN"
LEFT JOIN "TAXCAT" AS c
    ON c."TAXIDN" = d."TAXCATIDN"
GROUP BY
    r."TAXISN",
    r."TAXOKBIDN",
    r."TAXYEA",
    r."TAXCCY"
"""


def upgrade() -> None:
    op.create_table(
        "REFEXC",
        sa.Column("REFIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column(
            "REFCRTDTS",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "REFUPDDTS",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("REFDAT", sa.Date(), nullable=False),
        sa.Column("REFCCY", sa.String(length=3), nullable=False),
        sa.Column("REFRAT", sa.Numeric(20, 10), nullable=False),
        sa.UniqueConstraint("REFDAT", "REFCCY", name="uq_refexc_date_currency"),
    )
    op.create_index("ix_refexc_currency_date", "REFEXC", ["REFCCY", "REFDAT"], unique=False)
    op.execute(sa.text('DROP VIEW IF EXISTS "V1_TAXDATPRE"'))
    op.execute(sa.text(VIEW_SQL))


def downgrade() -> None:
    op.execute(sa.text('DROP VIEW IF EXISTS "V1_TAXDATPRE"'))
    op.execute(
        sa.text(
            """
            CREATE VIEW "V1_TAXDATPRE" AS
            SELECT
                r."TAXISN" AS "TAXISN",
                r."TAXOKBIDN" AS "TAXOKBIDN",
                r."TAXYEA" AS "TAXYEA",
                MAX(CASE
                    WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'pv_mit'
                    THEN d."TAXAMT"
                END) AS "AGEPVM",
                MAX(CASE
                    WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'pv_ohne'
                    THEN d."TAXAMT"
                END) AS "AGEPVO",
                MAX(CASE
                    WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'pv_mit'
                    THEN d."TAXAMT"
                END) AS "CORAMTPVM",
                MAX(CASE
                    WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'pv_ohne'
                    THEN d."TAXAMT"
                END) AS "CORAMTPVO"
            FROM "TAXRPT" AS r
            LEFT JOIN "TAXDAT" AS d
                ON d."TAXRPTIDN" = r."TAXIDN"
            LEFT JOIN "TAXLIN" AS l
                ON l."TAXIDN" = d."TAXLINIDN"
            LEFT JOIN "TAXCAT" AS c
                ON c."TAXIDN" = d."TAXCATIDN"
            GROUP BY
                r."TAXISN",
                r."TAXOKBIDN",
                r."TAXYEA"
            """
        )
    )
    op.drop_index("ix_refexc_currency_date", table_name="REFEXC")
    op.drop_table("REFEXC")
