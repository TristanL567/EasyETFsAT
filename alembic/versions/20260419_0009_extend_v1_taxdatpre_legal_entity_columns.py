"""Extend V1_TAXDATPRE with legal-entity category columns."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260419_0009"
down_revision: str | None = "20260419_0008"
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
        WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'bv_mit'
        THEN d."TAXAMT"
    END) AS "AGEBVM",
    MAX(CASE
        WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'bv_ohne'
        THEN d."TAXAMT"
    END) AS "AGEBVO",
    MAX(CASE
        WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'bv_jur'
        THEN d."TAXAMT"
    END) AS "AGEBVJ",
    MAX(CASE
        WHEN l."TAXKEY" = 'ag_ertraege' AND c."TAXKEY" = 'stiftung'
        THEN d."TAXAMT"
    END) AS "AGESTI",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'pv_mit'
        THEN d."TAXAMT"
    END) AS "CORAMTPVM",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'pv_ohne'
        THEN d."TAXAMT"
    END) AS "CORAMTPVO",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'bv_mit'
        THEN d."TAXAMT"
    END) AS "CORAMTBVM",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'bv_ohne'
        THEN d."TAXAMT"
    END) AS "CORAMTBVO",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'bv_jur'
        THEN d."TAXAMT"
    END) AS "CORAMTBVJ",
    MAX(CASE
        WHEN l."TAXKEY" = 'korrekturbetrag_saldiert' AND c."TAXKEY" = 'stiftung'
        THEN d."TAXAMT"
    END) AS "CORAMTSTI"
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
        )
    )
