"""Add K40/K62 SOURCEAGE fields and extend V1_TAXDATPRE with K40/K61/K62."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260419_0010"
down_revision: str | None = "20260419_0009"
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
    END) AS "CORAMTSTI",
    MAX(CASE
        WHEN l."TAXCOD" = 'K61' AND c."TAXKEY" = 'pv_mit'
        THEN d."TAXAMT"
    END) AS "K61PVM",
    MAX(CASE
        WHEN l."TAXCOD" = 'K61' AND c."TAXKEY" = 'pv_ohne'
        THEN d."TAXAMT"
    END) AS "K61PVO",
    MAX(CASE
        WHEN l."TAXCOD" = 'K61' AND c."TAXKEY" = 'bv_mit'
        THEN d."TAXAMT"
    END) AS "K61BVM",
    MAX(CASE
        WHEN l."TAXCOD" = 'K61' AND c."TAXKEY" = 'bv_ohne'
        THEN d."TAXAMT"
    END) AS "K61BVO",
    MAX(CASE
        WHEN l."TAXCOD" = 'K61' AND c."TAXKEY" = 'bv_jur'
        THEN d."TAXAMT"
    END) AS "K61BVJ",
    MAX(CASE
        WHEN l."TAXCOD" = 'K61' AND c."TAXKEY" = 'stiftung'
        THEN d."TAXAMT"
    END) AS "K61STI",
    MAX(CASE
        WHEN l."TAXCOD" = 'K62' AND c."TAXKEY" = 'pv_mit'
        THEN d."TAXAMT"
    END) AS "K62PVM",
    MAX(CASE
        WHEN l."TAXCOD" = 'K62' AND c."TAXKEY" = 'pv_ohne'
        THEN d."TAXAMT"
    END) AS "K62PVO",
    MAX(CASE
        WHEN l."TAXCOD" = 'K62' AND c."TAXKEY" = 'bv_mit'
        THEN d."TAXAMT"
    END) AS "K62BVM",
    MAX(CASE
        WHEN l."TAXCOD" = 'K62' AND c."TAXKEY" = 'bv_ohne'
        THEN d."TAXAMT"
    END) AS "K62BVO",
    MAX(CASE
        WHEN l."TAXCOD" = 'K62' AND c."TAXKEY" = 'bv_jur'
        THEN d."TAXAMT"
    END) AS "K62BVJ",
    MAX(CASE
        WHEN l."TAXCOD" = 'K62' AND c."TAXKEY" = 'stiftung'
        THEN d."TAXAMT"
    END) AS "K62STI",
    MAX(CASE
        WHEN l."TAXCOD" = 'K40' AND c."TAXKEY" = 'pv_mit'
        THEN d."TAXAMT"
    END) AS "K40PVM",
    MAX(CASE
        WHEN l."TAXCOD" = 'K40' AND c."TAXKEY" = 'pv_ohne'
        THEN d."TAXAMT"
    END) AS "K40PVO",
    MAX(CASE
        WHEN l."TAXCOD" = 'K40' AND c."TAXKEY" = 'bv_mit'
        THEN d."TAXAMT"
    END) AS "K40BVM",
    MAX(CASE
        WHEN l."TAXCOD" = 'K40' AND c."TAXKEY" = 'bv_ohne'
        THEN d."TAXAMT"
    END) AS "K40BVO",
    MAX(CASE
        WHEN l."TAXCOD" = 'K40' AND c."TAXKEY" = 'bv_jur'
        THEN d."TAXAMT"
    END) AS "K40BVJ",
    MAX(CASE
        WHEN l."TAXCOD" = 'K40' AND c."TAXKEY" = 'stiftung'
        THEN d."TAXAMT"
    END) AS "K40STI"
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
    op.add_column("SOURCEAGE", sa.Column("SRCK40PVM", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK40PVO", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK40BVM", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK40BVO", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK40BVJ", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK40STF", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK62PVM", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK62PVO", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK62BVM", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK62BVO", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK62BVJ", sa.Numeric(20, 10), nullable=True))
    op.add_column("SOURCEAGE", sa.Column("SRCK62STF", sa.Numeric(20, 10), nullable=True))

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
        )
    )

    op.drop_column("SOURCEAGE", "SRCK62STF")
    op.drop_column("SOURCEAGE", "SRCK62BVJ")
    op.drop_column("SOURCEAGE", "SRCK62BVO")
    op.drop_column("SOURCEAGE", "SRCK62BVM")
    op.drop_column("SOURCEAGE", "SRCK62PVO")
    op.drop_column("SOURCEAGE", "SRCK62PVM")
    op.drop_column("SOURCEAGE", "SRCK40STF")
    op.drop_column("SOURCEAGE", "SRCK40BVJ")
    op.drop_column("SOURCEAGE", "SRCK40BVO")
    op.drop_column("SOURCEAGE", "SRCK40BVM")
    op.drop_column("SOURCEAGE", "SRCK40PVO")
    op.drop_column("SOURCEAGE", "SRCK40PVM")
