"""Refine V1_TAXDATPRE and add V2_TAXDATEUR."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260419_0011"
down_revision: str | None = "20260419_0010"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


V1_SQL = """
CREATE VIEW "V1_TAXDATPRE" AS
SELECT
    r."TAXISN" AS "TAXISN",
    r."TAXOKBIDN" AS "TAXOKBIDN",
    r."TAXYEA" AS "TAXYEA",
    r."TAXCCY" AS "FNDCCY",
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


V2_SQL = """
CREATE VIEW "V2_TAXDATEUR" AS
WITH base AS (
    SELECT
        v."TAXISN",
        v."TAXOKBIDN",
        v."TAXYEA",
        v."FNDCCY",
        r."TAXMDT" AS "TAXMDT",
        CASE
            WHEN v."FNDCCY" = 'EUR' THEN CAST(1 AS NUMERIC(20,10))
            ELSE fx."REFRAT"
        END AS "FXRAT",
        v."K61PVM",
        v."K61PVO",
        v."K61BVM",
        v."K61BVO",
        v."K61BVJ",
        v."K61STI",
        v."K62PVM",
        v."K62PVO",
        v."K62BVM",
        v."K62BVO",
        v."K62BVJ",
        v."K62STI",
        v."K40PVM",
        v."K40PVO",
        v."K40BVM",
        v."K40BVO",
        v."K40BVJ",
        v."K40STI"
    FROM "V1_TAXDATPRE" AS v
    JOIN "TAXRPT" AS r
        ON r."TAXISN" = v."TAXISN"
       AND r."TAXOKBIDN" = v."TAXOKBIDN"
    LEFT JOIN "REFEXC" AS fx
        ON fx."REFCCY" = v."FNDCCY"
       AND fx."REFDAT" = r."TAXMDT"
)
SELECT
    "TAXISN",
    "TAXOKBIDN",
    "TAXYEA",
    "FNDCCY",
    "TAXMDT",
    "FXRAT",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K61PVM" / "FXRAT" END AS "K61PVM",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K61PVO" / "FXRAT" END AS "K61PVO",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K61BVM" / "FXRAT" END AS "K61BVM",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K61BVO" / "FXRAT" END AS "K61BVO",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K61BVJ" / "FXRAT" END AS "K61BVJ",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K61STI" / "FXRAT" END AS "K61STI",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K62PVM" / "FXRAT" END AS "K62PVM",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K62PVO" / "FXRAT" END AS "K62PVO",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K62BVM" / "FXRAT" END AS "K62BVM",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K62BVO" / "FXRAT" END AS "K62BVO",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K62BVJ" / "FXRAT" END AS "K62BVJ",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K62STI" / "FXRAT" END AS "K62STI",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K40PVM" / "FXRAT" END AS "K40PVM",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K40PVO" / "FXRAT" END AS "K40PVO",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K40BVM" / "FXRAT" END AS "K40BVM",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K40BVO" / "FXRAT" END AS "K40BVO",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K40BVJ" / "FXRAT" END AS "K40BVJ",
    CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL ELSE "K40STI" / "FXRAT" END AS "K40STI"
FROM base
"""


V1_OLD_SQL = """
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
    op.execute(sa.text('DROP VIEW IF EXISTS "V2_TAXDATEUR"'))
    op.execute(sa.text('DROP VIEW IF EXISTS "V1_TAXDATPRE"'))
    op.execute(sa.text(V1_SQL))
    op.execute(sa.text(V2_SQL))


def downgrade() -> None:
    op.execute(sa.text('DROP VIEW IF EXISTS "V2_TAXDATEUR"'))
    op.execute(sa.text('DROP VIEW IF EXISTS "V1_TAXDATPRE"'))
    op.execute(sa.text(V1_OLD_SQL))
