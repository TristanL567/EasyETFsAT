"""Expand V2_TAXDATHOMCCY tax field coverage."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260531_0017"
down_revision: str | None = "20260531_0016"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


TAX_LINE_CODES = (
    "K40",
    "K11",
    "K12",
    "K81",
    "K82",
    "K10",
    "K55",
    "K61",
    "K62",
    "K36",
    "K21",
)
TAX_CATEGORY_SUFFIXES = ("PVM", "PVO", "BVM", "BVO", "BVJ", "STI")
TAX_CATEGORY_KEYS = {
    "PVM": "pv_mit",
    "PVO": "pv_ohne",
    "BVM": "bv_mit",
    "BVO": "bv_ohne",
    "BVJ": "bv_jur",
    "STI": "stiftung",
}

OLD_TAX_LINE_CODES = ("K40", "K61", "K62")


def _tax_columns(line_codes: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(
        f"{line_code}{suffix}"
        for line_code in line_codes
        for suffix in TAX_CATEGORY_SUFFIXES
    )


def _base_amount_columns_sql(line_codes: tuple[str, ...]) -> str:
    return ",\n        ".join(
        (
            "MAX(CASE "
            f"WHEN l.\"TAXCOD\" = '{line_code}' AND c.\"TAXKEY\" = '{TAX_CATEGORY_KEYS[suffix]}' "
            f"THEN d.\"TAXAMT\" END) AS \"{line_code}{suffix}\""
        )
        for line_code in line_codes
        for suffix in TAX_CATEGORY_SUFFIXES
    )


def _home_amount_columns_sql(line_codes: tuple[str, ...]) -> str:
    return ",\n    ".join(
        f'"{column}" AS "{column}_HOMCCY"' for column in _tax_columns(line_codes)
    )


def _eur_amount_columns_sql(line_codes: tuple[str, ...]) -> str:
    return ",\n    ".join(
        (
            'CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL '
            f'ELSE "{column}" / "FXRAT" END AS "{column}_EUR"'
        )
        for column in _tax_columns(line_codes)
    )


def _v2_taxdathomccy_sql(line_codes: tuple[str, ...]) -> str:
    return f"""
CREATE VIEW "V2_TAXDATHOMCCY" AS
WITH base AS (
    SELECT
        r."TAXISN" AS "TAXISN",
        r."TAXOKBIDN" AS "TAXOKBIDN",
        r."TAXYEA" AS "TAXYEA",
        r."TAXCCY" AS "FNDCCY",
        r."TAXMDT" AS "TAXMDT",
        CASE
            WHEN r."TAXCCY" = 'EUR' THEN CAST(1 AS NUMERIC(20,10))
            ELSE fx."REFRAT"
        END AS "FXRAT",
        {_base_amount_columns_sql(line_codes)}
    FROM "TAXRPT" AS r
    LEFT JOIN "TAXDAT" AS d
        ON d."TAXRPTIDN" = r."TAXIDN"
    LEFT JOIN "TAXLIN" AS l
        ON l."TAXIDN" = d."TAXLINIDN"
    LEFT JOIN "TAXCAT" AS c
        ON c."TAXIDN" = d."TAXCATIDN"
    LEFT JOIN "REFEXC" AS fx
        ON fx."REFCCY" = r."TAXCCY"
       AND fx."REFDAT" = r."TAXMDT"
    GROUP BY
        r."TAXISN",
        r."TAXOKBIDN",
        r."TAXYEA",
        r."TAXCCY",
        r."TAXMDT",
        fx."REFRAT"
)
SELECT
    "TAXISN",
    "TAXOKBIDN",
    "TAXYEA",
    "FNDCCY",
    "TAXMDT",
    "FXRAT",
    {_home_amount_columns_sql(line_codes)},
    {_eur_amount_columns_sql(line_codes)}
FROM base
"""


def upgrade() -> None:
    op.execute(sa.text('DROP VIEW IF EXISTS "V2_TAXDATHOMCCY"'))
    op.execute(sa.text(_v2_taxdathomccy_sql(TAX_LINE_CODES)))


def downgrade() -> None:
    op.execute(sa.text('DROP VIEW IF EXISTS "V2_TAXDATHOMCCY"'))
    op.execute(sa.text(_v2_taxdathomccy_sql(OLD_TAX_LINE_CODES)))
