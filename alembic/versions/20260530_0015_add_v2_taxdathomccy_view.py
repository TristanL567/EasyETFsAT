"""Add V2_TAXDATHOMCCY reporting view."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20260530_0015"
down_revision: str | None = "20260530_0014"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


TAX_COLUMNS = tuple(
    f"{line_code}{suffix}"
    for line_code in ("K40", "K61", "K62")
    for suffix in ("PVM", "PVO", "BVM", "BVO", "BVJ", "STI")
)

BASE_AMOUNT_COLUMNS_SQL = ",\n        ".join(f'v."{column}"' for column in TAX_COLUMNS)
HOME_AMOUNT_COLUMNS_SQL = ",\n    ".join(
    f'"{column}" AS "{column}_HOMCCY"' for column in TAX_COLUMNS
)
EUR_AMOUNT_COLUMNS_SQL = ",\n    ".join(
    (
        'CASE WHEN "FXRAT" IS NULL OR "FXRAT" = 0 THEN NULL '
        f'ELSE "{column}" / "FXRAT" END AS "{column}_EUR"'
    )
    for column in TAX_COLUMNS
)


V2_TAXDATHOMCCY_SQL = f"""
CREATE VIEW "V2_TAXDATHOMCCY" AS
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
        {BASE_AMOUNT_COLUMNS_SQL}
    FROM "V1_TAXDATPRE" AS v
    JOIN "TAXRPT" AS r
        ON r."TAXISN" = v."TAXISN"
       AND r."TAXOKBIDN" = v."TAXOKBIDN"
       AND r."TAXYEA" = v."TAXYEA"
       AND r."TAXCCY" = v."FNDCCY"
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
    {HOME_AMOUNT_COLUMNS_SQL},
    {EUR_AMOUNT_COLUMNS_SQL}
FROM base
"""


def upgrade() -> None:
    op.execute(sa.text('DROP VIEW IF EXISTS "V2_TAXDATHOMCCY"'))
    op.execute(sa.text(V2_TAXDATHOMCCY_SQL))


def downgrade() -> None:
    op.execute(sa.text('DROP VIEW IF EXISTS "V2_TAXDATHOMCCY"'))
