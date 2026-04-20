"""Rebuild schema into SOURCE layer + curated TAX architecture."""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import context, op

# revision identifiers, used by Alembic.
revision: str = "20260419_0006"
down_revision: str | None = "20260418_0005"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _has_table(table_name: str) -> bool:
    if context.is_offline_mode():
        return False
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names()


def _drop_table_if_exists(table_name: str) -> None:
    if _has_table(table_name):
        op.drop_table(table_name)


def _ensure_secmda() -> None:
    if _has_table("SECMDA"):
        return

    op.create_table(
        "SECMDA",
        sa.Column("SECIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("SECCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SECUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SECISN", sa.String(length=12), nullable=False),
        sa.Column("SECNAM", sa.String(length=255), nullable=False),
        sa.Column("SECCCY", sa.String(length=3), nullable=True),
        sa.Column("SECCTR", sa.String(length=2), nullable=True),
        sa.Column("SECERT", sa.String(length=64), nullable=True),
        sa.UniqueConstraint("SECISN", name="uq_secmda_isin"),
    )


def _create_sourcerpt() -> None:
    op.create_table(
        "SOURCERPT",
        sa.Column("SRCIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("SRCCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SRCUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SRCISN", sa.String(length=12), nullable=False),
        sa.Column("SRCOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("SRCVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("SRCSTS", sa.String(length=16), nullable=True),
        sa.Column("SRCYEA", sa.Integer(), nullable=True),
        sa.Column("SRCMDT", sa.Date(), nullable=True),
        sa.Column("SRCCCY", sa.String(length=3), nullable=True),
        sa.Column("SRCISB", sa.String(length=255), nullable=True),
        sa.Column("SRCGVN", sa.Date(), nullable=True),
        sa.Column("SRCGBS", sa.Date(), nullable=True),
        sa.Column("SRCBUSYEABEG", sa.Date(), nullable=True),
        sa.Column("SRCBUSYEAEND", sa.Date(), nullable=True),
        sa.Column("SRCENTDTS", sa.DateTime(timezone=False), nullable=True),
        sa.Column("SRCZFL", sa.Date(), nullable=True),
        sa.Column("SRCJMS", sa.Boolean(), nullable=True),
        sa.Column("SRCAMS", sa.Boolean(), nullable=True),
        sa.Column("SRCSNW", sa.Boolean(), nullable=True),
        sa.Column("SRCKIDN", sa.BigInteger(), nullable=True),
        sa.UniqueConstraint("SRCISN", "SRCOKBIDN", name="uq_sourcerpt_isin_okbidn"),
    )
    op.create_index("ix_sourcerpt_isin_year", "SOURCERPT", ["SRCISN", "SRCYEA"], unique=False)


def _create_sourceage() -> None:
    numeric = sa.Numeric(20, 10)
    columns: list[sa.Column] = [
        sa.Column("SRCIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("SRCCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SRCUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SRCISN", sa.String(length=12), nullable=False),
        sa.Column("SRCOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("SRCVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("SRCYEA", sa.Integer(), nullable=True),
    ]

    for metric in ("K11", "K12", "K81", "K82", "K10", "K55", "K61", "K36", "K21"):
        for category in ("PVM", "PVO", "BVM", "BVO", "BVJ", "STF"):
            columns.append(sa.Column(f"SRC{metric}{category}", numeric, nullable=True))

    op.create_table(
        "SOURCEAGE",
        *columns,
        sa.UniqueConstraint("SRCISN", "SRCOKBIDN", name="uq_sourceage_isin_okbidn"),
        sa.ForeignKeyConstraint(
            ["SRCISN", "SRCOKBIDN"],
            ["SOURCERPT.SRCISN", "SOURCERPT.SRCOKBIDN"],
            name="fk_sourceage_sourcerpt",
            ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_sourceage_isin_year", "SOURCEAGE", ["SRCISN", "SRCYEA"], unique=False)


def _create_sourceraw() -> None:
    op.create_table(
        "SOURCERAW",
        sa.Column("SRCIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("SRCCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SRCUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SRCISN", sa.String(length=12), nullable=False),
        sa.Column("SRCOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("SRCVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column(
            "SRCPAY",
            sa.JSON().with_variant(postgresql.JSONB(astext_type=sa.Text()), "postgresql"),
            nullable=False,
        ),
        sa.UniqueConstraint("SRCISN", "SRCOKBIDN", name="uq_sourceraw_isin_okbidn"),
        sa.ForeignKeyConstraint(
            ["SRCISN", "SRCOKBIDN"],
            ["SOURCERPT.SRCISN", "SOURCERPT.SRCOKBIDN"],
            name="fk_sourceraw_sourcerpt",
            ondelete="RESTRICT",
        ),
    )


def _create_tax_curated() -> None:
    op.create_table(
        "TAXRPT",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("TAXCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXISN", sa.String(length=12), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXVRN", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("TAXSTS", sa.String(length=16), nullable=True),
        sa.Column("TAXYEA", sa.Integer(), nullable=True),
        sa.Column("TAXMDT", sa.Date(), nullable=True),
        sa.Column("TAXCCY", sa.String(length=3), nullable=True),
        sa.Column("TAXISB", sa.String(length=255), nullable=True),
        sa.Column("TAXGVN", sa.Date(), nullable=True),
        sa.Column("TAXGBS", sa.Date(), nullable=True),
        sa.Column("TAXBUSYEABEG", sa.Date(), nullable=True),
        sa.Column("TAXBUSYEAEND", sa.Date(), nullable=True),
        sa.Column("TAXZFL", sa.Date(), nullable=True),
        sa.Column("TAXJMS", sa.Boolean(), nullable=True),
        sa.Column("TAXAMS", sa.Boolean(), nullable=True),
        sa.Column("TAXSNW", sa.Boolean(), nullable=True),
        sa.Column("TAXKIDN", sa.BigInteger(), nullable=True),
        sa.UniqueConstraint("TAXISN", "TAXOKBIDN", name="uq_taxrpt_isin_okbidn"),
        sa.UniqueConstraint("TAXIDN", "TAXOKBIDN", name="uq_taxrpt_id_okbidn"),
        sa.ForeignKeyConstraint(["TAXISN"], ["SECMDA.SECISN"], ondelete="RESTRICT"),
    )
    op.create_index("ix_taxrpt_isin_year", "TAXRPT", ["TAXISN", "TAXYEA"], unique=False)

    op.create_table(
        "TAXLIN",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("TAXCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXCOD", sa.String(length=16), nullable=False),
        sa.Column("TAXKEY", sa.String(length=64), nullable=False),
        sa.Column("TAXNDE", sa.String(length=255), nullable=False),
        sa.Column("TAXNEN", sa.String(length=255), nullable=True),
        sa.Column("TAXORD", sa.SmallInteger(), nullable=False),
        sa.Column("TAXACT", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("TAXGVN", sa.Date(), nullable=True),
        sa.Column("TAXGBS", sa.Date(), nullable=True),
        sa.UniqueConstraint("TAXCOD", name="uq_taxlin_code"),
        sa.UniqueConstraint("TAXKEY", name="uq_taxlin_key"),
    )

    op.create_table(
        "TAXCAT",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("TAXCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXCOD", sa.String(length=16), nullable=False),
        sa.Column("TAXKEY", sa.String(length=64), nullable=False),
        sa.Column("TAXNDE", sa.String(length=255), nullable=False),
        sa.Column("TAXNEN", sa.String(length=255), nullable=True),
        sa.Column("TAXORD", sa.SmallInteger(), nullable=False),
        sa.UniqueConstraint("TAXCOD", name="uq_taxcat_code"),
        sa.UniqueConstraint("TAXKEY", name="uq_taxcat_key"),
    )

    op.create_table(
        "TAXDAT",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("TAXCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXRPTIDN", sa.Integer(), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXLINIDN", sa.Integer(), nullable=False),
        sa.Column("TAXCATIDN", sa.Integer(), nullable=False),
        sa.Column("TAXAMT", sa.Numeric(20, 10), nullable=False),
        sa.Column("TAXCCY", sa.String(length=3), nullable=True),
        sa.UniqueConstraint("TAXRPTIDN", "TAXLINIDN", "TAXCATIDN", name="uq_taxdat_point"),
        sa.ForeignKeyConstraint(["TAXLINIDN"], ["TAXLIN.TAXIDN"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["TAXCATIDN"], ["TAXCAT.TAXIDN"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["TAXRPTIDN", "TAXOKBIDN"],
            ["TAXRPT.TAXIDN", "TAXRPT.TAXOKBIDN"],
            name="fk_taxdat_taxrpt",
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_taxdat_okbidn", "TAXDAT", ["TAXOKBIDN"], unique=False)

    op.create_table(
        "TAXADJ",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("TAXCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXRPTIDN", sa.Integer(), nullable=False),
        sa.Column("TAXOKBIDN", sa.BigInteger(), nullable=False),
        sa.Column("TAXCATIDN", sa.Integer(), nullable=False),
        sa.Column("TAXCOD", sa.String(length=16), nullable=False),
        sa.Column("TAXAMT", sa.Numeric(20, 10), nullable=False),
        sa.Column("TAXCCY", sa.String(length=3), nullable=True),
        sa.UniqueConstraint("TAXRPTIDN", "TAXCATIDN", "TAXCOD", name="uq_taxadj_point"),
        sa.ForeignKeyConstraint(["TAXCATIDN"], ["TAXCAT.TAXIDN"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["TAXRPTIDN", "TAXOKBIDN"],
            ["TAXRPT.TAXIDN", "TAXRPT.TAXOKBIDN"],
            name="fk_taxadj_taxrpt",
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_taxadj_okbidn", "TAXADJ", ["TAXOKBIDN"], unique=False)

    op.create_table(
        "TAXCOR",
        sa.Column("TAXIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("TAXCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("TAXOLDRPTIDN", sa.Integer(), nullable=False),
        sa.Column("TAXNEWRPTIDN", sa.Integer(), nullable=False),
        sa.Column("TAXRSN", sa.String(length=32), nullable=True),
        sa.UniqueConstraint("TAXOLDRPTIDN", "TAXNEWRPTIDN", name="uq_taxcor_link"),
        sa.ForeignKeyConstraint(["TAXOLDRPTIDN"], ["TAXRPT.TAXIDN"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["TAXNEWRPTIDN"], ["TAXRPT.TAXIDN"], ondelete="CASCADE"),
    )


def _create_secdiv() -> None:
    op.create_table(
        "SECDIV",
        sa.Column("SECIDN", sa.Integer(), autoincrement=True, primary_key=True, nullable=False),
        sa.Column("SECCRTDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SECUPDDTS", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("SECISN", sa.String(length=12), nullable=False),
        sa.Column("SECOKBIDN", sa.BigInteger(), nullable=True),
        sa.Column("SECFLWTYP", sa.String(length=24), nullable=False),
        sa.Column("SECFLWDAT", sa.Date(), nullable=False),
        sa.Column("SECFLWAMT", sa.Numeric(20, 10), nullable=True),
        sa.Column("SECCCY", sa.String(length=3), nullable=True),
        sa.Column("SECYEA", sa.Integer(), nullable=True),
        sa.Column("SECSTS", sa.String(length=16), nullable=True),
        sa.UniqueConstraint("SECISN", "SECFLWDAT", "SECFLWTYP", "SECOKBIDN", name="uq_secdiv_event"),
        sa.ForeignKeyConstraint(["SECISN"], ["SECMDA.SECISN"], ondelete="RESTRICT"),
    )
    op.create_index("ix_secdiv_isin_flowdate", "SECDIV", ["SECISN", "SECFLWDAT"], unique=False)


def upgrade() -> None:
    # Drop legacy/obsolete shape first.
    for table_name in ("TAXCOR", "TAXADJ", "TAXDAT", "TAXCAT", "TAXLIN", "SECDIV"):
        _drop_table_if_exists(table_name)

    for table_name in ("SOURCERAW", "SOURCEAGE", "SOURCERPT"):
        _drop_table_if_exists(table_name)

    for table_name in ("TAXLST", "TAXRAW", "TAXAGE", "TAXRPT", "SECXRF"):
        _drop_table_if_exists(table_name)

    _ensure_secmda()
    _create_sourcerpt()
    _create_sourceage()
    _create_sourceraw()
    _create_tax_curated()
    _create_secdiv()


def downgrade() -> None:
    # One-way architecture rebuild migration.
    pass
