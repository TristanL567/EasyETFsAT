"""Safely rename legacy columns to enterprise coded names without data loss."""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import context, op

# revision identifiers, used by Alembic.
revision: str = "20260418_0002"
down_revision: str | None = "20260418_0001"
branch_labels: Sequence[str] | None = None
depends_on: Sequence[str] | None = None


def _has_table(table_name: str) -> bool:
    if context.is_offline_mode():
        return False
    inspector = sa.inspect(op.get_bind())
    return table_name in inspector.get_table_names()


def _get_columns(table_name: str) -> set[str]:
    if context.is_offline_mode():
        return set()
    inspector = sa.inspect(op.get_bind())
    return {column["name"] for column in inspector.get_columns(table_name)}


def _has_index(table_name: str, index_name: str) -> bool:
    if context.is_offline_mode():
        return False
    inspector = sa.inspect(op.get_bind())
    return any(index["name"] == index_name for index in inspector.get_indexes(table_name))


def _rename_columns(
    table_name: str,
    renames: list[tuple[str, str, sa.types.TypeEngine]],
) -> None:
    if not _has_table(table_name):
        return

    current_columns = _get_columns(table_name)
    operations: list[tuple[str, str, sa.types.TypeEngine]] = []
    for old_name, new_name, existing_type in renames:
        if old_name in current_columns and new_name not in current_columns:
            operations.append((old_name, new_name, existing_type))
            current_columns.remove(old_name)
            current_columns.add(new_name)

    if not operations:
        return

    with op.batch_alter_table(table_name) as batch_op:
        for old_name, new_name, existing_type in operations:
            batch_op.alter_column(old_name, existing_type=existing_type, new_column_name=new_name)


def upgrade() -> None:
    _rename_columns(
        "SECMDA",
        [
            ("id", "SECIDN", sa.Integer()),
            ("created_at", "SECCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "SECUPDDTS", sa.DateTime(timezone=True)),
            ("isin", "SECISN", sa.String(length=12)),
            ("name", "SECNAM", sa.String(length=255)),
            ("waehrung", "SECCCY", sa.String(length=3)),
            ("domicile_ctr", "SECCTR", sa.String(length=2)),
            ("ertragstyp", "SECERT", sa.String(length=64)),
        ],
    )

    _rename_columns(
        "SECXRF",
        [
            ("id", "SECIDN", sa.Integer()),
            ("created_at", "SECCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "SECUPDDTS", sa.DateTime(timezone=True)),
            ("isin", "SECISN", sa.String(length=12)),
            ("xref_type", "SECXTP", sa.String(length=32)),
            ("xref_value", "SECXVL", sa.String(length=128)),
            ("provider", "SECPVD", sa.String(length=64)),
        ],
    )

    _rename_columns(
        "REFCCY",
        [
            ("id", "REFIDN", sa.Integer()),
            ("created_at", "REFCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "REFUPDDTS", sa.DateTime(timezone=True)),
            ("code", "REFCOD", sa.String(length=3)),
            ("REFCCY", "REFCOD", sa.String(length=3)),
            ("name", "REFNAM", sa.String(length=64)),
            ("minor_units", "REFMUN", sa.SmallInteger()),
        ],
    )

    _rename_columns(
        "REFCTR",
        [
            ("id", "REFIDN", sa.Integer()),
            ("created_at", "REFCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "REFUPDDTS", sa.DateTime(timezone=True)),
            ("code", "REFCOD", sa.String(length=2)),
            ("REFCTR", "REFCOD", sa.String(length=2)),
            ("name_de", "REFNDE", sa.String(length=128)),
            ("name_en", "REFNEN", sa.String(length=128)),
        ],
    )

    _rename_columns(
        "TAXRPT",
        [
            ("id", "TAXIDN", sa.Integer()),
            ("created_at", "TAXCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "TAXUPDDTS", sa.DateTime(timezone=True)),
            ("isin", "TAXISN", sa.String(length=12)),
            ("stm_id", "TAXOKBIDN", sa.BigInteger()),
            ("versions_nr", "TAXVRN", sa.Integer()),
            ("status_code", "TAXSTS", sa.String(length=16)),
            ("report_year", "TAXYER", sa.Integer()),
            ("meldg_datum", "TAXMDT", sa.Date()),
            ("waehrung", "TAXCCY", sa.String(length=3)),
            ("isin_bez", "TAXISB", sa.String(length=255)),
            ("gueltig_von", "TAXGVN", sa.Date()),
            ("gueltig_bis", "TAXGBS", sa.Date()),
        ],
    )

    _rename_columns(
        "TAXAGE",
        [
            ("id", "TAXIDN", sa.Integer()),
            ("created_at", "TAXCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "TAXUPDDTS", sa.DateTime(timezone=True)),
            ("isin", "TAXISN", sa.String(length=12)),
            ("stm_id", "TAXOKBIDN", sa.BigInteger()),
            ("versions_nr", "TAXVRN", sa.Integer()),
            ("ag_ertraege_pv_mit", "TAXK11PVM", sa.Numeric(20, 10)),
            ("ag_ertraege_pv_ohne", "TAXK11PVO", sa.Numeric(20, 10)),
            ("ag_ertraege_bv_mit", "TAXK11BVM", sa.Numeric(20, 10)),
            ("ag_ertraege_bv_ohne", "TAXK11BVO", sa.Numeric(20, 10)),
            ("ag_ertraege_bv_jur", "TAXK11BVJ", sa.Numeric(20, 10)),
            ("ag_ertraege_stiftung", "TAXK11STF", sa.Numeric(20, 10)),
            ("korrekturbetrag_saldiert_pv_mit", "TAXK12PVM", sa.Numeric(20, 10)),
            ("korrekturbetrag_saldiert_pv_ohne", "TAXK12PVO", sa.Numeric(20, 10)),
            ("korrekturbetrag_saldiert_bv_mit", "TAXK12BVM", sa.Numeric(20, 10)),
            ("korrekturbetrag_saldiert_bv_ohne", "TAXK12BVO", sa.Numeric(20, 10)),
            ("korrekturbetrag_saldiert_bv_jur", "TAXK12BVJ", sa.Numeric(20, 10)),
            ("korrekturbetrag_saldiert_stiftung", "TAXK12STF", sa.Numeric(20, 10)),
            ("kest_total_pv_mit", "TAXK81PVM", sa.Numeric(20, 10)),
            ("kest_total_pv_ohne", "TAXK81PVO", sa.Numeric(20, 10)),
            ("kest_total_bv_mit", "TAXK81BVM", sa.Numeric(20, 10)),
            ("kest_total_bv_ohne", "TAXK81BVO", sa.Numeric(20, 10)),
            ("kest_total_bv_jur", "TAXK81BVJ", sa.Numeric(20, 10)),
            ("kest_total_stiftung", "TAXK81STF", sa.Numeric(20, 10)),
            ("kest_substanzgewinne_pv_mit", "TAXK82PVM", sa.Numeric(20, 10)),
            ("kest_substanzgewinne_pv_ohne", "TAXK82PVO", sa.Numeric(20, 10)),
            ("kest_substanzgewinne_bv_mit", "TAXK82BVM", sa.Numeric(20, 10)),
            ("kest_substanzgewinne_bv_ohne", "TAXK82BVO", sa.Numeric(20, 10)),
            ("kest_substanzgewinne_bv_jur", "TAXK82BVJ", sa.Numeric(20, 10)),
            ("kest_substanzgewinne_stiftung", "TAXK82STF", sa.Numeric(20, 10)),
            ("substanzgewinne_kestpfl_pv_mit", "TAXK10PVM", sa.Numeric(20, 10)),
            ("substanzgewinne_kestpfl_pv_ohne", "TAXK10PVO", sa.Numeric(20, 10)),
            ("substanzgewinne_kestpfl_bv_mit", "TAXK10BVM", sa.Numeric(20, 10)),
            ("substanzgewinne_kestpfl_bv_ohne", "TAXK10BVO", sa.Numeric(20, 10)),
            ("substanzgewinne_kestpfl_bv_jur", "TAXK10BVJ", sa.Numeric(20, 10)),
            ("substanzgewinne_kestpfl_stiftung", "TAXK10STF", sa.Numeric(20, 10)),
            ("TAXK92PVM", "TAXK10PVM", sa.Numeric(20, 10)),
            ("TAXK92PVO", "TAXK10PVO", sa.Numeric(20, 10)),
            ("TAXK92BVM", "TAXK10BVM", sa.Numeric(20, 10)),
            ("TAXK92BVO", "TAXK10BVO", sa.Numeric(20, 10)),
            ("TAXK92BVJ", "TAXK10BVJ", sa.Numeric(20, 10)),
            ("TAXK92STF", "TAXK10STF", sa.Numeric(20, 10)),
            ("fondsergebnis_nichtausg_pv_mit", "TAXK55PVM", sa.Numeric(20, 10)),
            ("fondsergebnis_nichtausg_pv_ohne", "TAXK55PVO", sa.Numeric(20, 10)),
            ("fondsergebnis_nichtausg_bv_mit", "TAXK55BVM", sa.Numeric(20, 10)),
            ("fondsergebnis_nichtausg_bv_ohne", "TAXK55BVO", sa.Numeric(20, 10)),
            ("fondsergebnis_nichtausg_bv_jur", "TAXK55BVJ", sa.Numeric(20, 10)),
            ("fondsergebnis_nichtausg_stiftung", "TAXK55STF", sa.Numeric(20, 10)),
            ("korrekturbetrag_age_ak_pv_mit", "TAXK61PVM", sa.Numeric(20, 10)),
            ("korrekturbetrag_age_ak_pv_ohne", "TAXK61PVO", sa.Numeric(20, 10)),
            ("korrekturbetrag_age_ak_bv_mit", "TAXK61BVM", sa.Numeric(20, 10)),
            ("korrekturbetrag_age_ak_bv_ohne", "TAXK61BVO", sa.Numeric(20, 10)),
            ("korrekturbetrag_age_ak_bv_jur", "TAXK61BVJ", sa.Numeric(20, 10)),
            ("korrekturbetrag_age_ak_stiftung", "TAXK61STF", sa.Numeric(20, 10)),
            ("substanzgew_folgejahre_pv_mit", "TAXK36PVM", sa.Numeric(20, 10)),
            ("substanzgew_folgejahre_pv_ohne", "TAXK36PVO", sa.Numeric(20, 10)),
            ("substanzgew_folgejahre_bv_mit", "TAXK36BVM", sa.Numeric(20, 10)),
            ("substanzgew_folgejahre_bv_ohne", "TAXK36BVO", sa.Numeric(20, 10)),
            ("substanzgew_folgejahre_bv_jur", "TAXK36BVJ", sa.Numeric(20, 10)),
            ("substanzgew_folgejahre_stiftung", "TAXK36STF", sa.Numeric(20, 10)),
            ("quellensteuern_einbeh_pv_mit", "TAXK21PVM", sa.Numeric(20, 10)),
            ("quellensteuern_einbeh_pv_ohne", "TAXK21PVO", sa.Numeric(20, 10)),
            ("quellensteuern_einbeh_bv_mit", "TAXK21BVM", sa.Numeric(20, 10)),
            ("quellensteuern_einbeh_bv_ohne", "TAXK21BVO", sa.Numeric(20, 10)),
            ("quellensteuern_einbeh_bv_jur", "TAXK21BVJ", sa.Numeric(20, 10)),
            ("quellensteuern_einbeh_stiftung", "TAXK21STF", sa.Numeric(20, 10)),
        ],
    )

    _rename_columns(
        "TAXRAW",
        [
            ("id", "TAXIDN", sa.Integer()),
            ("created_at", "TAXCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "TAXUPDDTS", sa.DateTime(timezone=True)),
            ("isin", "TAXISN", sa.String(length=12)),
            ("stm_id", "TAXOKBIDN", sa.BigInteger()),
            ("versions_nr", "TAXVRN", sa.Integer()),
            ("payload", "TAXPAY", sa.JSON()),
        ],
    )

    _rename_columns(
        "IMPLOG",
        [
            ("id", "IMPIDN", sa.Integer()),
            ("created_at", "IMPCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "IMPUPDDTS", sa.DateTime(timezone=True)),
            ("run_id", "IMPRUNIDN", sa.Uuid()),
            ("isin", "IMPISN", sa.String(length=12)),
            ("stm_id", "IMPOKBIDN", sa.Integer()),
            ("status", "IMPSTS", sa.String(length=24)),
            ("message", "IMPMSG", sa.Text()),
            ("records_seen", "IMPRSN", sa.Integer()),
            ("records_written", "IMPRSW", sa.Integer()),
            ("started_at", "IMPSTADTS", sa.DateTime(timezone=True)),
            ("finished_at", "IMPFINDTS", sa.DateTime(timezone=True)),
        ],
    )

    _rename_columns(
        "IMPERR",
        [
            ("id", "IMPIDN", sa.Integer()),
            ("created_at", "IMPCRTDTS", sa.DateTime(timezone=True)),
            ("updated_at", "IMPUPDDTS", sa.DateTime(timezone=True)),
            ("run_id", "IMPRUNIDN", sa.Uuid()),
            ("isin", "IMPISN", sa.String(length=12)),
            ("stm_id", "IMPOKBIDN", sa.Integer()),
            ("stage", "IMPSTG", sa.String(length=64)),
            ("error_code", "IMPECD", sa.String(length=64)),
            ("error_message", "IMPEMS", sa.Text()),
            ("payload", "IMPPAY", sa.JSON()),
        ],
    )

    if _has_table("TAXRPT") and _has_index("TAXRPT", "ix_taxrpt_isin_year"):
        op.drop_index("ix_taxrpt_isin_year", table_name="TAXRPT")
        taxrpt_columns = _get_columns("TAXRPT")
        if {"TAXISN", "TAXYER"}.issubset(taxrpt_columns):
            op.create_index("ix_taxrpt_isin_year", "TAXRPT", ["TAXISN", "TAXYER"], unique=False)


def downgrade() -> None:
    # Non-destructive one-way rename migration.
    pass
