from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import (
    BIGINT,
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from fondant.db.base import Base, IdTimestampMixin


def _payload_json_type() -> JSONB | JSON:
    return JSON().with_variant(JSONB, "postgresql")


class SOURCERPT(IdTimestampMixin, Base):
    __tablename__ = "SOURCERPT"
    DOMAIN_PREFIX = "SRC"
    __table_args__ = (
        UniqueConstraint("SRCISN", "SRCOKBIDN", name="uq_sourcerpt_isin_okbidn"),
        Index("ix_sourcerpt_isin_year", "SRCISN", "SRCYEA"),
    )

    isin: Mapped[str] = mapped_column("SRCISN", String(12), nullable=False)
    stm_id: Mapped[int] = mapped_column("SRCOKBIDN", BIGINT, nullable=False)
    versions_nr: Mapped[int] = mapped_column("SRCVRN", Integer, nullable=False, default=1)
    status_code: Mapped[str | None] = mapped_column("SRCSTS", String(16))
    report_year: Mapped[int | None] = mapped_column("SRCYEA", Integer)
    meldg_datum: Mapped[date | None] = mapped_column("SRCMDT", Date)
    waehrung: Mapped[str | None] = mapped_column("SRCCCY", String(3))
    isin_bez: Mapped[str | None] = mapped_column("SRCISB", String(255))
    gueltig_von: Mapped[date | None] = mapped_column("SRCGVN", Date)
    gueltig_bis: Mapped[date | None] = mapped_column("SRCGBS", Date)
    gj_beginn: Mapped[date | None] = mapped_column("SRCBUSYEABEG", Date)
    gj_ende: Mapped[date | None] = mapped_column("SRCBUSYEAEND", Date)
    eintragezeit: Mapped[datetime | None] = mapped_column("SRCENTDTS", DateTime(timezone=False))
    zufluss: Mapped[date | None] = mapped_column("SRCZFL", Date)
    jahresmeldung: Mapped[bool | None] = mapped_column("SRCJMS", Boolean)
    ausschuettungsmeldung: Mapped[bool | None] = mapped_column("SRCAMS", Boolean)
    selbstnachweis: Mapped[bool | None] = mapped_column("SRCSNW", Boolean)
    korrigierte_stm_id: Mapped[int | None] = mapped_column("SRCKIDN", BIGINT)


class SOURCEAGE(IdTimestampMixin, Base):
    __tablename__ = "SOURCEAGE"
    DOMAIN_PREFIX = "SRC"
    __table_args__ = (
        UniqueConstraint("SRCISN", "SRCOKBIDN", name="uq_sourceage_isin_okbidn"),
        Index("ix_sourceage_isin_year", "SRCISN", "SRCYEA"),
        ForeignKeyConstraint(
            ["SRCISN", "SRCOKBIDN"],
            ["SOURCERPT.SRCISN", "SOURCERPT.SRCOKBIDN"],
            name="fk_sourceage_sourcerpt",
            ondelete="RESTRICT",
        ),
    )

    isin: Mapped[str] = mapped_column("SRCISN", String(12), nullable=False)
    stm_id: Mapped[int] = mapped_column("SRCOKBIDN", BIGINT, nullable=False)
    versions_nr: Mapped[int] = mapped_column("SRCVRN", Integer, nullable=False, default=1)
    report_year: Mapped[int | None] = mapped_column("SRCYEA", Integer)

    steuerpflichtige_einkuenfte_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK40PVM", Numeric(20, 10))
    steuerpflichtige_einkuenfte_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK40PVO", Numeric(20, 10))
    steuerpflichtige_einkuenfte_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK40BVM", Numeric(20, 10))
    steuerpflichtige_einkuenfte_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK40BVO", Numeric(20, 10))
    steuerpflichtige_einkuenfte_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK40BVJ", Numeric(20, 10))
    steuerpflichtige_einkuenfte_stiftung: Mapped[Decimal | None] = mapped_column("SRCK40STF", Numeric(20, 10))

    ag_ertraege_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK11PVM", Numeric(20, 10))
    ag_ertraege_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK11PVO", Numeric(20, 10))
    ag_ertraege_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK11BVM", Numeric(20, 10))
    ag_ertraege_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK11BVO", Numeric(20, 10))
    ag_ertraege_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK11BVJ", Numeric(20, 10))
    ag_ertraege_stiftung: Mapped[Decimal | None] = mapped_column("SRCK11STF", Numeric(20, 10))

    korrekturbetrag_saldiert_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK12PVM", Numeric(20, 10))
    korrekturbetrag_saldiert_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK12PVO", Numeric(20, 10))
    korrekturbetrag_saldiert_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK12BVM", Numeric(20, 10))
    korrekturbetrag_saldiert_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK12BVO", Numeric(20, 10))
    korrekturbetrag_saldiert_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK12BVJ", Numeric(20, 10))
    korrekturbetrag_saldiert_stiftung: Mapped[Decimal | None] = mapped_column("SRCK12STF", Numeric(20, 10))

    kest_total_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK81PVM", Numeric(20, 10))
    kest_total_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK81PVO", Numeric(20, 10))
    kest_total_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK81BVM", Numeric(20, 10))
    kest_total_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK81BVO", Numeric(20, 10))
    kest_total_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK81BVJ", Numeric(20, 10))
    kest_total_stiftung: Mapped[Decimal | None] = mapped_column("SRCK81STF", Numeric(20, 10))

    kest_substanzgewinne_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK82PVM", Numeric(20, 10))
    kest_substanzgewinne_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK82PVO", Numeric(20, 10))
    kest_substanzgewinne_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK82BVM", Numeric(20, 10))
    kest_substanzgewinne_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK82BVO", Numeric(20, 10))
    kest_substanzgewinne_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK82BVJ", Numeric(20, 10))
    kest_substanzgewinne_stiftung: Mapped[Decimal | None] = mapped_column("SRCK82STF", Numeric(20, 10))

    substanzgewinne_kestpfl_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK10PVM", Numeric(20, 10))
    substanzgewinne_kestpfl_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK10PVO", Numeric(20, 10))
    substanzgewinne_kestpfl_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK10BVM", Numeric(20, 10))
    substanzgewinne_kestpfl_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK10BVO", Numeric(20, 10))
    substanzgewinne_kestpfl_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK10BVJ", Numeric(20, 10))
    substanzgewinne_kestpfl_stiftung: Mapped[Decimal | None] = mapped_column("SRCK10STF", Numeric(20, 10))

    fondsergebnis_nichtausg_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK55PVM", Numeric(20, 10))
    fondsergebnis_nichtausg_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK55PVO", Numeric(20, 10))
    fondsergebnis_nichtausg_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK55BVM", Numeric(20, 10))
    fondsergebnis_nichtausg_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK55BVO", Numeric(20, 10))
    fondsergebnis_nichtausg_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK55BVJ", Numeric(20, 10))
    fondsergebnis_nichtausg_stiftung: Mapped[Decimal | None] = mapped_column("SRCK55STF", Numeric(20, 10))

    korrekturbetrag_age_ak_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK61PVM", Numeric(20, 10))
    korrekturbetrag_age_ak_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK61PVO", Numeric(20, 10))
    korrekturbetrag_age_ak_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK61BVM", Numeric(20, 10))
    korrekturbetrag_age_ak_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK61BVO", Numeric(20, 10))
    korrekturbetrag_age_ak_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK61BVJ", Numeric(20, 10))
    korrekturbetrag_age_ak_stiftung: Mapped[Decimal | None] = mapped_column("SRCK61STF", Numeric(20, 10))

    korrekturbetrag_aussch_ak_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK62PVM", Numeric(20, 10))
    korrekturbetrag_aussch_ak_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK62PVO", Numeric(20, 10))
    korrekturbetrag_aussch_ak_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK62BVM", Numeric(20, 10))
    korrekturbetrag_aussch_ak_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK62BVO", Numeric(20, 10))
    korrekturbetrag_aussch_ak_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK62BVJ", Numeric(20, 10))
    korrekturbetrag_aussch_ak_stiftung: Mapped[Decimal | None] = mapped_column("SRCK62STF", Numeric(20, 10))

    substanzgew_folgejahre_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK36PVM", Numeric(20, 10))
    substanzgew_folgejahre_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK36PVO", Numeric(20, 10))
    substanzgew_folgejahre_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK36BVM", Numeric(20, 10))
    substanzgew_folgejahre_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK36BVO", Numeric(20, 10))
    substanzgew_folgejahre_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK36BVJ", Numeric(20, 10))
    substanzgew_folgejahre_stiftung: Mapped[Decimal | None] = mapped_column("SRCK36STF", Numeric(20, 10))

    quellensteuern_einbeh_pv_mit: Mapped[Decimal | None] = mapped_column("SRCK21PVM", Numeric(20, 10))
    quellensteuern_einbeh_pv_ohne: Mapped[Decimal | None] = mapped_column("SRCK21PVO", Numeric(20, 10))
    quellensteuern_einbeh_bv_mit: Mapped[Decimal | None] = mapped_column("SRCK21BVM", Numeric(20, 10))
    quellensteuern_einbeh_bv_ohne: Mapped[Decimal | None] = mapped_column("SRCK21BVO", Numeric(20, 10))
    quellensteuern_einbeh_bv_jur: Mapped[Decimal | None] = mapped_column("SRCK21BVJ", Numeric(20, 10))
    quellensteuern_einbeh_stiftung: Mapped[Decimal | None] = mapped_column("SRCK21STF", Numeric(20, 10))


class SOURCERAW(IdTimestampMixin, Base):
    __tablename__ = "SOURCERAW"
    DOMAIN_PREFIX = "SRC"
    __table_args__ = (
        UniqueConstraint("SRCISN", "SRCOKBIDN", name="uq_sourceraw_isin_okbidn"),
        ForeignKeyConstraint(
            ["SRCISN", "SRCOKBIDN"],
            ["SOURCERPT.SRCISN", "SOURCERPT.SRCOKBIDN"],
            name="fk_sourceraw_sourcerpt",
            ondelete="RESTRICT",
        ),
    )

    isin: Mapped[str] = mapped_column("SRCISN", String(12), nullable=False)
    stm_id: Mapped[int] = mapped_column("SRCOKBIDN", BIGINT, nullable=False)
    versions_nr: Mapped[int] = mapped_column("SRCVRN", Integer, nullable=False, default=1)
    payload: Mapped[dict] = mapped_column("SRCPAY", _payload_json_type(), nullable=False)


class TAXRPT(IdTimestampMixin, Base):
    __tablename__ = "TAXRPT"
    DOMAIN_PREFIX = "TAX"
    __table_args__ = (
        UniqueConstraint("TAXISN", "TAXOKBIDN", name="uq_taxrpt_isin_okbidn"),
        UniqueConstraint("TAXIDN", "TAXOKBIDN", name="uq_taxrpt_id_okbidn"),
        Index("ix_taxrpt_isin_year", "TAXISN", "TAXYEA"),
    )

    isin: Mapped[str] = mapped_column(
        "TAXISN",
        String(12),
        ForeignKey("SECMDA.SECISN", ondelete="RESTRICT"),
        nullable=False,
    )
    stm_id: Mapped[int] = mapped_column("TAXOKBIDN", BIGINT, nullable=False)
    versions_nr: Mapped[int] = mapped_column("TAXVRN", Integer, nullable=False, default=1)
    status_code: Mapped[str | None] = mapped_column("TAXSTS", String(16))
    report_year: Mapped[int | None] = mapped_column("TAXYEA", Integer)
    meldg_datum: Mapped[date | None] = mapped_column("TAXMDT", Date)
    waehrung: Mapped[str | None] = mapped_column("TAXCCY", String(3))
    isin_bez: Mapped[str | None] = mapped_column("TAXISB", String(255))
    gueltig_von: Mapped[date | None] = mapped_column("TAXGVN", Date)
    gueltig_bis: Mapped[date | None] = mapped_column("TAXGBS", Date)
    bus_year_begin: Mapped[date | None] = mapped_column("TAXBUSYEABEG", Date)
    bus_year_end: Mapped[date | None] = mapped_column("TAXBUSYEAEND", Date)
    zufluss: Mapped[date | None] = mapped_column("TAXZFL", Date)
    jahresmeldung: Mapped[bool | None] = mapped_column("TAXJMS", Boolean)
    ausschuettungsmeldung: Mapped[bool | None] = mapped_column("TAXAMS", Boolean)
    selbstnachweis: Mapped[bool | None] = mapped_column("TAXSNW", Boolean)
    korrigierte_stm_id: Mapped[int | None] = mapped_column("TAXKIDN", BIGINT)


class TAXLIN(IdTimestampMixin, Base):
    __tablename__ = "TAXLIN"
    DOMAIN_PREFIX = "TAX"
    __table_args__ = (
        UniqueConstraint("TAXCOD", name="uq_taxlin_code"),
        UniqueConstraint("TAXKEY", name="uq_taxlin_key"),
    )

    line_code: Mapped[str] = mapped_column("TAXCOD", String(16), nullable=False)
    metric_key: Mapped[str] = mapped_column("TAXKEY", String(64), nullable=False)
    name_de: Mapped[str] = mapped_column("TAXNDE", String(255), nullable=False)
    name_en: Mapped[str | None] = mapped_column("TAXNEN", String(255))
    line_order: Mapped[int] = mapped_column("TAXORD", SmallInteger, nullable=False)
    is_active: Mapped[bool] = mapped_column("TAXACT", Boolean, nullable=False, default=True)
    valid_from: Mapped[date | None] = mapped_column("TAXGVN", Date)
    valid_to: Mapped[date | None] = mapped_column("TAXGBS", Date)


class TAXCAT(IdTimestampMixin, Base):
    __tablename__ = "TAXCAT"
    DOMAIN_PREFIX = "TAX"
    __table_args__ = (
        UniqueConstraint("TAXCOD", name="uq_taxcat_code"),
        UniqueConstraint("TAXKEY", name="uq_taxcat_key"),
    )

    category_code: Mapped[str] = mapped_column("TAXCOD", String(16), nullable=False)
    category_key: Mapped[str] = mapped_column("TAXKEY", String(64), nullable=False)
    name_de: Mapped[str] = mapped_column("TAXNDE", String(255), nullable=False)
    name_en: Mapped[str | None] = mapped_column("TAXNEN", String(255))
    category_order: Mapped[int] = mapped_column("TAXORD", SmallInteger, nullable=False)


class TAXDAT(IdTimestampMixin, Base):
    __tablename__ = "TAXDAT"
    DOMAIN_PREFIX = "TAX"
    __table_args__ = (
        UniqueConstraint("TAXRPTIDN", "TAXLINIDN", "TAXCATIDN", name="uq_taxdat_point"),
        Index("ix_taxdat_okbidn", "TAXOKBIDN"),
        ForeignKeyConstraint(
            ["TAXRPTIDN", "TAXOKBIDN"],
            ["TAXRPT.TAXIDN", "TAXRPT.TAXOKBIDN"],
            name="fk_taxdat_taxrpt",
            ondelete="CASCADE",
        ),
    )

    taxrpt_id: Mapped[int] = mapped_column("TAXRPTIDN", Integer, nullable=False)
    okb_id: Mapped[int] = mapped_column("TAXOKBIDN", BIGINT, nullable=False)
    taxlin_id: Mapped[int] = mapped_column(
        "TAXLINIDN",
        Integer,
        ForeignKey("TAXLIN.TAXIDN", ondelete="RESTRICT"),
        nullable=False,
    )
    taxcat_id: Mapped[int] = mapped_column(
        "TAXCATIDN",
        Integer,
        ForeignKey("TAXCAT.TAXIDN", ondelete="RESTRICT"),
        nullable=False,
    )
    amount: Mapped[Decimal] = mapped_column("TAXAMT", Numeric(20, 10), nullable=False)
    waehrung: Mapped[str | None] = mapped_column("TAXCCY", String(3))


class TAXADJ(IdTimestampMixin, Base):
    __tablename__ = "TAXADJ"
    DOMAIN_PREFIX = "TAX"
    __table_args__ = (
        UniqueConstraint("TAXRPTIDN", "TAXCATIDN", "TAXCOD", name="uq_taxadj_point"),
        Index("ix_taxadj_okbidn", "TAXOKBIDN"),
        ForeignKeyConstraint(
            ["TAXRPTIDN", "TAXOKBIDN"],
            ["TAXRPT.TAXIDN", "TAXRPT.TAXOKBIDN"],
            name="fk_taxadj_taxrpt",
            ondelete="CASCADE",
        ),
    )

    taxrpt_id: Mapped[int] = mapped_column("TAXRPTIDN", Integer, nullable=False)
    okb_id: Mapped[int] = mapped_column("TAXOKBIDN", BIGINT, nullable=False)
    taxcat_id: Mapped[int] = mapped_column(
        "TAXCATIDN",
        Integer,
        ForeignKey("TAXCAT.TAXIDN", ondelete="RESTRICT"),
        nullable=False,
    )
    adj_code: Mapped[str] = mapped_column("TAXCOD", String(16), nullable=False)
    amount: Mapped[Decimal] = mapped_column("TAXAMT", Numeric(20, 10), nullable=False)
    waehrung: Mapped[str | None] = mapped_column("TAXCCY", String(3))


class TAXCOR(IdTimestampMixin, Base):
    __tablename__ = "TAXCOR"
    DOMAIN_PREFIX = "TAX"
    __table_args__ = (
        UniqueConstraint("TAXOLDRPTIDN", "TAXNEWRPTIDN", name="uq_taxcor_link"),
    )

    old_taxrpt_id: Mapped[int] = mapped_column(
        "TAXOLDRPTIDN",
        Integer,
        ForeignKey("TAXRPT.TAXIDN", ondelete="CASCADE"),
        nullable=False,
    )
    new_taxrpt_id: Mapped[int] = mapped_column(
        "TAXNEWRPTIDN",
        Integer,
        ForeignKey("TAXRPT.TAXIDN", ondelete="CASCADE"),
        nullable=False,
    )
    reason_code: Mapped[str | None] = mapped_column("TAXRSN", String(32))
