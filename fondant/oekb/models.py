from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class OeKBBaseModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="allow")


class OeKBReportListItem(OeKBBaseModel):
    stm_id: int = Field(alias="stmId")
    isin: str | None = None
    status_code: str | None = Field(default=None, alias="statusCode")
    versions_nr: int | None = Field(default=None, alias="versionsNr")
    waehrung: str | None = None
    isin_bez: str | None = Field(default=None, alias="isinBez")
    meldg_datum: date | None = Field(default=None, alias="meldgDatum")
    report_year: int | None = Field(default=None, alias="jahr")
    gueltig_von: date | None = Field(default=None, alias="gueltigVon")
    gueltig_bis: date | None = Field(default=None, alias="gueltigBis")


class OeKBReportDetailResponse(OeKBBaseModel):
    stm_id: int | None = Field(default=None, alias="stmId")
    status_code: str | None = Field(default=None, alias="statusCode")
    versions_nr: int | None = Field(default=None, alias="versionsNr")
    waehrung: str | None = None
    payload: dict[str, Any]


class ParsedCategoryValues(OeKBBaseModel):
    pv_mit: Decimal | None = None
    pv_ohne: Decimal | None = None
    bv_mit: Decimal | None = None
    bv_ohne: Decimal | None = None
    bv_jur: Decimal | None = None
    stiftung: Decimal | None = None


class ParsedTaxAge(OeKBBaseModel):
    steuerpflichtige_einkuenfte: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    ag_ertraege: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    korrekturbetrag_saldiert: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    kest_total: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    kest_substanzgewinne: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    substanzgewinne_kestpfl: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    fondsergebnis_nichtausg: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    korrekturbetrag_age_ak: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    korrekturbetrag_aussch_ak: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    substanzgew_folgejahre: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
    quellensteuern_einbeh: ParsedCategoryValues = Field(default_factory=ParsedCategoryValues)
