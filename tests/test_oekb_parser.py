from __future__ import annotations

from decimal import Decimal

from fondant.oekb.models import OeKBReportDetailResponse, OeKBReportListItem
from fondant.oekb.parser import build_sourceage_values


def test_build_sourceage_values_maps_bvjurperson_and_stiftung_suffix_keys() -> None:
    report = OeKBReportListItem(stmId=12345, isin="IE00BMTX1Y45", statusCode="FIN", versionsNr=1)
    detail = OeKBReportDetailResponse(
        stmId=12345,
        statusCode="FIN",
        versionsNr=1,
        waehrung="EUR",
        payload={
            "werte": [
                {
                    "steuerName": "StB_Korrekturbetrag_AGErtrag_Anschaffungskosten",
                    "pvMitOption4": "1.0",
                    "pvOhneOption4": "2.0",
                    "bvMitOption4": "3.0",
                    "bvOhneOption4": "4.0",
                    "bvJurPerson4": "5.0",
                    "stiftung4": "6.0",
                }
            ]
        },
    )

    values = build_sourceage_values("IE00BMTX1Y45", report, detail)

    assert values["korrekturbetrag_age_ak_pv_mit"] == Decimal("1.0")
    assert values["korrekturbetrag_age_ak_pv_ohne"] == Decimal("2.0")
    assert values["korrekturbetrag_age_ak_bv_mit"] == Decimal("3.0")
    assert values["korrekturbetrag_age_ak_bv_ohne"] == Decimal("4.0")
    assert values["korrekturbetrag_age_ak_bv_jur"] == Decimal("5.0")
    assert values["korrekturbetrag_age_ak_stiftung"] == Decimal("6.0")


def test_build_sourceage_values_maps_k62_and_k40() -> None:
    report = OeKBReportListItem(stmId=12345, isin="IE00BMTX1Y45", statusCode="FIN", versionsNr=1)
    detail = OeKBReportDetailResponse(
        stmId=12345,
        statusCode="FIN",
        versionsNr=1,
        waehrung="EUR",
        payload={
            "werte": [
                {
                    "steuerName": "StB_Korrekturbetrag_Ausschuettung_Anschaffungskosten",
                    "pvMitOption4": "10.0",
                    "pvOhneOption4": "20.0",
                    "bvMitOption4": "30.0",
                    "bvOhneOption4": "40.0",
                    "bvJurPerson4": "50.0",
                    "stiftung4": "60.0",
                },
                {
                    "steuerName": "StB_Einkuenfte_steuerpflichtig",
                    "pvMitOption4": "1.1",
                    "pvOhneOption4": "2.2",
                    "bvMitOption4": "3.3",
                    "bvOhneOption4": "4.4",
                    "bvJurPerson4": "5.5",
                    "stiftung4": "6.6",
                },
            ]
        },
    )

    values = build_sourceage_values("IE00BMTX1Y45", report, detail)

    assert values["korrekturbetrag_aussch_ak_pv_mit"] == Decimal("10.0")
    assert values["korrekturbetrag_aussch_ak_pv_ohne"] == Decimal("20.0")
    assert values["korrekturbetrag_aussch_ak_bv_mit"] == Decimal("30.0")
    assert values["korrekturbetrag_aussch_ak_bv_ohne"] == Decimal("40.0")
    assert values["korrekturbetrag_aussch_ak_bv_jur"] == Decimal("50.0")
    assert values["korrekturbetrag_aussch_ak_stiftung"] == Decimal("60.0")

    assert values["steuerpflichtige_einkuenfte_pv_mit"] == Decimal("1.1")
    assert values["steuerpflichtige_einkuenfte_pv_ohne"] == Decimal("2.2")
    assert values["steuerpflichtige_einkuenfte_bv_mit"] == Decimal("3.3")
    assert values["steuerpflichtige_einkuenfte_bv_ohne"] == Decimal("4.4")
    assert values["steuerpflichtige_einkuenfte_bv_jur"] == Decimal("5.5")
    assert values["steuerpflichtige_einkuenfte_stiftung"] == Decimal("6.6")
