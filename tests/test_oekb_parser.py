from __future__ import annotations

from decimal import Decimal

from fondant.oekb.models import OeKBReportDetailResponse, OeKBReportListItem
from fondant.oekb.parser import build_sourceage_values


def _report() -> OeKBReportListItem:
    return OeKBReportListItem(stmId=12345, isin="IE00BMTX1Y45", statusCode="FIN", versionsNr=1)


def _detail(payload: dict) -> OeKBReportDetailResponse:
    return OeKBReportDetailResponse(
        stmId=12345,
        statusCode="FIN",
        versionsNr=1,
        waehrung="EUR",
        payload=payload,
    )


def _sourceage_values(payload: dict) -> dict:
    return build_sourceage_values("IE00BMTX1Y45", _report(), _detail(payload))


def test_build_sourceage_values_maps_bvjurperson_and_stiftung_suffix_keys() -> None:
    values = _sourceage_values(
        {
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

    assert values["korrekturbetrag_age_ak_pv_mit"] == Decimal("1.0")
    assert values["korrekturbetrag_age_ak_pv_ohne"] == Decimal("2.0")
    assert values["korrekturbetrag_age_ak_bv_mit"] == Decimal("3.0")
    assert values["korrekturbetrag_age_ak_bv_ohne"] == Decimal("4.0")
    assert values["korrekturbetrag_age_ak_bv_jur"] == Decimal("5.0")
    assert values["korrekturbetrag_age_ak_stiftung"] == Decimal("6.0")


def test_build_sourceage_values_maps_k62_and_k40() -> None:
    values = _sourceage_values(
        {
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


def test_build_sourceage_values_drops_unknown_tax_line_code() -> None:
    values = _sourceage_values(
        {
            "werte": [
                {
                    "steuerName": "StB_Unbekannte_Steuerzeile",
                    "pvMitOption4": "999.0",
                },
                {
                    "steuerName": "StB_Einkuenfte_steuerpflichtig",
                    "pvMitOption4": "1.0",
                },
            ]
        },
    )

    assert values["steuerpflichtige_einkuenfte_pv_mit"] == Decimal("1.0")
    assert Decimal("999.0") not in values.values()
    assert all("unbekannte" not in key for key in values)


def test_build_sourceage_values_drops_unknown_category_code() -> None:
    values = _sourceage_values(
        {
            "werte": [
                {
                    "steuerName": "StB_Einkuenfte_steuerpflichtig",
                    "pvMitOption4": "1.0",
                    "institutionalInvestor4": "999.0",
                }
            ]
        },
    )

    assert values["steuerpflichtige_einkuenfte_pv_mit"] == Decimal("1.0")
    assert Decimal("999.0") not in values.values()
    assert all("institutional" not in key for key in values)


def test_build_sourceage_values_drops_malformed_numeric_value() -> None:
    values = _sourceage_values(
        {
            "werte": [
                {
                    "steuerName": "StB_Einkuenfte_steuerpflichtig",
                    "pvMitOption4": "not-a-number",
                    "pvOhneOption4": "2.0",
                }
            ]
        },
    )

    assert values["steuerpflichtige_einkuenfte_pv_mit"] is None
    assert values["steuerpflichtige_einkuenfte_pv_ohne"] == Decimal("2.0")


def test_build_sourceage_values_leaves_missing_expected_value_as_none() -> None:
    values = _sourceage_values(
        {
            "werte": [
                {
                    "steuerName": "StB_Einkuenfte_steuerpflichtig",
                    "pvMitOption4": "1.0",
                }
            ]
        },
    )

    assert values["steuerpflichtige_einkuenfte_pv_mit"] == Decimal("1.0")
    assert values["steuerpflichtige_einkuenfte_pv_ohne"] is None
