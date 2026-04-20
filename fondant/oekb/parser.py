from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fondant.oekb.models import OeKBReportDetailResponse, OeKBReportListItem, ParsedTaxAge

CATEGORY_KEY_MAP = {
    "pvmitoption4": "pv_mit",
    "pv_mit": "pv_mit",
    "pvm": "pv_mit",
    "pvohneoption4": "pv_ohne",
    "pv_ohne": "pv_ohne",
    "bvmitoption4": "bv_mit",
    "bv_mit": "bv_mit",
    "bvohneoption4": "bv_ohne",
    "bv_ohne": "bv_ohne",
    "bvjurperson4": "bv_jur",
    "bvjur": "bv_jur",
    "bv_jur": "bv_jur",
    "stiftung4": "stiftung",
    "stiftung": "stiftung",
}

METRIC_CODE_BY_KEY = {
    "steuerpflichtige_einkuenfte": "K40",
    "ag_ertraege": "K11",
    "korrekturbetrag_saldiert": "K12",
    "kest_total": "K81",
    "kest_substanzgewinne": "K82",
    "substanzgewinne_kestpfl": "K10",
    "fondsergebnis_nichtausg": "K55",
    "korrekturbetrag_age_ak": "K61",
    "korrekturbetrag_aussch_ak": "K62",
    "substanzgew_folgejahre": "K36",
    "quellensteuern_einbeh": "K21",
}

CATEGORY_CODE_BY_KEY = {
    "pv_mit": "PVM",
    "pv_ohne": "PVO",
    "bv_mit": "BVM",
    "bv_ohne": "BVO",
    "bv_jur": "BVJ",
    "stiftung": "STF",
}

TAX_FIELD_MAP = {
    "StB_Einkuenfte_steuerpflichtig": "steuerpflichtige_einkuenfte",
    "StB_E1KV_AGErtraege": "ag_ertraege",
    "StB_E1KV_Korrekturbetrag_saldiert": "korrekturbetrag_saldiert",
    "StB_KESt": "kest_total",
    "StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2": "kest_substanzgewinne",
    "StB_Substanzgewinne_KEStpflichtig": "substanzgewinne_kestpfl",
    "StB_Fondsergebnis_nichtausgeschuettet": "fondsergebnis_nichtausg",
    "StB_Korrekturbetrag_AGErtrag_Anschaffungskosten": "korrekturbetrag_age_ak",
    "StB_Korrekturbetrag_Ausschuettung_Anschaffungskosten": "korrekturbetrag_aussch_ak",
    "StB_Substanzgewinn_steuerpflichtig_beiAusschuettunginFolgejahren": "substanzgew_folgejahre",
    "StB_Abzugsteuern_einbehalten_Kapitaleinkuenfte": "quellensteuern_einbeh",
}


def build_sourcerpt_values(
    isin: str,
    report: OeKBReportListItem,
    detail: OeKBReportDetailResponse | None = None,
) -> dict[str, Any]:
    payload: Mapping[str, Any] = detail.payload if detail is not None else {}
    report_extra = report.model_extra or {}

    report_year = report.report_year or _extract_int(payload, "jahr", "reportYear", "steuerjahr")
    if report_year is None:
        report_year = _extract_year_from_mapping(report_extra)

    meldg_datum = (
        report.meldg_datum
        or _extract_date(payload, "meldgDatum", "meldedatum")
        or _extract_date_from_mapping(report_extra)
    )
    if report_year is None and meldg_datum is not None:
        report_year = meldg_datum.year

    eintragezeit = _extract_datetime(report_extra, "eintragezeit")
    if report_year is None and eintragezeit is not None:
        report_year = eintragezeit.year

    return {
        "isin": isin,
        "stm_id": report.stm_id,
        "versions_nr": report.versions_nr or (detail.versions_nr if detail is not None else None) or 1,
        "status_code": report.status_code or (detail.status_code if detail is not None else None),
        "report_year": report_year,
        "meldg_datum": meldg_datum,
        "waehrung": report.waehrung or (detail.waehrung if detail is not None else None) or payload.get("waehrung"),
        "isin_bez": report.isin_bez or payload.get("isinBez"),
        "gueltig_von": report.gueltig_von or _extract_date(payload, "gueltigVon") or _extract_date(report_extra, "gueltigVon"),
        "gueltig_bis": report.gueltig_bis or _extract_date(payload, "gueltigBis") or _extract_date(report_extra, "gueltigBis"),
        "gj_beginn": _extract_date(report_extra, "gjBeginn"),
        "gj_ende": _extract_date(report_extra, "gjEnde"),
        "eintragezeit": eintragezeit,
        "zufluss": _extract_date(report_extra, "zufluss"),
        "jahresmeldung": _extract_bool_from_mapping(
            report_extra,
            "jahresmeldung",
            "jahresMeldung",
            "meldgJahresM",
            "jahresdatenmeldung",
        ),
        "ausschuettungsmeldung": _extract_bool_from_mapping(
            report_extra,
            "ausschuettungsmeldung",
            "ausschuettungsMeldung",
            "meldgAusschuettungsM",
        ),
        "selbstnachweis": _extract_bool_from_mapping(
            report_extra,
            "selbstnachweis",
            "selbstNachweis",
        ),
        "korrigierte_stm_id": _extract_int(
            report_extra,
            "korrigierteMeldeId",
            "korrigierteMeldgId",
            "korrigierteStmId",
            "korrMeldeId",
        ),
    }


def build_sourceraw_values(
    isin: str,
    report: OeKBReportListItem,
    detail: OeKBReportDetailResponse,
) -> dict[str, Any]:
    return {
        "isin": isin,
        "stm_id": report.stm_id,
        "versions_nr": report.versions_nr or detail.versions_nr or 1,
        "payload": detail.payload,
    }


def build_sourceage_values(
    isin: str,
    report: OeKBReportListItem,
    detail: OeKBReportDetailResponse,
) -> dict[str, Any]:
    parsed = ParsedTaxAge()
    _collect_tax_values(detail.payload, parsed)
    output = parsed.model_dump()

    values: dict[str, Any] = {
        "isin": isin,
        "stm_id": report.stm_id,
        "versions_nr": report.versions_nr or detail.versions_nr or 1,
    }
    for metric, categories in output.items():
        for category, value in categories.items():
            values[f"{metric}_{category}"] = value
    return values


def _collect_tax_values(node: Any, parsed: ParsedTaxAge, current_tax_field: str | None = None) -> None:
    if isinstance(node, list):
        for item in node:
            _collect_tax_values(item, parsed, current_tax_field)
        return

    if not isinstance(node, dict):
        return

    tax_field = current_tax_field
    steuer_name = node.get("steuerName")
    if isinstance(steuer_name, str):
        tax_field = TAX_FIELD_MAP.get(steuer_name, tax_field)

    if tax_field is not None:
        for key, value in node.items():
            mapped_category = _map_category(key)
            if mapped_category is not None:
                dec = _to_decimal(value)
                if dec is not None:
                    setattr(getattr(parsed, tax_field), mapped_category, dec)

        category = _map_category(
            str(node.get("anlegerKategorie") or node.get("anlegerKat") or node.get("kategorie") or "")
        )
        if category is not None:
            amount = _to_decimal(node.get("betrag") or node.get("wert") or node.get("value"))
            if amount is not None:
                setattr(getattr(parsed, tax_field), category, amount)

    for value in node.values():
        if isinstance(value, (dict, list)):
            _collect_tax_values(value, parsed, tax_field)


def _map_category(raw_key: str) -> str | None:
    normalized = raw_key.replace("-", "_").replace(" ", "_").lower()
    return CATEGORY_KEY_MAP.get(normalized)


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        candidate = value.strip().replace(",", ".")
        if not candidate:
            return None
        try:
            return Decimal(candidate)
        except InvalidOperation:
            return None
    return None


def _extract_int(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _extract_date(payload: Mapping[str, Any], *keys: str) -> date | None:
    for key in keys:
        value = payload.get(key)
        parsed = _parse_date_value(value)
        if parsed is not None:
            return parsed
    return None


def _extract_year_from_mapping(payload: Mapping[str, Any]) -> int | None:
    explicit_keys = (
        "jahr",
        "Jahr",
        "steuerjahr",
        "reportYear",
        "meldgJahr",
        "besteuerungsjahr",
        "periodeJahr",
    )
    direct = _extract_int(payload, *explicit_keys)
    if direct is not None:
        return direct

    explicit_datetime_keys = (
        "eintragezeit",
        "meldgDatum",
        "meldedatum",
        "meldeDatum",
        "zufluss",
        "gjEnde",
        "gjBeginn",
        "gueltAb",
        "guelt",
        "gueltigBis",
        "gueltigVon",
    )
    for key in explicit_datetime_keys:
        parsed_date = _parse_date_value(payload.get(key))
        if parsed_date is not None:
            return parsed_date.year

    for key, value in payload.items():
        key_lower = key.lower()
        if ("jahr" in key_lower or "year" in key_lower) and isinstance(value, int):
            if 1900 <= value <= 3000:
                return value
        if ("jahr" in key_lower or "year" in key_lower) and isinstance(value, str) and value.isdigit():
            parsed = int(value)
            if 1900 <= parsed <= 3000:
                return parsed
        if ("jahr" in key_lower or "year" in key_lower) and isinstance(value, str):
            parsed_date = _parse_date_value(value)
            if parsed_date is not None and 1900 <= parsed_date.year <= 3000:
                return parsed_date.year
    return None


def _extract_date_from_mapping(payload: Mapping[str, Any]) -> date | None:
    explicit_keys = (
        "meldgDatum",
        "meldedatum",
        "meldeDatum",
        "gueltigVon",
        "gueltigBis",
        "behalteFristDatum",
        "eintragezeit",
        "zufluss",
        "gjBeginn",
        "gjEnde",
        "gueltAb",
        "guelt",
    )
    direct = _extract_date(payload, *explicit_keys)
    if direct is not None:
        return direct

    for key, value in payload.items():
        key_lower = key.lower()
        if any(token in key_lower for token in ("datum", "date", "gueltig", "eintrag", "zufluss", "gj")) and isinstance(value, str):
            parsed = _parse_date_value(value)
            if parsed is not None:
                return parsed
    return None


def _extract_bool_from_mapping(payload: Mapping[str, Any], *keys: str) -> bool | None:
    for key in keys:
        parsed = _parse_bool_value(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_bool_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"ja", "j", "yes", "y", "true", "1"}:
            return True
        if normalized in {"nein", "n", "no", "false", "0"}:
            return False
    return None


def _parse_date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if not candidate:
        return None
    try:
        return date.fromisoformat(candidate)
    except ValueError:
        pass

    for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(candidate, fmt).date()
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(candidate.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _extract_datetime(payload: Mapping[str, Any], *keys: str) -> datetime | None:
    for key in keys:
        parsed = _parse_datetime_value(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if not isinstance(value, str):
        return None

    candidate = value.strip()
    if not candidate:
        return None

    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed
    except ValueError:
        return None
