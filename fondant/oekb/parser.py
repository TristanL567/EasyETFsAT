from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fondant import tax_registry
from fondant.oekb.models import OeKBReportDetailResponse, OeKBReportListItem, ParsedTaxAge

CATEGORY_CODE_BY_KEY = tax_registry.CATEGORY_CODE_BY_KEY
CATEGORY_KEY_MAP = tax_registry.CATEGORY_KEY_MAP
METRIC_CODE_BY_KEY = tax_registry.METRIC_CODE_BY_KEY
TAX_FIELD_MAP = tax_registry.TAX_FIELD_MAP

TAX_VALUE_STRUCTURAL_KEYS = {
    "steuerName",
    "anlegerKategorie",
    "anlegerKat",
    "kategorie",
    "betrag",
    "wert",
    "value",
}


@dataclass(frozen=True, slots=True)
class ParserDiagnostic:
    code: str
    path: tuple[str, ...]
    raw_key: str | None = None
    raw_value: Any = None
    tax_field: str | None = None


@dataclass(frozen=True, slots=True)
class SourceAgeParseResult:
    values: dict[str, Any]
    diagnostics: tuple[ParserDiagnostic, ...]


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
    return build_sourceage_result(isin=isin, report=report, detail=detail).values


def build_sourceage_result(
    isin: str,
    report: OeKBReportListItem,
    detail: OeKBReportDetailResponse,
) -> SourceAgeParseResult:
    parsed = ParsedTaxAge()
    diagnostics: list[ParserDiagnostic] = []
    _collect_tax_values(detail.payload, parsed, diagnostics=diagnostics)
    output = parsed.model_dump()

    values: dict[str, Any] = {
        "isin": isin,
        "stm_id": report.stm_id,
        "versions_nr": report.versions_nr or detail.versions_nr or 1,
    }
    for metric, categories in output.items():
        for category, value in categories.items():
            values[f"{metric}_{category}"] = value
    return SourceAgeParseResult(values=values, diagnostics=tuple(diagnostics))


def _collect_tax_values(
    node: Any,
    parsed: ParsedTaxAge,
    current_tax_field: str | None = None,
    *,
    diagnostics: list[ParserDiagnostic],
    path: tuple[str, ...] = (),
) -> None:
    if isinstance(node, list):
        for index, item in enumerate(node):
            _collect_tax_values(
                item,
                parsed,
                current_tax_field,
                diagnostics=diagnostics,
                path=(*path, str(index)),
            )
        return

    if not isinstance(node, dict):
        return

    tax_field = current_tax_field
    steuer_name = node.get("steuerName")
    if isinstance(steuer_name, str):
        mapped_tax_field = TAX_FIELD_MAP.get(steuer_name)
        if mapped_tax_field is None:
            diagnostics.append(
                ParserDiagnostic(
                    code="unknown_tax_field",
                    path=(*path, "steuerName"),
                    raw_key="steuerName",
                    raw_value=steuer_name,
                    tax_field=current_tax_field,
                )
            )
        else:
            tax_field = mapped_tax_field

    if tax_field is not None:
        for key, value in node.items():
            mapped_category = _map_category(key)
            if mapped_category is not None:
                dec = _to_decimal(value)
                if dec is not None:
                    setattr(getattr(parsed, tax_field), mapped_category, dec)
                elif _is_invalid_decimal_value(value):
                    diagnostics.append(
                        ParserDiagnostic(
                            code="invalid_numeric_value",
                            path=(*path, key),
                            raw_key=key,
                            raw_value=value,
                            tax_field=tax_field,
                        )
                    )
            elif key not in TAX_VALUE_STRUCTURAL_KEYS and not isinstance(value, (dict, list)):
                diagnostics.append(
                    ParserDiagnostic(
                        code="unknown_category",
                        path=(*path, key),
                        raw_key=key,
                        raw_value=value,
                        tax_field=tax_field,
                    )
                )

        category_key = str(
            node.get("anlegerKategorie") or node.get("anlegerKat") or node.get("kategorie") or ""
        )
        category = _map_category(category_key)
        if category is None and category_key:
            diagnostics.append(
                ParserDiagnostic(
                    code="unknown_category",
                    path=(*path, "anlegerKategorie"),
                    raw_key="anlegerKategorie",
                    raw_value=category_key,
                    tax_field=tax_field,
                )
            )
        elif category is not None:
            amount_key = next((key for key in ("betrag", "wert", "value") if node.get(key)), None)
            amount_value = node.get(amount_key) if amount_key is not None else None
            amount = _to_decimal(amount_value)
            if amount is not None:
                setattr(getattr(parsed, tax_field), category, amount)
            elif _is_invalid_decimal_value(amount_value):
                diagnostics.append(
                    ParserDiagnostic(
                        code="invalid_numeric_value",
                        path=(*path, amount_key or "betrag"),
                        raw_key=amount_key,
                        raw_value=amount_value,
                        tax_field=tax_field,
                    )
                )

    for key, value in node.items():
        if isinstance(value, (dict, list)):
            _collect_tax_values(
                value,
                parsed,
                tax_field,
                diagnostics=diagnostics,
                path=(*path, key),
            )


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


def _is_invalid_decimal_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return _to_decimal(value) is None


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
