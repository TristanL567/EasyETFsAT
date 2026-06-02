from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from fondant.tax_registry import TAX_LINES

ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
ALL_AVAILABLE_YEARS = "all_available_years"
MOST_RECENT_COMMON_AVAILABLE_YEAR = "most_recent_common_available_year"

LEGAL_ENTITY_SUFFIXES = MappingProxyType(
    {
        "natural person": ("PVM", "PVO"),
        "business": ("BVM", "BVO", "BVJ"),
        "Stiftung": ("STI",),
    }
)

LEGAL_ENTITY_SUBCATEGORY_SUFFIXES = MappingProxyType(
    {
        "natural person": MappingProxyType(
            {
                "natural_person_pa_with_option": ("PVM",),
                "natural_person_pa_without_option": ("PVO",),
                "natural_person_all": ("PVM", "PVO"),
            }
        ),
        "business": MappingProxyType(
            {
                "business_bv_with_option": ("BVM",),
                "business_bv_without_option": ("BVO",),
                "business_bv_legal_person": ("BVJ",),
                "business_all": ("BVM", "BVO", "BVJ"),
            }
        ),
        "Stiftung": MappingProxyType(
            {
                "stiftung": ("STI",),
            }
        ),
    }
)

DEFAULT_SUBCATEGORY_KEYS = MappingProxyType(
    {
        "natural person": "natural_person_all",
        "business": "business_all",
        "Stiftung": "stiftung",
    }
)

TAX_FIELD_LABEL_OVERRIDES = MappingProxyType(
    {
        "K40": "Taxable income",
        "K61": "Cost-basis adjustment",
        "K62": "Distribution cost-basis adjustment",
    }
)

TAX_FIELD_LABELS = MappingProxyType(
    {tax_line.line_code: TAX_FIELD_LABEL_OVERRIDES.get(tax_line.line_code, tax_line.description) for tax_line in TAX_LINES}
)

DEFAULT_TAX_FIELDS = ("K40", "K61", "K62")

IDENTITY_COLUMNS = ("TAXISN", "TAXOKBIDN", "TAXYEA", "FNDCCY", "TAXMDT", "FXRAT")
AMOUNT_COLUMN_BASES = tuple(
    f"{field_code}{suffix}"
    for field_code in TAX_FIELD_LABELS
    for suffixes in LEGAL_ENTITY_SUFFIXES.values()
    for suffix in suffixes
)
AMOUNT_COLUMNS = tuple(
    f"{column_base}_{currency_suffix}"
    for column_base in AMOUNT_COLUMN_BASES
    for currency_suffix in ("HOMCCY", "EUR")
)

V2_TAXDATHOMCCY = sa.Table(
    "V2_TAXDATHOMCCY",
    sa.MetaData(),
    sa.Column("TAXISN", sa.String(12)),
    sa.Column("TAXOKBIDN", sa.BigInteger()),
    sa.Column("TAXYEA", sa.Integer()),
    sa.Column("FNDCCY", sa.String(3)),
    sa.Column("TAXMDT", sa.Date()),
    sa.Column("FXRAT", sa.Numeric(20, 10)),
    *(sa.Column(column_name, sa.Numeric(20, 10)) for column_name in AMOUNT_COLUMNS),
)


class BusinessQueryValidationError(ValueError):
    """Raised when a structured BusinessQuery input is outside the whitelist."""


@dataclass(frozen=True)
class BusinessQueryPosition:
    isin: str
    amount: Decimal


@dataclass(frozen=True)
class BusinessQueryInput:
    query_name: str
    isins: tuple[str, ...]
    legal_entity_type: str
    amount_multiplier: Decimal
    tax_fields: tuple[str, ...] = DEFAULT_TAX_FIELDS
    year: int | None = None
    year_from: int | None = None
    year_to: int | None = None
    subcategory_key: str | None = None
    tax_year_filter: str | int | None = None
    positions: tuple[BusinessQueryPosition, ...] = ()


@dataclass(frozen=True)
class BusinessQueryResultRow:
    query_name: str
    isin: str
    tax_year: int | None
    oekb_report_id: int
    fund_currency: str | None
    report_date: date | None
    fx_rate: Decimal | None
    legal_entity_category: str
    tax_field_code: str
    tax_field_label: str
    base_eur_value: Decimal | None
    amount_multiplier: Decimal
    calculated_eur_value: Decimal | None
    original_currency_code: str | None = None
    home_currency_code: str | None = None
    fx_date: date | None = None
    base_home_currency_value: Decimal | None = None
    calculated_home_currency_value: Decimal | None = None


@dataclass(frozen=True)
class BusinessQueryResult:
    query: BusinessQueryInput
    rows: tuple[BusinessQueryResultRow, ...] = field(default_factory=tuple)
    missing_year_isins: tuple[str, ...] = field(default_factory=tuple)
    no_common_year_isins: tuple[str, ...] = field(default_factory=tuple)

    @property
    def missing_year_messages(self) -> tuple[str, ...]:
        return tuple(
            f"Data for ISIN {isin} is not available for the selected year." for isin in self.missing_year_isins
        )

    @property
    def no_common_year_messages(self) -> tuple[str, ...]:
        if not self.no_common_year_isins:
            return ()
        joined_isins = ", ".join(self.no_common_year_isins)
        return (f"No common tax year is available for every submitted ISIN: {joined_isins}.",)

    @property
    def count(self) -> int:
        return len(self.rows)

    @property
    def is_empty(self) -> bool:
        return not self.rows


def validate_business_query_input(query: BusinessQueryInput) -> BusinessQueryInput:
    query_name = query.query_name.strip()
    if not query_name:
        raise BusinessQueryValidationError("query_name is required")

    positions = _normalize_positions(query.positions)
    isins = _normalize_isins(query.isins)
    if positions:
        position_isins = tuple(position.isin for position in positions)
        if isins and set(isins) != set(position_isins):
            raise BusinessQueryValidationError("positions and isins must reference the same ISIN set")
        isins = position_isins
    if not isins:
        raise BusinessQueryValidationError("at least one ISIN is required")

    if query.legal_entity_type not in LEGAL_ENTITY_SUFFIXES:
        raise BusinessQueryValidationError("unsupported legal entity type")
    subcategory_key = _normalize_subcategory_key(query.legal_entity_type, query.subcategory_key)

    amount_multiplier = _normalize_decimal(query.amount_multiplier, "amount_multiplier")
    if amount_multiplier <= 0:
        raise BusinessQueryValidationError("amount_multiplier must be positive")

    tax_fields = _normalize_tax_fields(query.tax_fields)
    tax_year_filter, year, year_from, year_to = _normalize_tax_year_selection(
        query.tax_year_filter,
        query.year,
        query.year_from,
        query.year_to,
    )

    return BusinessQueryInput(
        query_name=query_name,
        isins=isins,
        legal_entity_type=query.legal_entity_type,
        amount_multiplier=amount_multiplier,
        subcategory_key=subcategory_key,
        tax_year_filter=tax_year_filter,
        tax_fields=tax_fields,
        year=year,
        year_from=year_from,
        year_to=year_to,
        positions=positions,
    )


async def execute_business_query(
    session: AsyncSession,
    query: BusinessQueryInput,
) -> BusinessQueryResult:
    validated = validate_business_query_input(query)
    if validated.tax_year_filter == MOST_RECENT_COMMON_AVAILABLE_YEAR:
        resolved_year = await _resolve_most_recent_common_tax_year(session, validated)
        if resolved_year is None:
            return BusinessQueryResult(query=validated, no_common_year_isins=validated.isins)
        validated = replace(validated, tax_year_filter=str(resolved_year), year=resolved_year)

    suffixes = _suffixes_for_validated_query(validated)
    selected_amount_columns = _selected_amount_column_names(validated.tax_fields, suffixes)

    statement = _build_business_query_statement(validated, selected_amount_columns)
    result = await session.execute(statement)
    source_rows = result.mappings().all()

    rows: list[BusinessQueryResultRow] = []
    for source_row in source_rows:
        for field_code in validated.tax_fields:
            for suffix in suffixes:
                column_name = f"{field_code}{suffix}"
                rows.append(_result_row_from_mapping(validated, source_row, field_code, suffix, column_name))

    return BusinessQueryResult(
        query=validated,
        rows=_sort_result_rows(rows, tax_fields=validated.tax_fields, suffixes=suffixes),
        missing_year_isins=_missing_year_isins(validated, source_rows),
    )


def _build_business_query_statement(
    query: BusinessQueryInput,
    amount_column_bases: tuple[str, ...],
) -> sa.Select[tuple[Any, ...]]:
    selected_columns = [
        V2_TAXDATHOMCCY.c.TAXISN,
        V2_TAXDATHOMCCY.c.TAXOKBIDN,
        V2_TAXDATHOMCCY.c.TAXYEA,
        V2_TAXDATHOMCCY.c.FNDCCY,
        V2_TAXDATHOMCCY.c.TAXMDT,
        V2_TAXDATHOMCCY.c.FXRAT,
        *(
            V2_TAXDATHOMCCY.c[column_name]
            for column_base in amount_column_bases
            for column_name in _amount_currency_column_names(column_base)
        ),
    ]

    statement = (
        sa.select(*selected_columns)
        .select_from(V2_TAXDATHOMCCY)
        .where(V2_TAXDATHOMCCY.c.TAXISN.in_(query.isins))
        .order_by(V2_TAXDATHOMCCY.c.TAXISN, V2_TAXDATHOMCCY.c.TAXYEA, V2_TAXDATHOMCCY.c.TAXOKBIDN)
    )

    if query.year is not None:
        statement = statement.where(V2_TAXDATHOMCCY.c.TAXYEA == query.year)
    elif query.year_from is not None and query.year_to is not None:
        statement = statement.where(V2_TAXDATHOMCCY.c.TAXYEA.between(query.year_from, query.year_to))

    return statement


async def _resolve_most_recent_common_tax_year(
    session: AsyncSession,
    query: BusinessQueryInput,
) -> int | None:
    statement = (
        sa.select(V2_TAXDATHOMCCY.c.TAXISN, V2_TAXDATHOMCCY.c.TAXYEA)
        .select_from(V2_TAXDATHOMCCY)
        .where(V2_TAXDATHOMCCY.c.TAXISN.in_(query.isins))
        .where(V2_TAXDATHOMCCY.c.TAXYEA.is_not(None))
        .distinct()
    )
    result = await session.execute(statement)
    availability_rows = result.mappings().all()

    years_by_isin: dict[str, set[int]] = {isin: set() for isin in query.isins}
    for row in availability_rows:
        isin = str(row["TAXISN"]).upper()
        if isin not in years_by_isin:
            continue
        year = row["TAXYEA"]
        if year is not None:
            years_by_isin[isin].add(int(year))

    if any(not years for years in years_by_isin.values()):
        return None

    common_years = set.intersection(*years_by_isin.values())
    if not common_years:
        return None
    return max(common_years)


def _normalize_isins(raw_isins: tuple[str, ...]) -> tuple[str, ...]:
    normalized_isins: list[str] = []
    seen: set[str] = set()
    for raw_isin in raw_isins:
        isin = _normalize_single_isin(raw_isin)
        if isin not in seen:
            normalized_isins.append(isin)
            seen.add(isin)
    return tuple(normalized_isins)


def _normalize_positions(raw_positions: tuple[BusinessQueryPosition, ...]) -> tuple[BusinessQueryPosition, ...]:
    normalized_positions: list[BusinessQueryPosition] = []
    seen: set[str] = set()
    for raw_position in raw_positions:
        isin = _normalize_single_isin(raw_position.isin)
        if isin in seen:
            raise BusinessQueryValidationError("duplicate position ISIN")

        amount = _normalize_decimal(raw_position.amount, "position amount")
        if amount <= 0:
            raise BusinessQueryValidationError("position amount must be positive")

        normalized_positions.append(BusinessQueryPosition(isin=isin, amount=amount))
        seen.add(isin)
    return tuple(normalized_positions)


def _normalize_single_isin(raw_isin: str) -> str:
    isin = raw_isin.strip().upper()
    if not ISIN_PATTERN.fullmatch(isin):
        raise BusinessQueryValidationError("invalid ISIN")
    return isin


def _normalize_decimal(value: Decimal, field_name: str) -> Decimal:
    try:
        normalized = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise BusinessQueryValidationError(f"{field_name} must be numeric") from exc
    if not normalized.is_finite():
        raise BusinessQueryValidationError(f"{field_name} must be numeric")
    return normalized


def _normalize_tax_fields(raw_tax_fields: tuple[str, ...]) -> tuple[str, ...]:
    if not raw_tax_fields:
        raise BusinessQueryValidationError("at least one tax field is required")

    normalized_fields: list[str] = []
    seen: set[str] = set()
    for raw_field in raw_tax_fields:
        field_code = raw_field.strip().upper()
        if field_code not in TAX_FIELD_LABELS:
            raise BusinessQueryValidationError("unsupported tax field")
        if field_code not in seen:
            normalized_fields.append(field_code)
            seen.add(field_code)
    return tuple(normalized_fields)


def _normalize_subcategory_key(legal_entity_type: str, raw_subcategory_key: str | None) -> str:
    if raw_subcategory_key is None:
        return DEFAULT_SUBCATEGORY_KEYS[legal_entity_type]

    subcategory_key = raw_subcategory_key.strip()
    allowed_subcategories = LEGAL_ENTITY_SUBCATEGORY_SUFFIXES[legal_entity_type]
    if subcategory_key not in allowed_subcategories:
        raise BusinessQueryValidationError("subcategory is not supported for legal entity type")
    return subcategory_key


def _normalize_tax_year_selection(
    tax_year_filter: str | int | None,
    year: int | None,
    year_from: int | None,
    year_to: int | None,
) -> tuple[str | None, int | None, int | None, int | None]:
    has_legacy_year_filter = year is not None or year_from is not None or year_to is not None
    if tax_year_filter is not None and has_legacy_year_filter:
        raise BusinessQueryValidationError("use either tax_year_filter or legacy year filters")
    if tax_year_filter is not None:
        normalized_filter, normalized_year = _normalize_tax_year_filter(tax_year_filter)
        return normalized_filter, normalized_year, None, None

    normalized_year, normalized_year_from, normalized_year_to = _normalize_year_filters(year, year_from, year_to)
    if normalized_year is not None:
        return str(normalized_year), normalized_year, None, None
    if normalized_year_from is None and normalized_year_to is None:
        return ALL_AVAILABLE_YEARS, None, None, None
    return None, None, normalized_year_from, normalized_year_to


def _normalize_tax_year_filter(tax_year_filter: str | int) -> tuple[str, int | None]:
    if isinstance(tax_year_filter, int):
        year = _validate_year(tax_year_filter)
        return str(year), year

    if not isinstance(tax_year_filter, str):
        raise BusinessQueryValidationError(
            f"tax_year_filter must be {ALL_AVAILABLE_YEARS}, {MOST_RECENT_COMMON_AVAILABLE_YEAR}, "
            "or a year between 1900 and 3000"
        )

    normalized_filter = tax_year_filter.strip()
    if normalized_filter in {ALL_AVAILABLE_YEARS, MOST_RECENT_COMMON_AVAILABLE_YEAR}:
        return normalized_filter, None
    if normalized_filter.isdecimal():
        year = _validate_year(int(normalized_filter))
        return str(year), year
    raise BusinessQueryValidationError(
        f"tax_year_filter must be {ALL_AVAILABLE_YEARS}, {MOST_RECENT_COMMON_AVAILABLE_YEAR}, "
        "or a year between 1900 and 3000"
    )


def _normalize_year_filters(
    year: int | None,
    year_from: int | None,
    year_to: int | None,
) -> tuple[int | None, int | None, int | None]:
    has_year = year is not None
    has_range = year_from is not None or year_to is not None
    if has_year and has_range:
        raise BusinessQueryValidationError("use either year or year range")
    if year is not None:
        return _validate_year(year), None, None
    if year_from is None and year_to is None:
        return None, None, None
    if year_from is None or year_to is None:
        raise BusinessQueryValidationError("year range requires start and end years")

    normalized_from = _validate_year(year_from)
    normalized_to = _validate_year(year_to)
    if normalized_from > normalized_to:
        raise BusinessQueryValidationError("year range start must be before end")
    return None, normalized_from, normalized_to


def _validate_year(year: int) -> int:
    if not isinstance(year, int):
        raise BusinessQueryValidationError("year must be numeric")
    if year < 1900 or year > 3000:
        raise BusinessQueryValidationError("year must be between 1900 and 3000")
    return year


def _suffixes_for_validated_query(query: BusinessQueryInput) -> tuple[str, ...]:
    if query.subcategory_key is None:
        raise BusinessQueryValidationError("subcategory is required")
    return LEGAL_ENTITY_SUBCATEGORY_SUFFIXES[query.legal_entity_type][query.subcategory_key]


def _selected_amount_column_names(
    tax_fields: tuple[str, ...],
    suffixes: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(f"{field_code}{suffix}" for field_code in tax_fields for suffix in suffixes)


def _amount_currency_column_names(column_base: str) -> tuple[str, str]:
    return f"{column_base}_HOMCCY", f"{column_base}_EUR"


def _missing_year_isins(
    query: BusinessQueryInput,
    source_rows: list[sa.RowMapping],
) -> tuple[str, ...]:
    if query.year is None:
        return ()

    isins_with_selected_year = {
        str(source_row["TAXISN"]).upper() for source_row in source_rows if source_row["TAXYEA"] == query.year
    }
    return tuple(isin for isin in query.isins if isin not in isins_with_selected_year)


def _sort_result_rows(
    rows: list[BusinessQueryResultRow],
    *,
    tax_fields: tuple[str, ...],
    suffixes: tuple[str, ...],
) -> tuple[BusinessQueryResultRow, ...]:
    tax_field_order = {field_code: index for index, field_code in enumerate(tax_fields)}
    suffix_order = {suffix: index for index, suffix in enumerate(suffixes)}

    return tuple(
        sorted(
            rows,
            key=lambda row: (
                tax_field_order[row.tax_field_code],
                row.isin,
                row.tax_year if row.tax_year is not None else -1,
                row.oekb_report_id,
                suffix_order[row.legal_entity_category],
            ),
        )
    )


def _result_row_from_mapping(
    query: BusinessQueryInput,
    source_row: sa.RowMapping,
    field_code: str,
    suffix: str,
    column_name: str,
) -> BusinessQueryResultRow:
    home_column_name, eur_column_name = _amount_currency_column_names(column_name)
    base_home_currency_value = _decimal_or_none(source_row[home_column_name])
    base_eur_value = _decimal_or_none(source_row[eur_column_name])
    amount_multiplier = _amount_multiplier_for_isin(query, str(source_row["TAXISN"]))
    calculated_home_currency_value = (
        None if base_home_currency_value is None else base_home_currency_value * amount_multiplier
    )
    calculated_eur_value = None if base_eur_value is None else base_eur_value * amount_multiplier
    currency_code = source_row["FNDCCY"]
    fx_date = source_row["TAXMDT"]
    return BusinessQueryResultRow(
        query_name=query.query_name,
        isin=source_row["TAXISN"],
        tax_year=source_row["TAXYEA"],
        oekb_report_id=source_row["TAXOKBIDN"],
        fund_currency=currency_code,
        report_date=fx_date,
        fx_rate=_decimal_or_none(source_row["FXRAT"]),
        legal_entity_category=suffix,
        tax_field_code=field_code,
        tax_field_label=TAX_FIELD_LABELS[field_code],
        base_eur_value=base_eur_value,
        amount_multiplier=amount_multiplier,
        calculated_eur_value=calculated_eur_value,
        original_currency_code=currency_code,
        home_currency_code=currency_code,
        fx_date=fx_date,
        base_home_currency_value=base_home_currency_value,
        calculated_home_currency_value=calculated_home_currency_value,
    )


def _amount_multiplier_for_isin(query: BusinessQueryInput, isin: str) -> Decimal:
    if not query.positions:
        return query.amount_multiplier

    normalized_isin = isin.upper()
    for position in query.positions:
        if position.isin == normalized_isin:
            return position.amount
    raise BusinessQueryValidationError("result row ISIN is not present in positions")


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
