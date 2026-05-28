from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

ISIN_PATTERN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")

LEGAL_ENTITY_SUFFIXES = MappingProxyType(
    {
        "natural person": ("PVM", "PVO"),
        "business": ("BVM", "BVO", "BVJ"),
        "Stiftung": ("STI",),
    }
)

TAX_FIELD_LABELS = MappingProxyType(
    {
        "K40": "Taxable income",
        "K61": "Cost-basis adjustment",
        "K62": "Distribution cost-basis adjustment",
    }
)

DEFAULT_TAX_FIELDS = tuple(TAX_FIELD_LABELS)

IDENTITY_COLUMNS = ("TAXISN", "TAXOKBIDN", "TAXYEA", "FNDCCY", "TAXMDT", "FXRAT")
AMOUNT_COLUMNS = tuple(
    f"{field_code}{suffix}"
    for field_code in TAX_FIELD_LABELS
    for suffixes in LEGAL_ENTITY_SUFFIXES.values()
    for suffix in suffixes
)

V2_TAXDATEUR = sa.Table(
    "V2_TAXDATEUR",
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
class BusinessQueryInput:
    query_name: str
    isins: tuple[str, ...]
    legal_entity_type: str
    amount_multiplier: Decimal
    tax_fields: tuple[str, ...] = DEFAULT_TAX_FIELDS
    year: int | None = None
    year_from: int | None = None
    year_to: int | None = None


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


@dataclass(frozen=True)
class BusinessQueryResult:
    query: BusinessQueryInput
    rows: tuple[BusinessQueryResultRow, ...] = field(default_factory=tuple)

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

    isins = _normalize_isins(query.isins)
    if not isins:
        raise BusinessQueryValidationError("at least one ISIN is required")

    if query.legal_entity_type not in LEGAL_ENTITY_SUFFIXES:
        raise BusinessQueryValidationError("unsupported legal entity type")

    amount_multiplier = _normalize_decimal(query.amount_multiplier, "amount_multiplier")
    if amount_multiplier <= 0:
        raise BusinessQueryValidationError("amount_multiplier must be positive")

    tax_fields = _normalize_tax_fields(query.tax_fields)
    year, year_from, year_to = _normalize_year_filters(query.year, query.year_from, query.year_to)

    return BusinessQueryInput(
        query_name=query_name,
        isins=isins,
        legal_entity_type=query.legal_entity_type,
        amount_multiplier=amount_multiplier,
        tax_fields=tax_fields,
        year=year,
        year_from=year_from,
        year_to=year_to,
    )


async def execute_business_query(
    session: AsyncSession,
    query: BusinessQueryInput,
) -> BusinessQueryResult:
    validated = validate_business_query_input(query)
    suffixes = LEGAL_ENTITY_SUFFIXES[validated.legal_entity_type]
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

    return BusinessQueryResult(query=validated, rows=tuple(rows))


def _build_business_query_statement(
    query: BusinessQueryInput,
    amount_column_names: tuple[str, ...],
) -> sa.Select[tuple[Any, ...]]:
    selected_columns = [
        V2_TAXDATEUR.c.TAXISN,
        V2_TAXDATEUR.c.TAXOKBIDN,
        V2_TAXDATEUR.c.TAXYEA,
        V2_TAXDATEUR.c.FNDCCY,
        V2_TAXDATEUR.c.TAXMDT,
        V2_TAXDATEUR.c.FXRAT,
        *(V2_TAXDATEUR.c[column_name] for column_name in amount_column_names),
    ]

    statement = (
        sa.select(*selected_columns)
        .select_from(V2_TAXDATEUR)
        .where(V2_TAXDATEUR.c.TAXISN.in_(query.isins))
        .order_by(V2_TAXDATEUR.c.TAXISN, V2_TAXDATEUR.c.TAXYEA, V2_TAXDATEUR.c.TAXOKBIDN)
    )

    if query.year is not None:
        statement = statement.where(V2_TAXDATEUR.c.TAXYEA == query.year)
    elif query.year_from is not None and query.year_to is not None:
        statement = statement.where(V2_TAXDATEUR.c.TAXYEA.between(query.year_from, query.year_to))

    return statement


def _normalize_isins(raw_isins: tuple[str, ...]) -> tuple[str, ...]:
    normalized_isins: list[str] = []
    seen: set[str] = set()
    for raw_isin in raw_isins:
        isin = raw_isin.strip().upper()
        if not ISIN_PATTERN.fullmatch(isin):
            raise BusinessQueryValidationError("invalid ISIN")
        if isin not in seen:
            normalized_isins.append(isin)
            seen.add(isin)
    return tuple(normalized_isins)


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
    if year < 1900 or year > 3000:
        raise BusinessQueryValidationError("year must be between 1900 and 3000")
    return year


def _selected_amount_column_names(
    tax_fields: tuple[str, ...],
    suffixes: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(f"{field_code}{suffix}" for field_code in tax_fields for suffix in suffixes)


def _result_row_from_mapping(
    query: BusinessQueryInput,
    source_row: sa.RowMapping,
    field_code: str,
    suffix: str,
    column_name: str,
) -> BusinessQueryResultRow:
    base_value = _decimal_or_none(source_row[column_name])
    calculated_value = None if base_value is None else base_value * query.amount_multiplier
    return BusinessQueryResultRow(
        query_name=query.query_name,
        isin=source_row["TAXISN"],
        tax_year=source_row["TAXYEA"],
        oekb_report_id=source_row["TAXOKBIDN"],
        fund_currency=source_row["FNDCCY"],
        report_date=source_row["TAXMDT"],
        fx_rate=_decimal_or_none(source_row["FXRAT"]),
        legal_entity_category=suffix,
        tax_field_code=field_code,
        tax_field_label=TAX_FIELD_LABELS[field_code],
        base_eur_value=base_value,
        amount_multiplier=query.amount_multiplier,
        calculated_eur_value=calculated_value,
    )


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))
