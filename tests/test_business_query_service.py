from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Select

from fondant.business_query import (
    BusinessQueryInput,
    BusinessQueryValidationError,
    execute_business_query,
)
from fondant.db.models.business_query import ALL_AVAILABLE_YEARS


class _FakeMappingResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeExecuteResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def mappings(self) -> _FakeMappingResult:
        return _FakeMappingResult(self._rows)


class _FakeSession:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.statement: Select[tuple[Any, ...]] | None = None

    async def execute(self, statement: Select[tuple[Any, ...]]) -> _FakeExecuteResult:
        self.statement = statement
        return _FakeExecuteResult(self.rows)


def _view_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "TAXISN": "IE00BMTX1Y45",
        "TAXOKBIDN": 1001,
        "TAXYEA": 2025,
        "FNDCCY": "EUR",
        "TAXMDT": date(2025, 6, 15),
        "FXRAT": Decimal("1.0000000000"),
        "K40PVM": Decimal("10.0000000000"),
        "K40PVO": Decimal("20.0000000000"),
        "K40BVM": Decimal("30.0000000000"),
        "K40BVO": Decimal("40.0000000000"),
        "K40BVJ": Decimal("50.0000000000"),
        "K40STI": Decimal("60.0000000000"),
        "K61PVM": Decimal("1.0000000000"),
        "K61PVO": Decimal("2.0000000000"),
        "K61BVM": Decimal("3.0000000000"),
        "K61BVO": Decimal("4.0000000000"),
        "K61BVJ": Decimal("5.0000000000"),
        "K61STI": Decimal("6.0000000000"),
        "K62PVM": Decimal("7.0000000000"),
        "K62PVO": Decimal("8.0000000000"),
        "K62BVM": Decimal("9.0000000000"),
        "K62BVO": Decimal("10.0000000000"),
        "K62BVJ": Decimal("11.0000000000"),
        "K62STI": Decimal("12.0000000000"),
    }
    row.update(overrides)
    return row


def _compiled_sql(statement: Select[tuple[Any, ...]]) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


@pytest.mark.asyncio
async def test_business_query_executes_only_v2_taxdateur_with_parameterized_filters() -> None:
    session = _FakeSession([_view_row()])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="  Monthly review  ",
            isins=("ie00bmtx1y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("2"),
            tax_fields=("K40",),
            year=2025,
        ),
    )

    assert session.statement is not None
    compiled_sql = _compiled_sql(session.statement)
    assert 'FROM "V2_TAXDATEUR"' in compiled_sql
    assert '"TAXISN" IN (\'IE00BMTX1Y45\')' in compiled_sql
    assert '"TAXYEA" = 2025' in compiled_sql
    assert '"TAXRPT"' not in compiled_sql
    assert '"TAXDAT"' not in compiled_sql
    assert result.query.query_name == "Monthly review"
    assert result.query.isins == ("IE00BMTX1Y45",)


@pytest.mark.asyncio
async def test_business_query_maps_natural_person_suffixes_and_multiplies_amount() -> None:
    session = _FakeSession([_view_row()])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Natural person case",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1.5"),
            tax_fields=("K40",),
        ),
    )

    assert [(row.tax_field_code, row.legal_entity_category) for row in result.rows] == [
        ("K40", "PVM"),
        ("K40", "PVO"),
    ]
    assert result.rows[0].base_eur_value == Decimal("10.0000000000")
    assert result.rows[0].calculated_eur_value == Decimal("15.00000000000")
    assert result.rows[1].base_eur_value == Decimal("20.0000000000")
    assert result.rows[1].calculated_eur_value == Decimal("30.00000000000")


@pytest.mark.asyncio
async def test_business_query_maps_business_suffixes_for_multiple_isins() -> None:
    rows = [
        _view_row(TAXISN="IE00BMTX1Y45", TAXOKBIDN=1001),
        _view_row(TAXISN="LU1681044993", TAXOKBIDN=1002, K61BVM=Decimal("1.2500000000")),
    ]
    session = _FakeSession(rows)

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Business case",
            isins=("IE00BMTX1Y45", "lu1681044993", "IE00BMTX1Y45"),
            legal_entity_type="business",
            amount_multiplier=Decimal("4"),
            tax_fields=("K61",),
        ),
    )

    assert result.query.isins == ("IE00BMTX1Y45", "LU1681044993")
    assert result.count == 6
    assert {row.legal_entity_category for row in result.rows} == {"BVM", "BVO", "BVJ"}
    assert result.rows[3].isin == "LU1681044993"
    assert result.rows[3].base_eur_value == Decimal("1.2500000000")
    assert result.rows[3].calculated_eur_value == Decimal("5.0000000000")


@pytest.mark.asyncio
async def test_business_query_maps_stiftung_suffix_and_preserves_null_amounts() -> None:
    session = _FakeSession([_view_row(K62STI=None, FXRAT=Decimal("0E-10"))])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Foundation case",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="Stiftung",
            amount_multiplier=Decimal("10"),
            tax_fields=("K62",),
        ),
    )

    assert result.count == 1
    assert result.rows[0].legal_entity_category == "STI"
    assert result.rows[0].fx_rate == Decimal("0E-10")
    assert result.rows[0].base_eur_value is None
    assert result.rows[0].calculated_eur_value is None


@pytest.mark.parametrize(
    ("legal_entity_type", "subcategory_key", "expected_categories"),
    [
        ("natural person", "natural_person_pa_with_option", ("PVM",)),
        ("natural person", "natural_person_pa_without_option", ("PVO",)),
        ("natural person", "natural_person_all", ("PVM", "PVO")),
        ("business", "business_bv_with_option", ("BVM",)),
        ("business", "business_bv_without_option", ("BVO",)),
        ("business", "business_bv_legal_person", ("BVJ",)),
        ("business", "business_all", ("BVM", "BVO", "BVJ")),
        ("Stiftung", "stiftung", ("STI",)),
    ],
)
@pytest.mark.asyncio
async def test_business_query_maps_subcategory_suffixes(
    legal_entity_type: str,
    subcategory_key: str,
    expected_categories: tuple[str, ...],
) -> None:
    session = _FakeSession([_view_row()])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Subcategory case",
            isins=("IE00BMTX1Y45",),
            legal_entity_type=legal_entity_type,
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            subcategory_key=subcategory_key,
        ),
    )

    assert tuple(row.legal_entity_category for row in result.rows) == expected_categories
    assert result.query.subcategory_key == subcategory_key


@pytest.mark.parametrize(
    ("legal_entity_type", "subcategory_key"),
    [
        ("natural person", "business_bv_with_option"),
        ("business", "natural_person_pa_with_option"),
        ("Stiftung", "natural_person_all"),
        ("Stiftung", "business_all"),
    ],
)
@pytest.mark.asyncio
async def test_business_query_rejects_incompatible_subcategory_combinations(
    legal_entity_type: str,
    subcategory_key: str,
) -> None:
    session = _FakeSession([])

    with pytest.raises(BusinessQueryValidationError, match="subcategory"):
        await execute_business_query(
            session,
            BusinessQueryInput(
                query_name="Bad subcategory",
                isins=("IE00BMTX1Y45",),
                legal_entity_type=legal_entity_type,
                amount_multiplier=Decimal("1"),
                subcategory_key=subcategory_key,
            ),
        )

    assert session.statement is None


@pytest.mark.asyncio
async def test_business_query_all_available_years_preserves_unfiltered_year_behavior() -> None:
    session = _FakeSession(
        [
            _view_row(TAXYEA=2024, TAXOKBIDN=1000),
            _view_row(TAXYEA=2025, TAXOKBIDN=1001),
        ]
    )

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="All years",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            tax_year_filter=ALL_AVAILABLE_YEARS,
        ),
    )

    assert session.statement is not None
    compiled_sql = _compiled_sql(session.statement)
    assert '"TAXYEA" =' not in compiled_sql
    assert '"TAXYEA" BETWEEN' not in compiled_sql
    assert result.query.tax_year_filter == ALL_AVAILABLE_YEARS
    assert {row.tax_year for row in result.rows} == {2024, 2025}


@pytest.mark.asyncio
async def test_business_query_specific_tax_year_filter_adds_year_predicate() -> None:
    session = _FakeSession([_view_row(TAXYEA=2024)])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Specific year",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            tax_year_filter="2024",
        ),
    )

    assert session.statement is not None
    compiled_sql = _compiled_sql(session.statement)
    assert '"TAXYEA" = 2024' in compiled_sql
    assert result.query.tax_year_filter == "2024"
    assert result.query.year == 2024


@pytest.mark.parametrize("tax_year_filter", ["all", "202X", "1899", "3001"])
@pytest.mark.asyncio
async def test_business_query_rejects_invalid_tax_year_filter(tax_year_filter: str) -> None:
    session = _FakeSession([])

    with pytest.raises(BusinessQueryValidationError, match="tax_year_filter|year must be between"):
        await execute_business_query(
            session,
            BusinessQueryInput(
                query_name="Bad year",
                isins=("IE00BMTX1Y45",),
                legal_entity_type="business",
                amount_multiplier=Decimal("1"),
                tax_year_filter=tax_year_filter,
            ),
        )

    assert session.statement is None


@pytest.mark.asyncio
async def test_business_query_multiplies_amount_with_subcategory_and_tax_year_filters() -> None:
    session = _FakeSession([_view_row(TAXYEA=2025)])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Filtered amount",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="business",
            amount_multiplier=Decimal("2.5"),
            tax_fields=("K40",),
            subcategory_key="business_bv_without_option",
            tax_year_filter=2025,
        ),
    )

    assert result.count == 1
    assert result.rows[0].legal_entity_category == "BVO"
    assert result.rows[0].base_eur_value == Decimal("40.0000000000")
    assert result.rows[0].calculated_eur_value == Decimal("100.00000000000")


@pytest.mark.asyncio
async def test_business_query_empty_result_set_is_structured() -> None:
    session = _FakeSession([])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="No rows",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="business",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
        ),
    )

    assert result.count == 0
    assert result.is_empty is True
    assert result.rows == ()


@pytest.mark.parametrize(
    "query",
    [
        BusinessQueryInput("", ("IE00BMTX1Y45",), "business", Decimal("1")),
        BusinessQueryInput("Bad ISIN", ("not-an-isin",), "business", Decimal("1")),
        BusinessQueryInput("Bad entity", ("IE00BMTX1Y45",), "raw sql", Decimal("1")),
        BusinessQueryInput("Bad amount", ("IE00BMTX1Y45",), "business", Decimal("0")),
        BusinessQueryInput("Bad field", ("IE00BMTX1Y45",), "business", Decimal("1"), ("SQL",)),
        BusinessQueryInput("Bad year", ("IE00BMTX1Y45",), "business", Decimal("1"), year=1800),
        BusinessQueryInput(
            "Bad range",
            ("IE00BMTX1Y45",),
            "business",
            Decimal("1"),
            year_from=2026,
            year_to=2025,
        ),
    ],
)
@pytest.mark.asyncio
async def test_business_query_rejects_invalid_structured_inputs(
    query: BusinessQueryInput,
) -> None:
    session = _FakeSession([])

    with pytest.raises(BusinessQueryValidationError):
        await execute_business_query(session, query)

    assert session.statement is None


def test_business_query_input_does_not_accept_raw_sql_argument() -> None:
    with pytest.raises(TypeError):
        BusinessQueryInput(  # type: ignore[call-arg]
            query_name="Raw SQL attempt",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="business",
            amount_multiplier=Decimal("1"),
            raw_sql='SELECT * FROM "TAXRPT"',
        )
