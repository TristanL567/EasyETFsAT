from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Select

from fondant.business_query import (
    MOST_RECENT_COMMON_AVAILABLE_YEAR,
    BusinessQueryInput,
    BusinessQueryPosition,
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
    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        execute_rows: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        self.rows = rows
        self.statement: Select[tuple[Any, ...]] | None = None
        self.statements: list[Select[tuple[Any, ...]]] = []
        self._execute_rows = list(execute_rows) if execute_rows is not None else None

    async def execute(self, statement: Select[tuple[Any, ...]]) -> _FakeExecuteResult:
        self.statement = statement
        self.statements.append(statement)
        rows = self._execute_rows.pop(0) if self._execute_rows is not None else self.rows
        return _FakeExecuteResult(rows)


def _view_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "TAXISN": "IE00BMTX1Y45",
        "TAXOKBIDN": 1001,
        "TAXYEA": 2025,
        "FNDCCY": "EUR",
        "TAXMDT": date(2025, 6, 15),
        "FXRAT": Decimal("1.0000000000"),
    }
    base_amounts: dict[str, Decimal | None] = {
        "K40PVM": Decimal("10.0000000000"),
        "K40PVO": Decimal("20.0000000000"),
        "K40BVM": Decimal("30.0000000000"),
        "K40BVO": Decimal("40.0000000000"),
        "K40BVJ": Decimal("50.0000000000"),
        "K40STI": Decimal("60.0000000000"),
        "K11PVM": Decimal("100.0000000000"),
        "K11PVO": Decimal("200.0000000000"),
        "K11BVM": Decimal("300.0000000000"),
        "K11BVO": Decimal("400.0000000000"),
        "K11BVJ": Decimal("500.0000000000"),
        "K11STI": Decimal("600.0000000000"),
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
    for column_name, value in base_amounts.items():
        row[f"{column_name}_HOMCCY"] = value
        row[f"{column_name}_EUR"] = value
    for key, value in overrides.items():
        if key in base_amounts:
            row[f"{key}_HOMCCY"] = value
            row[f"{key}_EUR"] = value
        else:
            row[key] = value
    return row


def _compiled_sql(statement: Select[tuple[Any, ...]]) -> str:
    return str(
        statement.compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )


@pytest.mark.asyncio
async def test_business_query_executes_only_v2_taxdathomccy_with_parameterized_filters() -> None:
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
    assert 'FROM "V2_TAXDATHOMCCY"' in compiled_sql
    assert '"K40PVM_HOMCCY"' in compiled_sql
    assert '"K40PVM_EUR"' in compiled_sql
    assert '"K40PVO_HOMCCY"' in compiled_sql
    assert '"K40PVO_EUR"' in compiled_sql
    assert '"TAXISN" IN (\'IE00BMTX1Y45\')' in compiled_sql
    assert '"TAXYEA" = 2025' in compiled_sql
    assert '"V2_TAXDATEUR"' not in compiled_sql
    assert '"TAXRPT"' not in compiled_sql
    assert '"TAXDAT"' not in compiled_sql
    assert result.query.query_name == "Monthly review"
    assert result.query.isins == ("IE00BMTX1Y45",)


@pytest.mark.asyncio
async def test_business_query_default_tax_fields_remain_legacy_fields() -> None:
    session = _FakeSession([_view_row()])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Default fields",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
        ),
    )

    assert session.statement is not None
    compiled_sql = _compiled_sql(session.statement)
    assert result.query.tax_fields == ("K40", "K61", "K62")
    assert [(row.tax_field_code, row.legal_entity_category) for row in result.rows] == [
        ("K40", "PVM"),
        ("K40", "PVO"),
        ("K61", "PVM"),
        ("K61", "PVO"),
        ("K62", "PVM"),
        ("K62", "PVO"),
    ]
    assert '"K40PVM_HOMCCY"' in compiled_sql
    assert '"K61PVM_HOMCCY"' in compiled_sql
    assert '"K62PVM_HOMCCY"' in compiled_sql
    assert '"K11PVM_HOMCCY"' not in compiled_sql


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
    assert result.rows[0].home_currency_code == "EUR"
    assert result.rows[0].original_currency_code == "EUR"
    assert result.rows[0].fx_date == date(2025, 6, 15)
    assert result.rows[0].base_home_currency_value == Decimal("10.0000000000")
    assert result.rows[0].calculated_home_currency_value == Decimal("15.00000000000")
    assert result.rows[1].base_eur_value == Decimal("20.0000000000")
    assert result.rows[1].calculated_eur_value == Decimal("30.00000000000")


@pytest.mark.asyncio
async def test_business_query_selects_multiple_active_registry_tax_fields() -> None:
    session = _FakeSession([_view_row()])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Selected fields",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("2"),
            tax_fields=("k11", "K61", "K11"),
            subcategory_key="natural_person_pa_with_option",
        ),
    )

    assert session.statement is not None
    compiled_sql = _compiled_sql(session.statement)
    assert result.query.tax_fields == ("K11", "K61")
    assert [(row.tax_field_code, row.legal_entity_category) for row in result.rows] == [
        ("K11", "PVM"),
        ("K61", "PVM"),
    ]
    assert result.rows[0].base_home_currency_value == Decimal("100.0000000000")
    assert result.rows[0].calculated_home_currency_value == Decimal("200.0000000000")
    assert result.rows[1].base_eur_value == Decimal("1.0000000000")
    assert result.rows[1].calculated_eur_value == Decimal("2.0000000000")
    assert '"K11PVM_HOMCCY"' in compiled_sql
    assert '"K11PVM_EUR"' in compiled_sql
    assert '"K61PVM_HOMCCY"' in compiled_sql
    assert '"K61PVM_EUR"' in compiled_sql
    assert '"K40PVM_HOMCCY"' not in compiled_sql
    assert '"K62PVM_HOMCCY"' not in compiled_sql


@pytest.mark.asyncio
async def test_business_query_orders_results_by_tax_field_before_isin() -> None:
    rows = [
        _view_row(TAXISN="LU1681044993", TAXOKBIDN=1002, K11PVM=Decimal("21.0000000000")),
        _view_row(TAXISN="IE00BMTX1Y45", TAXOKBIDN=1001, K11PVM=Decimal("11.0000000000")),
    ]
    session = _FakeSession(rows)

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Field-first ordering",
            isins=("LU1681044993", "IE00BMTX1Y45"),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K11", "K61"),
            subcategory_key="natural_person_pa_with_option",
        ),
    )

    assert [(row.tax_field_code, row.isin, row.tax_year, row.oekb_report_id) for row in result.rows] == [
        ("K11", "IE00BMTX1Y45", 2025, 1001),
        ("K11", "LU1681044993", 2025, 1002),
        ("K61", "IE00BMTX1Y45", 2025, 1001),
        ("K61", "LU1681044993", 2025, 1002),
    ]


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
async def test_business_query_applies_position_amounts_by_matching_isin() -> None:
    rows = [
        _view_row(TAXISN="IE00BMTX1Y45", TAXOKBIDN=1001, K40PVM=Decimal("10.0000000000")),
        _view_row(TAXISN="LU1681044993", TAXOKBIDN=1002, K40PVM=Decimal("20.0000000000")),
    ]
    session = _FakeSession(rows)

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Position amounts",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("99"),
            tax_fields=("K40",),
            subcategory_key="natural_person_pa_with_option",
            positions=(
                BusinessQueryPosition(isin="ie00bmtx1y45", amount=Decimal("2")),
                BusinessQueryPosition(isin="lu1681044993", amount=Decimal("3")),
            ),
        ),
    )

    assert result.query.isins == ("IE00BMTX1Y45", "LU1681044993")
    assert result.query.positions == (
        BusinessQueryPosition(isin="IE00BMTX1Y45", amount=Decimal("2")),
        BusinessQueryPosition(isin="LU1681044993", amount=Decimal("3")),
    )
    assert [(row.isin, row.amount_multiplier, row.calculated_eur_value) for row in result.rows] == [
        ("IE00BMTX1Y45", Decimal("2"), Decimal("20.0000000000")),
        ("LU1681044993", Decimal("3"), Decimal("60.0000000000")),
    ]


@pytest.mark.asyncio
async def test_business_query_positions_can_provide_isin_filter_without_legacy_isins() -> None:
    session = _FakeSession([_view_row(TAXISN="LU1681044993", K61BVM=Decimal("1.2500000000"))])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Position-only ISINs",
            isins=(),
            legal_entity_type="business",
            amount_multiplier=Decimal("1"),
            tax_fields=("K61",),
            subcategory_key="business_bv_with_option",
            positions=(BusinessQueryPosition(isin="lu1681044993", amount=Decimal("4")),),
        ),
    )

    assert session.statement is not None
    compiled_sql = _compiled_sql(session.statement)
    assert '"TAXISN" IN (\'LU1681044993\')' in compiled_sql
    assert result.query.isins == ("LU1681044993",)
    assert result.rows[0].amount_multiplier == Decimal("4")
    assert result.rows[0].calculated_eur_value == Decimal("5.0000000000")


@pytest.mark.asyncio
async def test_business_query_rejects_conflicting_isin_and_position_sets_before_sql() -> None:
    session = _FakeSession([])

    with pytest.raises(BusinessQueryValidationError, match="positions and isins"):
        await execute_business_query(
            session,
            BusinessQueryInput(
                query_name="Conflicting positions",
                isins=("IE00BMTX1Y45",),
                legal_entity_type="natural person",
                amount_multiplier=Decimal("1"),
                positions=(BusinessQueryPosition(isin="LU1681044993", amount=Decimal("4")),),
            ),
        )

    assert session.statement is None


@pytest.mark.parametrize(
    "positions",
    [
        (BusinessQueryPosition(isin="not-an-isin", amount=Decimal("1")),),
        (BusinessQueryPosition(isin="IE00BMTX1Y45", amount=Decimal("0")),),
        (
            BusinessQueryPosition(isin="IE00BMTX1Y45", amount=Decimal("1")),
            BusinessQueryPosition(isin="ie00bmtx1y45", amount=Decimal("2")),
        ),
    ],
)
@pytest.mark.asyncio
async def test_business_query_rejects_invalid_positions_before_sql(
    positions: tuple[BusinessQueryPosition, ...],
) -> None:
    session = _FakeSession([])

    with pytest.raises(BusinessQueryValidationError):
        await execute_business_query(
            session,
            BusinessQueryInput(
                query_name="Bad positions",
                isins=(),
                legal_entity_type="business",
                amount_multiplier=Decimal("1"),
                positions=positions,
            ),
        )

    assert session.statement is None


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
    assert result.rows[0].base_home_currency_value is None
    assert result.rows[0].calculated_home_currency_value is None
    assert result.rows[0].base_eur_value is None
    assert result.rows[0].calculated_eur_value is None


@pytest.mark.asyncio
async def test_business_query_keeps_home_currency_values_when_eur_conversion_is_null() -> None:
    session = _FakeSession(
        [
            _view_row(
                FNDCCY="USD",
                FXRAT=None,
                K40PVM_HOMCCY=Decimal("12.5000000000"),
                K40PVM_EUR=None,
            )
        ]
    )

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Missing FX",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("3"),
            tax_fields=("K40",),
            subcategory_key="natural_person_pa_with_option",
        ),
    )

    assert result.count == 1
    row = result.rows[0]
    assert row.fund_currency == "USD"
    assert row.home_currency_code == "USD"
    assert row.original_currency_code == "USD"
    assert row.fx_rate is None
    assert row.base_home_currency_value == Decimal("12.5000000000")
    assert row.calculated_home_currency_value == Decimal("37.5000000000")
    assert row.base_eur_value is None
    assert row.calculated_eur_value is None


@pytest.mark.asyncio
async def test_business_query_returns_distinct_home_and_eur_values_for_non_eur_currency() -> None:
    session = _FakeSession(
        [
            _view_row(
                FNDCCY="USD",
                FXRAT=Decimal("1.2500000000"),
                K40BVO_HOMCCY=Decimal("25.0000000000"),
                K40BVO_EUR=Decimal("20.0000000000"),
            )
        ]
    )

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Dual currency",
            isins=("IE00BMTX1Y45",),
            legal_entity_type="business",
            amount_multiplier=Decimal("2"),
            tax_fields=("K40",),
            subcategory_key="business_bv_without_option",
            tax_year_filter=2025,
        ),
    )

    assert result.count == 1
    row = result.rows[0]
    assert row.base_home_currency_value == Decimal("25.0000000000")
    assert row.calculated_home_currency_value == Decimal("50.0000000000")
    assert row.base_eur_value == Decimal("20.0000000000")
    assert row.calculated_eur_value == Decimal("40.0000000000")


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
    assert result.missing_year_isins == ()


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
    assert result.missing_year_isins == ()


@pytest.mark.asyncio
async def test_business_query_most_recent_common_available_year_resolves_before_main_query() -> None:
    availability_rows = [
        _view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2023),
        _view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2025),
        _view_row(TAXISN="LU1681044993", TAXYEA=2023),
        _view_row(TAXISN="LU1681044993", TAXYEA=2024),
    ]
    main_rows = [
        _view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2023),
        _view_row(TAXISN="LU1681044993", TAXYEA=2023),
    ]
    session = _FakeSession([], execute_rows=[availability_rows, main_rows])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="Latest common",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="business",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            subcategory_key="business_bv_with_option",
            tax_year_filter=MOST_RECENT_COMMON_AVAILABLE_YEAR,
        ),
    )

    assert len(session.statements) == 2
    availability_sql = _compiled_sql(session.statements[0])
    main_sql = _compiled_sql(session.statements[1])
    assert 'FROM "V2_TAXDATHOMCCY"' in availability_sql
    assert "DISTINCT" in availability_sql
    assert '"TAXYEA" IS NOT NULL' in availability_sql
    assert '"TAXYEA" = 2023' in main_sql
    assert result.query.tax_year_filter == "2023"
    assert result.query.year == 2023
    assert result.count == 2
    assert result.no_common_year_isins == ()


@pytest.mark.asyncio
async def test_business_query_most_recent_common_available_year_returns_structured_no_common_result() -> None:
    availability_rows = [
        _view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2025),
        _view_row(TAXISN="LU1681044993", TAXYEA=2024),
    ]
    session = _FakeSession([], execute_rows=[availability_rows])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="No common year",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            tax_year_filter=MOST_RECENT_COMMON_AVAILABLE_YEAR,
        ),
    )

    assert len(session.statements) == 1
    assert result.is_empty is True
    assert result.rows == ()
    assert result.query.tax_year_filter == MOST_RECENT_COMMON_AVAILABLE_YEAR
    assert result.no_common_year_isins == ("IE00BMTX1Y45", "LU1681044993")
    assert result.no_common_year_messages == (
        "No common tax year is available for every submitted ISIN: IE00BMTX1Y45, LU1681044993.",
    )
    assert result.missing_year_isins == ()


@pytest.mark.asyncio
async def test_business_query_specific_tax_year_reports_one_missing_isin() -> None:
    session = _FakeSession([_view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2025)])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="One missing",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            tax_year_filter=2025,
        ),
    )

    assert result.count == 2
    assert {row.isin for row in result.rows} == {"IE00BMTX1Y45"}
    assert result.missing_year_isins == ("LU1681044993",)
    assert result.missing_year_messages == (
        "Data for ISIN LU1681044993 is not available for the selected year.",
    )


@pytest.mark.asyncio
async def test_business_query_specific_tax_year_reports_all_missing_isins() -> None:
    session = _FakeSession([])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="All missing",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            tax_year_filter=2025,
        ),
    )

    assert result.count == 0
    assert result.is_empty is True
    assert result.missing_year_isins == ("IE00BMTX1Y45", "LU1681044993")


@pytest.mark.asyncio
async def test_business_query_specific_tax_year_reports_no_missing_isins_when_all_have_data() -> None:
    session = _FakeSession(
        [
            _view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2025),
            _view_row(TAXISN="LU1681044993", TAXYEA=2025),
        ]
    )

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="None missing",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="business",
            amount_multiplier=Decimal("2"),
            tax_fields=("K61",),
            subcategory_key="business_bv_with_option",
            tax_year_filter=2025,
        ),
    )

    assert result.count == 2
    assert {row.isin for row in result.rows} == {"IE00BMTX1Y45", "LU1681044993"}
    assert result.missing_year_isins == ()


@pytest.mark.asyncio
async def test_business_query_all_available_years_does_not_report_missing_selected_year_data() -> None:
    session = _FakeSession([_view_row(TAXISN="IE00BMTX1Y45", TAXYEA=2024)])

    result = await execute_business_query(
        session,
        BusinessQueryInput(
            query_name="All years availability",
            isins=("IE00BMTX1Y45", "LU1681044993"),
            legal_entity_type="natural person",
            amount_multiplier=Decimal("1"),
            tax_fields=("K40",),
            tax_year_filter=ALL_AVAILABLE_YEARS,
        ),
    )

    assert result.count == 2
    assert result.missing_year_isins == ()


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
    assert result.missing_year_isins == ()


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
    assert result.missing_year_isins == ()


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
