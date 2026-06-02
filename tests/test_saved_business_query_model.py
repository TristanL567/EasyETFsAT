from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from fondant.db.models import ALL_AVAILABLE_YEARS, BQGROUP, BQSAVED


def _create_bqsaved_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    BQGROUP.__table__.create(engine)
    BQSAVED.__table__.create(engine)
    return Session(engine)


def test_saved_business_query_group_model_exposes_required_columns() -> None:
    columns = BQGROUP.__table__.columns

    assert "BQGIDN" in columns
    assert "BQGCRTDTS" in columns
    assert "BQGUPDDTS" in columns
    assert BQGROUP.owner_username.property.columns[0].name == "BQGUSR"
    assert BQGROUP.group_name.property.columns[0].name == "BQGNAM"
    assert BQGROUP.description.property.columns[0].name == "BQGDSC"


def test_saved_business_query_model_exposes_required_columns() -> None:
    columns = BQSAVED.__table__.columns

    assert "BQSIDN" in columns
    assert "BQSCRTDTS" in columns
    assert "BQSUPDDTS" in columns
    assert BQSAVED.owner_username.property.columns[0].name == "BQSUSR"
    assert BQSAVED.group_id.property.columns[0].name == "BQSGRPIDN"
    assert BQSAVED.query_name.property.columns[0].name == "BQSNAM"
    assert BQSAVED.legal_entity_type.property.columns[0].name == "BQSLENTYP"
    assert BQSAVED.subcategory_key.property.columns[0].name == "BQSSUBCAT"
    assert BQSAVED.tax_year_filter.property.columns[0].name == "BQSTXYR"
    assert BQSAVED.amount.property.columns[0].name == "BQSAMT"
    assert BQSAVED.note.property.columns[0].name == "BQSNOTE"
    assert BQSAVED.default_isins.property.columns[0].name == "BQSISNS"
    assert BQSAVED.selected_tax_fields.property.columns[0].name == "BQSTXFLDS"
    assert BQSAVED.ordered_positions.property.columns[0].name == "BQSPOSNS"


def test_saved_business_query_unique_name_is_scoped_to_owner() -> None:
    with _create_bqsaved_session() as session:
        session.add_all(
            [
                BQSAVED(
                    owner_username="alice",
                    query_name="Quarterly review",
                    legal_entity_type="natural person",
                    subcategory_key="natural_person_all",
                    tax_year_filter=ALL_AVAILABLE_YEARS,
                    amount=Decimal("100.00"),
                ),
                BQSAVED(
                    owner_username="bob",
                    query_name="Quarterly review",
                    legal_entity_type="business",
                    subcategory_key="business_all",
                    tax_year_filter="2025",
                    amount=Decimal("250.00"),
                ),
            ]
        )
        session.commit()

        session.add(
            BQSAVED(
                owner_username="alice",
                query_name="Quarterly review",
                legal_entity_type="Stiftung",
                subcategory_key="stiftung",
                tax_year_filter=ALL_AVAILABLE_YEARS,
                amount=Decimal("500.00"),
            )
        )

        with pytest.raises(IntegrityError):
            session.commit()


def test_saved_business_query_group_unique_name_is_scoped_to_owner() -> None:
    with _create_bqsaved_session() as session:
        session.add_all(
            [
                BQGROUP(owner_username="alice", group_name="Quarterly review"),
                BQGROUP(owner_username="bob", group_name="Quarterly review"),
            ]
        )
        session.commit()

        session.add(BQGROUP(owner_username="alice", group_name="Quarterly review"))

        with pytest.raises(IntegrityError):
            session.commit()


def test_saved_business_query_optional_fields_accept_null_and_structured_lists() -> None:
    with _create_bqsaved_session() as session:
        no_optional_values = BQSAVED(
            owner_username="alice",
            query_name="No defaults",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter=ALL_AVAILABLE_YEARS,
            amount=Decimal("100.00"),
            note=None,
            default_isins=None,
            selected_tax_fields=None,
            ordered_positions=None,
        )
        with_structured_lists = BQSAVED(
            owner_username="alice",
            query_name="With defaults",
            legal_entity_type="business",
            subcategory_key="business_all",
            tax_year_filter="2025",
            amount=Decimal("250.00"),
            note="Run for model portfolio",
            default_isins=["AT0000A0ETF1", "AT0000A0ETF2"],
            selected_tax_fields=["K11", "K61"],
            ordered_positions=[
                {"isin": "AT0000A0ETF2", "amount": "3.5"},
                {"isin": "AT0000A0ETF1", "amount": "2"},
            ],
        )
        session.add_all([no_optional_values, with_structured_lists])
        session.commit()

        assert no_optional_values.note is None
        assert no_optional_values.default_isins is None
        assert no_optional_values.selected_tax_fields is None
        assert no_optional_values.ordered_positions is None
        assert with_structured_lists.note == "Run for model portfolio"
        assert with_structured_lists.default_isins == ["AT0000A0ETF1", "AT0000A0ETF2"]
        assert with_structured_lists.selected_tax_fields == ["K11", "K61"]
        assert with_structured_lists.ordered_positions == [
            {"isin": "AT0000A0ETF2", "amount": "3.5"},
            {"isin": "AT0000A0ETF1", "amount": "2"},
        ]


def test_saved_business_query_preserves_legacy_defaults_without_ordered_positions() -> None:
    with _create_bqsaved_session() as session:
        saved_query = BQSAVED(
            owner_username="alice",
            query_name="Legacy defaults",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter=ALL_AVAILABLE_YEARS,
            amount=Decimal("100.00"),
            default_isins=["AT0000A0ETF1", "AT0000A0ETF2"],
        )
        session.add(saved_query)
        session.commit()

        assert saved_query.default_isins == ["AT0000A0ETF1", "AT0000A0ETF2"]
        assert saved_query.ordered_positions is None


def test_saved_business_query_can_store_single_selected_tax_field() -> None:
    with _create_bqsaved_session() as session:
        saved_query = BQSAVED(
            owner_username="alice",
            query_name="Single field",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter=ALL_AVAILABLE_YEARS,
            amount=Decimal("100.00"),
            selected_tax_fields=["K40"],
        )
        session.add(saved_query)
        session.commit()

        assert saved_query.selected_tax_fields == ["K40"]


def test_saved_business_query_can_be_created_without_group() -> None:
    with _create_bqsaved_session() as session:
        saved_query = BQSAVED(
            owner_username="alice",
            group_id=None,
            query_name="No group",
            legal_entity_type="natural person",
            subcategory_key="natural_person_all",
            tax_year_filter=ALL_AVAILABLE_YEARS,
            amount=Decimal("100.00"),
        )
        session.add(saved_query)
        session.commit()

        assert saved_query.group_id is None


def test_saved_business_query_can_reference_group() -> None:
    with _create_bqsaved_session() as session:
        group = BQGROUP(
            owner_username="alice",
            group_name="Quarterly review",
            description="Recurring client review",
        )
        session.add(group)
        session.flush()

        saved_query = BQSAVED(
            owner_username="alice",
            group_id=group.id,
            query_name="With group",
            legal_entity_type="business",
            subcategory_key="business_all",
            tax_year_filter="2025",
            amount=Decimal("250.00"),
        )
        session.add(saved_query)
        session.commit()

        assert saved_query.group_id == group.id
