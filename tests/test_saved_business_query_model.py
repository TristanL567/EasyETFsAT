from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from fondant.db.models import ALL_AVAILABLE_YEARS, BQSAVED


def _create_bqsaved_session() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    BQSAVED.__table__.create(engine)
    return Session(engine)


def test_saved_business_query_model_exposes_required_columns() -> None:
    columns = BQSAVED.__table__.columns

    assert "BQSIDN" in columns
    assert "BQSCRTDTS" in columns
    assert "BQSUPDDTS" in columns
    assert BQSAVED.owner_username.property.columns[0].name == "BQSUSR"
    assert BQSAVED.query_name.property.columns[0].name == "BQSNAM"
    assert BQSAVED.legal_entity_type.property.columns[0].name == "BQSLENTYP"
    assert BQSAVED.subcategory_key.property.columns[0].name == "BQSSUBCAT"
    assert BQSAVED.tax_year_filter.property.columns[0].name == "BQSTXYR"
    assert BQSAVED.amount.property.columns[0].name == "BQSAMT"
    assert BQSAVED.note.property.columns[0].name == "BQSNOTE"
    assert BQSAVED.default_isins.property.columns[0].name == "BQSISNS"


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


def test_saved_business_query_optional_fields_accept_null_and_structured_isins() -> None:
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
        )
        with_default_isins = BQSAVED(
            owner_username="alice",
            query_name="With defaults",
            legal_entity_type="business",
            subcategory_key="business_all",
            tax_year_filter="2025",
            amount=Decimal("250.00"),
            note="Run for model portfolio",
            default_isins=["AT0000A0ETF1", "AT0000A0ETF2"],
        )
        session.add_all([no_optional_values, with_default_isins])
        session.commit()

        assert no_optional_values.note is None
        assert no_optional_values.default_isins is None
        assert with_default_isins.note == "Run for model portfolio"
        assert with_default_isins.default_isins == ["AT0000A0ETF1", "AT0000A0ETF2"]
