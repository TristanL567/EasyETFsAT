from __future__ import annotations

from fondant.db.models import SOURCEAGE
from fondant.ingestion import pipeline
from fondant.oekb import parser
from fondant.oekb.models import ParsedCategoryValues, ParsedTaxAge
from fondant.tax_registry import TAX_CATEGORIES, TAX_LINES


def test_tax_registry_contains_active_live_line_and_category_codes() -> None:
    assert [line.line_code for line in TAX_LINES] == [
        "K40",
        "K11",
        "K12",
        "K81",
        "K82",
        "K10",
        "K55",
        "K61",
        "K62",
        "K36",
        "K21",
    ]
    assert [category.category_code for category in TAX_CATEGORIES] == [
        "PVM",
        "PVO",
        "BVM",
        "BVO",
        "BVJ",
        "STF",
    ]


def test_parser_and_seed_dictionaries_are_derived_from_registry() -> None:
    assert parser.METRIC_CODE_BY_KEY == {line.metric_key: line.line_code for line in TAX_LINES}
    assert parser.CATEGORY_CODE_BY_KEY == {
        category.category_key: category.category_code for category in TAX_CATEGORIES
    }
    assert parser.TAX_FIELD_MAP == {
        source_name: line.metric_key for line in TAX_LINES for source_name in line.source_tax_names
    }
    assert parser.CATEGORY_KEY_MAP == {
        alias: category.category_key for category in TAX_CATEGORIES for alias in category.parser_aliases
    }

    assert tuple(pipeline.LINE_DICTIONARY) == tuple(
        {
            "line_code": line.line_code,
            "metric_key": line.metric_key,
            "name_de": line.name_de,
            "name_en": line.name_en,
            "line_order": line.line_order,
            "description": line.description,
            "usage_note": line.usage_note,
            "source_label": line.source_label,
        }
        for line in TAX_LINES
    )
    assert tuple(pipeline.CATEGORY_DICTIONARY) == tuple(
        {
            "category_code": category.category_code,
            "category_key": category.category_key,
            "name_de": category.name_de,
            "name_en": category.name_en,
            "category_order": category.category_order,
        }
        for category in TAX_CATEGORIES
    )


def test_line_dictionary_includes_taxlin_description_metadata_for_all_rows() -> None:
    assert len(pipeline.LINE_DICTIONARY) == 11
    assert all(line["description"] for line in pipeline.LINE_DICTIONARY)
    assert all(line["usage_note"] for line in pipeline.LINE_DICTIONARY)
    assert {
        line["source_label"]
        for line in pipeline.LINE_DICTIONARY
    } == {
        "OeKB Feldliste Steuerdaten Fonds (gesamt), Gueltig ab 14.04.2025, Vers. 07.10.2024"
    }


def test_parser_output_model_matches_registry_api_aliases() -> None:
    assert set(ParsedTaxAge.model_fields) == {line.metric_key for line in TAX_LINES}
    assert set(ParsedCategoryValues.model_fields) == {category.category_key for category in TAX_CATEGORIES}


def test_source_columns_and_view_aliases_match_registry() -> None:
    sourceage_columns = {column.name for column in SOURCEAGE.__table__.columns}
    expected_source_columns = {
        f"SRC{line.line_code}{category.source_alias}"
        for line in TAX_LINES
        for category in TAX_CATEGORIES
    }
    assert {column for column in sourceage_columns if column.startswith("SRCK")} == expected_source_columns

    view_lines = {"K40", "K61", "K62"}
    expected_view_columns = {
        f"{line.line_code}{category.view_alias}"
        for line in TAX_LINES
        if line.line_code in view_lines
        for category in TAX_CATEGORIES
    }
    assert {"K40STI", "K61STI", "K62STI"}.issubset(expected_view_columns)

    stiftung = next(category for category in TAX_CATEGORIES if category.category_key == "stiftung")
    assert stiftung.category_code == "STF"
    assert stiftung.source_alias == "STF"
    assert stiftung.view_alias == "STI"
    assert stiftung.alias_decision == "STF remains the source/category code; STI is the existing reporting-view alias."
