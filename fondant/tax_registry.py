from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TaxLine:
    line_code: str
    metric_key: str
    name_de: str
    name_en: str
    line_order: int
    source_tax_names: tuple[str, ...]
    description: str
    usage_note: str
    source_label: str


@dataclass(frozen=True, slots=True)
class TaxCategory:
    category_code: str
    category_key: str
    name_de: str
    name_en: str
    category_order: int
    parser_aliases: tuple[str, ...]
    source_alias: str
    view_alias: str
    alias_decision: str | None = None


TAX_SOURCE_LABEL = "OeKB Feldliste Steuerdaten Fonds (gesamt), Gueltig ab 14.04.2025, Vers. 07.10.2024"

TAX_LINES: tuple[TaxLine, ...] = (
    TaxLine(
        line_code="K40",
        metric_key="steuerpflichtige_einkuenfte",
        name_de="Steuerpflichtige Einkuenfte",
        name_en="taxable_income",
        line_order=5,
        source_tax_names=("StB_Einkuenfte_steuerpflichtig",),
        description="Steuerpflichtige Einkuenfte",
        usage_note="Primary taxable income amount for the selected investor category.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K11",
        metric_key="ag_ertraege",
        name_de="AGErtraege",
        name_en="distributed_income",
        line_order=10,
        source_tax_names=("StB_E1KV_AGErtraege",),
        description="Ausschuettungsgleiche Ertraege 27.5% for tax declaration fields 936 or 937.",
        usage_note="Distributed-equivalent income amount for income-tax reporting contexts.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K12",
        metric_key="korrekturbetrag_saldiert",
        name_de="Korrekturbetrag saldiert",
        name_en="net_correction_amount",
        line_order=20,
        source_tax_names=("StB_E1KV_Korrekturbetrag_saldiert",),
        description="Amount by which the fund share acquisition cost is corrected.",
        usage_note="Net correction amount used to adjust acquisition cost in income-tax reporting.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K81",
        metric_key="kest_total",
        name_de="KESt gesamt",
        name_en="withholding_tax_total",
        line_order=30,
        source_tax_names=("StB_KESt",),
        description="Austrian KESt collected by tax withholding.",
        usage_note="Total Austrian capital gains withholding tax for the selected investor category.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K82",
        metric_key="kest_substanzgewinne",
        name_de="KESt Substanzgewinne",
        name_en="withholding_tax_substance_gains",
        line_order=40,
        source_tax_names=("StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2",),
        description="KESt on capital income under Section 27(3), 27(4), and 27b(3) EStG 1998.",
        usage_note="Withholding tax attributable to substance or capital gains.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K10",
        metric_key="substanzgewinne_kestpfl",
        name_de="Substanzgewinne KESt-pflichtig",
        name_en="taxable_substance_gains",
        line_order=50,
        source_tax_names=("StB_Substanzgewinne_KEStpflichtig",),
        description="KESt-liable capital income under Section 27(3), 27(4), and 27b(3) EStG 1998.",
        usage_note="Taxable substance or capital gains amount subject to KESt treatment.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K55",
        metric_key="fondsergebnis_nichtausg",
        name_de="Fondsergebnis nicht ausgeschuettet",
        name_en="undistributed_fund_result",
        line_order=60,
        source_tax_names=("StB_Fondsergebnis_nichtausgeschuettet",),
        description="Undistributed fund result before loss carryforwards and withholding taxes.",
        usage_note="Undistributed fund result before considering loss carryforwards and withholding taxes.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K61",
        metric_key="korrekturbetrag_age_ak",
        name_de="Korrekturbetrag Anschaffungskosten",
        name_en="cost_basis_adjustment",
        line_order=70,
        source_tax_names=("StB_Korrekturbetrag_AGErtrag_Anschaffungskosten",),
        description="Correction amount for distributed-equivalent income acquisition costs.",
        usage_note="Cost-basis adjustment related to distributed-equivalent income.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K62",
        metric_key="korrekturbetrag_aussch_ak",
        name_de="Korrekturbetrag Ausschuettung Anschaffungskosten",
        name_en="distribution_cost_basis_adjustment",
        line_order=75,
        source_tax_names=("StB_Korrekturbetrag_Ausschuettung_Anschaffungskosten",),
        description="Correction amount for distribution acquisition costs.",
        usage_note="Distribution-related cost-basis adjustment for the selected investor category.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K36",
        metric_key="substanzgew_folgejahre",
        name_de="Substanzgewinn Folgejahre",
        name_en="taxable_substance_gain_followup_years",
        line_order=80,
        source_tax_names=("StB_Substanzgewinn_steuerpflichtig_beiAusschuettunginFolgejahren",),
        description="Capital income taxable on later-year distribution or share sale.",
        usage_note="Substance or capital gains taxable only on later-year distributions or sale.",
        source_label=TAX_SOURCE_LABEL,
    ),
    TaxLine(
        line_code="K21",
        metric_key="quellensteuern_einbeh",
        name_de="Quellensteuern einbehalten",
        name_en="withholding_taxes_retained",
        line_order=90,
        source_tax_names=("StB_Abzugsteuern_einbehalten_Kapitaleinkuenfte",),
        description="Retained domestic and foreign withholding taxes on capital income.",
        usage_note="Retained withholding taxes on capital income for the selected investor category.",
        source_label=TAX_SOURCE_LABEL,
    ),
)

TAX_CATEGORIES: tuple[TaxCategory, ...] = (
    TaxCategory(
        category_code="PVM",
        category_key="pv_mit",
        name_de="Privatvermoegen mit Option",
        name_en="private_assets_with_option",
        category_order=10,
        parser_aliases=("pvmitoption4", "pv_mit", "pvm"),
        source_alias="PVM",
        view_alias="PVM",
    ),
    TaxCategory(
        category_code="PVO",
        category_key="pv_ohne",
        name_de="Privatvermoegen ohne Option",
        name_en="private_assets_without_option",
        category_order=20,
        parser_aliases=("pvohneoption4", "pv_ohne"),
        source_alias="PVO",
        view_alias="PVO",
    ),
    TaxCategory(
        category_code="BVM",
        category_key="bv_mit",
        name_de="Betriebsvermoegen mit Option",
        name_en="business_assets_with_option",
        category_order=30,
        parser_aliases=("bvmitoption4", "bv_mit"),
        source_alias="BVM",
        view_alias="BVM",
    ),
    TaxCategory(
        category_code="BVO",
        category_key="bv_ohne",
        name_de="Betriebsvermoegen ohne Option",
        name_en="business_assets_without_option",
        category_order=40,
        parser_aliases=("bvohneoption4", "bv_ohne"),
        source_alias="BVO",
        view_alias="BVO",
    ),
    TaxCategory(
        category_code="BVJ",
        category_key="bv_jur",
        name_de="Betriebsvermoegen juristisch",
        name_en="business_assets_legal_entities",
        category_order=50,
        parser_aliases=("bvjurperson4", "bvjur", "bv_jur"),
        source_alias="BVJ",
        view_alias="BVJ",
    ),
    TaxCategory(
        category_code="STF",
        category_key="stiftung",
        name_de="Stiftung",
        name_en="foundation",
        category_order=60,
        parser_aliases=("stiftung4", "stiftung"),
        source_alias="STF",
        view_alias="STI",
        alias_decision="STF remains the source/category code; STI is the existing reporting-view alias.",
    ),
)

METRIC_CODE_BY_KEY: dict[str, str] = {line.metric_key: line.line_code for line in TAX_LINES}
CATEGORY_CODE_BY_KEY: dict[str, str] = {category.category_key: category.category_code for category in TAX_CATEGORIES}
TAX_FIELD_MAP: dict[str, str] = {
    source_name: line.metric_key for line in TAX_LINES for source_name in line.source_tax_names
}
CATEGORY_KEY_MAP: dict[str, str] = {
    alias: category.category_key for category in TAX_CATEGORIES for alias in category.parser_aliases
}

LINE_DICTIONARY: tuple[dict[str, object], ...] = tuple(
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

CATEGORY_DICTIONARY: tuple[dict[str, object], ...] = tuple(
    {
        "category_code": category.category_code,
        "category_key": category.category_key,
        "name_de": category.name_de,
        "name_en": category.name_en,
        "category_order": category.category_order,
    }
    for category in TAX_CATEGORIES
)
