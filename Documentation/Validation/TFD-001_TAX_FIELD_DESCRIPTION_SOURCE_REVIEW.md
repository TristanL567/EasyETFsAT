# TFD-001 Tax Field Description Source Review

Ticket: TFD-001  
Purpose: source-backed review contract for adding tax field descriptions later.  
Status: documentation only; no database, migration, code, ingestion, or frontend changes are made here.

## Source Review

OeKB is the canonical source for Austrian fund tax field descriptions used by this repository. The reviewed OeKB sources are:

- [OeKB Meldungen von Steuerdaten](https://www.oekb.at/kapitalmarkt-services/meldungen-und-hinterlegungen-von-dokumenten/meldungen-zu-investmentfonds/meldungen-von-steuerdaten.html): describes OeKB's role as Meldestelle for tax-relevant fund data and links the field-list documents.
- [OeKB Feldliste Steuerdaten Fonds (gesamt)](https://www.oekb.at/dam/jcr%3A5a83a4b7-c695-45ab-ad87-2de5b811a580/Feldliste_Steuerdaten_Fonds_%28gesamt%29_2024-10-07.pdf): official field list, "Gueltig ab 14.04.2025 (Vers. 07.10.2024)".

The table below separates source text from explanatory usage text. The `OeKB/source description` column uses OeKB wording where the reviewed field list clearly identifies the current source tax name. The `User-facing usage note` column is maintainer/operator-facing explanation and may be logically filled where OeKB text is too technical, split across PDF lines, or not a complete user-facing description. Inferred text is marked in `Source confidence` and `Notes`.

## Current TAXLIN Inventory

Current authoritative code inventory:

- `fondant/tax_registry.py` defines `TAX_LINES`, source OeKB `StB_*` names, metric keys, German labels, English labels, and display order.
- `fondant/tax_registry.py` derives `LINE_DICTIONARY`, which ingestion uses to seed tax line dictionaries.
- `fondant/db/models/tax.py` defines `TAXLIN` with current fields `TAXCOD`, `TAXKEY`, `TAXNDE`, `TAXNEN`, `TAXORD`, `TAXACT`, `TAXGVN`, and `TAXGBS`.
- `alembic/versions/20260419_0006_rebuild_source_curated_architecture.py` creates the current `TAXLIN` table shape.
- `tests/test_tax_registry.py` checks that parser, seed dictionary, and registry tax line definitions stay aligned.

Current active tax line rows from `TAX_LINES`:

| Internal ID | TAXKEY | German label | English label | Source tax name |
| --- | --- | --- | --- | --- |
| K40 | steuerpflichtige_einkuenfte | Steuerpflichtige Einkuenfte | taxable_income | StB_Einkuenfte_steuerpflichtig |
| K11 | ag_ertraege | AGErtraege | distributed_income | StB_E1KV_AGErtraege |
| K12 | korrekturbetrag_saldiert | Korrekturbetrag saldiert | net_correction_amount | StB_E1KV_Korrekturbetrag_saldiert |
| K81 | kest_total | KESt gesamt | withholding_tax_total | StB_KESt |
| K82 | kest_substanzgewinne | KESt Substanzgewinne | withholding_tax_substance_gains | StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2 |
| K10 | substanzgewinne_kestpfl | Substanzgewinne KESt-pflichtig | taxable_substance_gains | StB_Substanzgewinne_KEStpflichtig |
| K55 | fondsergebnis_nichtausg | Fondsergebnis nicht ausgeschuettet | undistributed_fund_result | StB_Fondsergebnis_nichtausgeschuettet |
| K61 | korrekturbetrag_age_ak | Korrekturbetrag Anschaffungskosten | cost_basis_adjustment | StB_Korrekturbetrag_AGErtrag_Anschaffungskosten |
| K62 | korrekturbetrag_aussch_ak | Korrekturbetrag Ausschuettung Anschaffungskosten | distribution_cost_basis_adjustment | StB_Korrekturbetrag_Ausschuettung_Anschaffungskosten |
| K36 | substanzgew_folgejahre | Substanzgewinn Folgejahre | taxable_substance_gain_followup_years | StB_Substanzgewinn_steuerpflichtig_beiAusschuettunginFolgejahren |
| K21 | quellensteuern_einbeh | Quellensteuern einbehalten | withholding_taxes_retained | StB_Abzugsteuern_einbehalten_Kapitaleinkuenfte |

## Field Mapping Table

| Internal ID | TAXKEY | German label | English label | OeKB/source description | User-facing usage note | Source confidence | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| K40 | steuerpflichtige_einkuenfte | Steuerpflichtige Einkuenfte | taxable_income | Official OeKB source row for `StB_Einkuenfte_steuerpflichtig`: "Steuerpflichtige Einkuenfte"; the PDF footnote says values for legal entities and foundations are added to taxable income in the tax return. | Inferred usage: taxable income amount for the selected investor category; use as the primary taxable-income measure in curated tax facts and downstream views. | High: OeKB field-list row and repo registry agree on the source name. | Keep the source-backed short description separate from the broader user-facing usage note. |
| K11 | ag_ertraege | AGErtraege | distributed_income | Official OeKB source row for `StB_E1KV_AGErtraege`: "Ausschuettungsgleiche Ertraege 27,5% (Kennzahlen 936 oder 937)" with a note about negative values and actual holder entitlement. | Inferred usage: distributed-equivalent income amount reported for income tax declaration contexts; used as a tax metric by parser, curation, and API tax fields. | High: OeKB field-list row and repo registry agree on the source name. | Repo English label says `distributed_income`; the OeKB wording is closer to "ausschuettungsgleiche Ertraege". Human review should decide whether to refine the label later. |
| K12 | korrekturbetrag_saldiert | Korrekturbetrag saldiert | net_correction_amount | Official OeKB source row for `StB_E1KV_Korrekturbetrag_saldiert`: "Die Anschaffungskosten des Fondsanteils sind zu korrigieren um". | Inferred usage: net correction amount used to adjust the fund share acquisition cost in the income-tax reporting context. | High: OeKB field-list row and repo registry agree on the source name. | The OeKB row is concise and technical; user-facing text should explain it as an acquisition-cost correction, not as tax advice. |
| K81 | kest_total | KESt gesamt | withholding_tax_total | Official OeKB source row for `StB_KESt`: "Oesterreichische KESt, die durch Steuerabzug erhoben wird". | Inferred usage: total Austrian capital gains withholding tax amount reported for the selected investor category. | High: OeKB field-list row and repo registry agree on the source name. | Keep as a tax amount field; do not imply that it is payable or refundable without consumer-specific tax context. |
| K82 | kest_substanzgewinne | KESt Substanzgewinne | withholding_tax_substance_gains | Official OeKB source row for `StB_KESt_Substanzgewinne_sonstige_steuerpflichtig_2`: "KESt auf Einkuenfte aus Kapitalvermoegen gem. Section 27 Abs. 3 und 4 sowie Section 27b Abs. 3 EStG 1998". | Inferred usage: withholding tax attributable to substance/capital gains for the selected investor category. | High: OeKB field-list row and repo registry agree on the source name, allowing case-normalized `KeSt`/`KESt`. | Current registry source key uses `StB_KeSt_...`; the PDF displays `StB_KESt_...`. Treat as the same OeKB tax-name family unless parser evidence proves otherwise. |
| K10 | substanzgewinne_kestpfl | Substanzgewinne KESt-pflichtig | taxable_substance_gains | Official OeKB source row for `StB_Substanzgewinne_KEStpflichtig`: "KESt-pflichtige Einkuenfte aus Kapitalvermoegen gem. Section 27 Abs. 3 und 4 sowie Section 27b Abs. 3 EStG 1998 (inkl. Altemissionen)". | Inferred usage: taxable substance/capital gains amount subject to KESt treatment for the selected investor category. | High: OeKB field-list row and repo registry agree on the source name. | The German repo label is shorter than the OeKB source text; keep OeKB text in `TAXDSC` if columns are approved. |
| K55 | fondsergebnis_nichtausg | Fondsergebnis nicht ausgeschuettet | undistributed_fund_result | Official OeKB source row for `StB_Fondsergebnis_nichtausgeschuettet`: "Nicht ausgeschuettetes Fondsergebnis ohne Beruecksichtigung von Verlustvortraegen und Quellensteuern". | Inferred usage: undistributed fund result before considering loss carryforwards and withholding taxes. | High: OeKB field-list row and repo registry agree on the source name. | Use the OeKB qualifier in descriptions so consumers do not treat this as a complete net result. |
| K61 | korrekturbetrag_age_ak | Korrekturbetrag Anschaffungskosten | cost_basis_adjustment | Official OeKB source row for `StB_Korrekturbetrag_AGErtrag_Anschaffungskosten`: correction amount for distributed-equivalent income acquisition costs; OeKB notes it increases acquisition costs. | Inferred usage: cost-basis adjustment related to distributed-equivalent income for the selected investor category. | High: OeKB field-list row and repo registry agree on the source name. | Field lineage docs already use this as `korrekturbetrag_age_ak`; do not rename casually because views expose K61 category pivots. |
| K62 | korrekturbetrag_aussch_ak | Korrekturbetrag Ausschuettung Anschaffungskosten | distribution_cost_basis_adjustment | Official OeKB source row for `StB_Korrekturbetrag_Ausschuettung_Anschaffungskosten`: correction amount for distribution acquisition costs; OeKB notes it decreases acquisition costs. | Inferred usage: distribution-related cost-basis adjustment for the selected investor category. | High: OeKB field-list row and repo registry agree on the source name, despite PDF line wrap splitting the source key. | Later `TAXDSC` should preserve the increase/decrease distinction between K61 and K62. |
| K36 | substanzgew_folgejahre | Substanzgewinn Folgejahre | taxable_substance_gain_followup_years | Official OeKB source row for `StB_Substanzgewinn_steuerpflichtig_beiAusschuettunginFolgejahren`: "Erst bei Ausschuettung in Folgejahren bzw. bei Verkauf der Anteile steuerpflichtige Ertraege gem. Section 27 Abs. 3 und 4 sowie Section 27b Abs. 3 EStG 1988 (inkl. Altemissionen)". | Inferred usage: substance/capital gains that become taxable only on later-year distributions or sale of fund shares. | High: OeKB field-list row and repo registry agree on the source name, with PDF line wrapping. | Repo label is compact; user-facing usage should include the later-year distribution/sale condition. |
| K21 | quellensteuern_einbeh | Quellensteuern einbehalten | withholding_taxes_retained | Official OeKB source row for `StB_Abzugsteuern_einbehalten_Kapitaleinkuenfte`: "Einbehaltene in- und auslaendische Abzugsteuern auf Kapitaleinkuenfte"; OeKB notes country details are taken from the respective income-type sheets. | Inferred usage: retained domestic and foreign withholding taxes on capital income for the selected investor category. | High: OeKB field-list row and repo registry agree on the source name. | Source text refers to detail sheets outside the current TAXLIN row; do not imply country-level detail is present in TAXDAT. |

## Implementation Recommendation

Recommended later database shape, pending human approval:

- `TAXDSC`: short source-backed description. Populate from OeKB field-list text where a row is clearly identified. This should remain close to the OeKB wording and source key.
- `TAXUSE`: user-facing usage note. Populate with concise explanatory text for operators, maintainers, and UI consumers. This may include inferred wording, but inferred wording should remain traceable to the source row and should not become tax advice.
- Optional `TAXSRC`: source/version label. Suggested value pattern: `OeKB Feldliste Steuerdaten Fonds (gesamt), Gueltig ab 14.04.2025, Vers. 07.10.2024`.

Do not add these fields in TFD-001. A later implementation ticket should decide whether to add columns to `TAXLIN`, whether historical versioning is needed, and how seed/update logic should handle description changes across OeKB versions.

## Human Approval Gate

Human approval required before TFD-002:
- Confirm field mappings.
- Confirm descriptions and usage notes.
- Confirm whether inferred descriptions are acceptable.
- Confirm database columns to add.
