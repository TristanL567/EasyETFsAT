# FD-002 OeKB Source Field Catalog

Ticket: FD-002  
Epic: Field Lineage Documentation  
Scope: OeKB report-list metadata, raw report-detail payloads, and parsed source matrix fields consumed by `SOURCERPT`, `SOURCERAW`, and `SOURCEAGE`.

## Evidence Used

- `Documentation\DATA_AND_QUERY_GUIDE.md` for source table purpose, raw payload inspection, and query strategy.
- `Documentation\TECHNICAL_ARCHITECTURE.md` for OeKB client, parser, ingestion, and source-to-curated pipeline behavior.
- `Documentation\MAINTAINER_LAYER_GUIDE.md` for the source-shaped OeKB storage boundary.
- `Documentation\STAKEHOLDER_BRIEF.md` for audit/reconciliation purpose.
- `Documentation\AgentInstructions\FETCH_ONLY_MISSING_ISINS.md` and `Documentation\AgentInstructions\REFRESH_EXISTING_ISINS.md` for operational ingestion context.
- `Documentation\Validation\FD-001_DATABASE_FIELD_INVENTORY.md` for database object and column inventory.
- `Documentation\Validation\SE-003_TAX_CODE_REGISTRY.md` and `Documentation\Validation\SE-004_FOUNDATION_CATEGORY_ALIAS_DECISION.md` for tax registry and `STF`/`stiftung`/`STI` naming decisions.
- `fondant\oekb\models.py`, `fondant\oekb\client.py`, `fondant\oekb\parser.py`, `fondant\ingestion\pipeline.py`, `fondant\tax_registry.py`, and `fondant\db\models\tax.py`.
- `tests\test_oekb_client.py`, `tests\test_oekb_parser.py`, `tests\test_ingestion.py`, and `tests\test_tax_registry.py`.
- Read-only local PostgreSQL `SOURCERAW` inspection: 38 stored raw rows were sampled with `SELECT` only. Stored payloads used a top-level `list` containing report-detail rows and `expandedRows`.

## Layer Summary

| Source class | Destination | Behavior | Later transformation |
|---|---|---|---|
| OeKB report-list item | `SOURCERPT` | Metadata copied or derived for every returned report, including non-`FIN` reports. | Copied to `TAXRPT`; selected fields also drive `SECDIV` and `TAXCOR`. |
| OeKB report-detail response | `SOURCERAW` | Full raw detail payload stored for `FIN` reports only when new, newer-version, or same-version changed. | Authoritative audit archive and parser input; not narrowed before storage. |
| OeKB report-detail tax matrix | `SOURCEAGE` | Known `steuerName` rows and known category keys are parsed into wide numeric source columns. | Expanded to narrow `TAXDAT`; `K61` also becomes `TAXADJ` with `AKC`. |
| Unknown detail tax/category fields | None, except retained in `SOURCERAW` | Parser diagnostics are returned for unknown tax names, unknown scalar category keys, and invalid numeric values. Unknown values are not written to `SOURCEAGE`. | Not curated unless parser/registry support is added later. |

## `SOURCERPT` Report-List Metadata

`OeKBClient.get_report_list(...)` calls `/steuerMeldung/liste` with `ctxEqIsin`, `meldgNurGuelt=true`, and `meldgJahresM=true` by default. `build_sourcerpt_values(...)` copies typed fields from `OeKBReportListItem`, with fallback extraction from report-detail payload or Pydantic extra fields where implemented. The ingestion pipeline writes `SOURCERPT` for every returned report before it decides whether to fetch detail for `FIN` reports.

| OeKB source field/path | Meaning evident from code/docs | Destination | Handling | Null/missing behavior | Authoritative reference |
|---|---|---|---|---|---|
| Caller input ISIN / list `isin` | Security identifier requested from OeKB; response value is modeled but pipeline uses the caller input for source table keys. | `SOURCERPT.SRCISN` | Copied to source metadata. | Required by pipeline call; not derived from missing response `isin`. | `fondant\ingestion\pipeline.py`, `fondant\oekb\models.py` |
| `stmId` | OeKB report ID / Melde-ID. | `SOURCERPT.SRCOKBIDN` | Copied to source metadata and key. | Required by `OeKBReportListItem`. | `fondant\oekb\models.py`, `fondant\db\models\tax.py` |
| `versionsNr` | OeKB version number. | `SOURCERPT.SRCVRN` | Copied to source metadata. | Falls back to detail `versionsNr`, then `1`. | `fondant\oekb\parser.py` |
| `statusCode` | OeKB report status; `FIN` controls detail fetching. | `SOURCERPT.SRCSTS` | Copied to source metadata. | Falls back to detail `statusCode`; nullable. Non-`FIN` reports remain metadata-only. | `fondant\ingestion\pipeline.py`, `fondant\oekb\parser.py` |
| `jahr`, detail `jahr` / `reportYear` / `steuerjahr`, extra year-like fields | Report/tax year when available. | `SOURCERPT.SRCYEA` | Copied or derived source metadata. | If absent, derived from `meldgDatum`, `eintragezeit`, or recognized date/year extras; otherwise null. | `fondant\oekb\parser.py` |
| `meldgDatum` / `meldedatum` / extra date-like fields | Report/message date. | `SOURCERPT.SRCMDT` | Copied or parsed source metadata. | Date parser accepts ISO, `DD.MM.YYYY`, `DD/MM/YYYY`, and ISO datetime strings; invalid/missing stays null. | `fondant\oekb\parser.py`, `tests\test_ingestion.py` |
| `waehrung` | Report or fund currency. | `SOURCERPT.SRCCCY` | Copied to source metadata. | Falls back list -> detail -> detail payload; nullable. | `fondant\oekb\models.py`, `fondant\oekb\parser.py` |
| `isinBez` | ISIN/fund description from source metadata. | `SOURCERPT.SRCISB` | Copied to source metadata. | Falls back to detail payload `isinBez`; nullable. | `fondant\oekb\models.py`, `fondant\oekb\parser.py` |
| `gueltigVon` | Valid-from date. | `SOURCERPT.SRCGVN` | Copied to source metadata. | Falls back to payload or extra field; invalid/missing stays null. | `fondant\oekb\parser.py` |
| `gueltigBis` | Valid-to date. | `SOURCERPT.SRCGBS` | Copied to source metadata. | Falls back to payload or extra field; invalid/missing stays null. | `fondant\oekb\parser.py` |
| Extra `gjBeginn` | Business-year begin date. | `SOURCERPT.SRCBUSYEABEG` | Copied from list extra fields. | Invalid/missing stays null. | `fondant\oekb\parser.py`, `tests\test_ingestion.py` |
| Extra `gjEnde` | Business-year end date. | `SOURCERPT.SRCBUSYEAEND` | Copied from list extra fields. | Invalid/missing stays null. | `fondant\oekb\parser.py`, `tests\test_ingestion.py` |
| Extra `eintragezeit` | Source entry timestamp. | `SOURCERPT.SRCENTDTS` | Copied from list extra fields. | Invalid/missing stays null; can supply year fallback. | `fondant\oekb\parser.py` |
| Extra `zufluss` | Cash-flow/distribution date. | `SOURCERPT.SRCZFL` | Copied from list extra fields. | Invalid/missing stays null. Later `SECDIV` is written only when this is present and `ausschuettungsmeldung` is true. | `fondant\oekb\parser.py`, `fondant\ingestion\pipeline.py` |
| Extra `jahresmeldung` / `jahresMeldung` / `meldgJahresM` / `jahresdatenmeldung` | Annual-report flag. | `SOURCERPT.SRCJMS` | Parsed source metadata boolean. | Accepts bool, `1`/`0`, and German/English yes/no strings; otherwise null. | `fondant\oekb\parser.py`, `tests\test_ingestion.py` |
| Extra `ausschuettungsmeldung` / `ausschuettungsMeldung` / `meldgAusschuettungsM` | Distribution-report flag. | `SOURCERPT.SRCAMS` | Parsed source metadata boolean. | Same boolean parser; null when absent/unrecognized. | `fondant\oekb\parser.py`, `fondant\ingestion\pipeline.py` |
| Extra `selbstnachweis` / `selbstNachweis` | Self-assessment flag. | `SOURCERPT.SRCSNW` | Parsed source metadata boolean. | Same boolean parser; null when absent/unrecognized. | `fondant\oekb\parser.py` |
| Extra `korrigierteMeldeId` / `korrigierteMeldgId` / `korrigierteStmId` / `korrMeldeId` | Corrected prior OeKB report ID. | `SOURCERPT.SRCKIDN` | Parsed source metadata integer. | Missing or non-integer stays null. Later `TAXCOR` is written only if the referenced old report exists. | `fondant\oekb\parser.py`, `fondant\ingestion\pipeline.py` |

## `SOURCERAW` Detail Payload Capture

`OeKBClient.get_report_detail(stm_id)` calls `/steuerMeldung/stmId/{stm_id}/ertrStBeh`. The client stores the full JSON object in `OeKBReportDetailResponse.payload`; if the response is not a JSON object, it wraps it as `{"data": payload}` before model validation.

| OeKB source field/path | Meaning evident from code/docs | Destination | Handling | Null/missing behavior | Authoritative reference |
|---|---|---|---|---|---|
| Report list `stmId` | OeKB report ID used to request detail. | `SOURCERAW.SRCOKBIDN` | Copied from report-list item, not from payload. | Required by list item; detail is not fetched without it. | `fondant\ingestion\pipeline.py`, `fondant\oekb\parser.py` |
| Report list `versionsNr`, detail `versionsNr` | OeKB version number used for source update decisions. | `SOURCERAW.SRCVRN` | Copied from list, falling back to detail and then `1`. | Missing version becomes `1`. Older versions are skipped; same-version changed payloads are persisted. | `fondant\ingestion\pipeline.py`, `fondant\oekb\parser.py` |
| Full detail payload | Authoritative raw OeKB detail JSON for audit and reprocessing. | `SOURCERAW.SRCPAY` | Stored raw-only for `FIN` reports that pass persistence checks. | Non-null in table. No detail payload is fetched or stored for non-`FIN` reports. | `fondant\ingestion\pipeline.py`, `fondant\db\models\tax.py` |
| Payload top-level `stmId`, `statusCode`, `versionsNr`, `waehrung` | Detail metadata surfaced by client model when present. | Raw in `SOURCERAW.SRCPAY`; some fields may fallback into `SOURCERPT`. | Raw archive plus limited metadata fallback. | If absent, client model may use request `stm_id`; payload itself remains as received. | `fondant\oekb\client.py`, `fondant\oekb\models.py`, `fondant\oekb\parser.py` |
| Stored payload `list` | Observed local raw-detail container for report-detail rows. | Raw in `SOURCERAW.SRCPAY`; parsed recursively for `SOURCEAGE`. | Raw-only container plus parser traversal. | Parser does not require this exact name; it recursively walks all lists/dicts. | Read-only local `SOURCERAW` scan, `fondant\oekb\parser.py` |
| Stored payload `list[].expandedRows[]` | Observed nested row container for tax detail rows. | Raw in `SOURCERAW.SRCPAY`; parsed recursively for `SOURCEAGE`. | Raw-only container plus parser traversal. | Parser does not require this exact path; known `steuerName` context propagates into nested rows. | Read-only local `SOURCERAW` scan, `fondant\oekb\parser.py` |
| Stored payload structural keys `rowKey`, `txtBez`, `satzArt`, `position`, `sortOrder`, `stbFieldId`, `steuerCode`, `headerKlappbar`, `filterNullValues`, `klappbarVonStbFieldIs` | OeKB row/display/control metadata. Business semantics are not defined in current code/docs. | Raw only in `SOURCERAW.SRCPAY`. | Ignored by parser except for recursive traversal into nested structures. | Preserved in raw payload when present; not copied to source/curated columns. | Read-only local `SOURCERAW` scan, `fondant\oekb\parser.py` |

## `SOURCEAGE` Parsed Tax Matrix

The parser recursively walks the full detail payload. A row becomes relevant when it contains `steuerName` that appears in `fondant\tax_registry.py`. Known category keys are parsed to `Decimal` and written to `SOURCEAGE`; missing, blank, or null values remain null. Numeric strings may use comma or dot decimal separators. Unknown tax names, unknown scalar category fields, and invalid numeric values are reported as parser diagnostics and are not written as source matrix values.

Destination column pattern: `SOURCEAGE.SRC{line_code}{category_code}`. Example: `StB_E1KV_AGErtraege` plus `pvMitOption4` writes `SOURCEAGE.SRCK11PVM`.

| `steuerName` source value | Registry line | Meaning from registry/docs | `SOURCEAGE` metric prefix | Handling | Later transformation | Authoritative reference |
|---|---|---|---|---|---|---|
| `StB_Einkuenfte_steuerpflichtig` | `K40` | Taxable income. | `SRCK40*` | Parsed when a known category key has a valid numeric value. | `TAXDAT` rows; selected reporting views expose `K40`. | `fondant\tax_registry.py`, `Documentation\DATA_AND_QUERY_GUIDE.md` |
| `StB_E1KV_AGErtraege` | `K11` | Distributed income / AGErtraege. | `SRCK11*` | Parsed when present; observed in tests and some stored raw rows. | `TAXDAT` rows. | `fondant\tax_registry.py`, `tests\test_ingestion.py` |
| `StB_E1KV_Korrekturbetrag_saldiert` | `K12` | Net correction amount. | `SRCK12*` | Parsed when present. | `TAXDAT` rows. | `fondant\tax_registry.py` |
| `StB_KESt` | `K81` | Total withholding tax. | `SRCK81*` | Parsed when present. | `TAXDAT` rows. | `fondant\tax_registry.py`, `tests\test_ingestion.py` |
| `StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2` | `K82` | Withholding tax on substance gains. | `SRCK82*` | Parser-supported; not observed in the 38 sampled local raw rows. | `TAXDAT` rows. | `fondant\tax_registry.py`, read-only local `SOURCERAW` scan |
| `StB_Substanzgewinne_KEStpflichtig` | `K10` | Taxable substance gains. | `SRCK10*` | Parsed when present. | `TAXDAT` rows. | `fondant\tax_registry.py` |
| `StB_Fondsergebnis_nichtausgeschuettet` | `K55` | Undistributed fund result. | `SRCK55*` | Parsed when present. | `TAXDAT` rows. | `fondant\tax_registry.py` |
| `StB_Korrekturbetrag_AGErtrag_Anschaffungskosten` | `K61` | Cost-basis adjustment. | `SRCK61*` | Parsed when present. | `TAXDAT` rows and `TAXADJ` rows with `AKC`. | `fondant\tax_registry.py`, `fondant\ingestion\pipeline.py`, `tests\test_oekb_parser.py` |
| `StB_Korrekturbetrag_Ausschuettung_Anschaffungskosten` | `K62` | Distribution cost-basis adjustment. | `SRCK62*` | Parsed when present. | `TAXDAT` rows; selected reporting views expose `K62`. | `fondant\tax_registry.py`, `tests\test_oekb_parser.py` |
| `StB_Substanzgewinn_steuerpflichtig_beiAusschuettunginFolgejahren` | `K36` | Substance gain in following years. | `SRCK36*` | Parsed when present. | `TAXDAT` rows. | `fondant\tax_registry.py` |
| `StB_Abzugsteuern_einbehalten_Kapitaleinkuenfte` | `K21` | Retained withholding taxes. | `SRCK21*` | Parsed when present. | `TAXDAT` rows. | `fondant\tax_registry.py` |

## Parsed Category Source Names

| Source category key/alias | Category code | Category key | Meaning from registry/docs | Destination suffix | Null/missing behavior | Authoritative reference |
|---|---|---|---|---|---|---|
| `pvMitOption4`, `pv_mit`, `pvm` | `PVM` | `pv_mit` | Private assets with option. | `*PVM` | Missing/blank/null stays null; invalid numeric is diagnosed and omitted. | `fondant\tax_registry.py`, `fondant\oekb\parser.py` |
| `pvOhneOption4`, `pv_ohne` | `PVO` | `pv_ohne` | Private assets without option. | `*PVO` | Same parser behavior. | `fondant\tax_registry.py`, `tests\test_oekb_parser.py` |
| `bvMitOption4`, `bv_mit` | `BVM` | `bv_mit` | Business assets with option. | `*BVM` | Same parser behavior. | `fondant\tax_registry.py` |
| `bvOhneOption4`, `bv_ohne` | `BVO` | `bv_ohne` | Business assets without option. | `*BVO` | Same parser behavior. | `fondant\tax_registry.py` |
| `bvJurPerson4`, `bvjur`, `bv_jur` | `BVJ` | `bv_jur` | Business assets for legal entities. | `*BVJ` | Same parser behavior. | `fondant\tax_registry.py`, `tests\test_oekb_parser.py` |
| `stiftung4`, `stiftung` | `STF` | `stiftung` | Foundation. `STF` is the source/category code; `STI` is only a reporting-view compatibility alias. | `*STF` | Same parser behavior. | `fondant\tax_registry.py`, `Documentation\Validation\SE-004_FOUNDATION_CATEGORY_ALIAS_DECISION.md` |
| `anlegerKategorie` / `anlegerKat` / `kategorie` plus `betrag` / `wert` / `value` | Depends on category text mapped by aliases above. | Depends on mapped category. | Alternate row shape where category and amount are separate fields. | Matching category suffix. | Unknown category is diagnosed; invalid amount is diagnosed; missing amount stays null. | `fondant\oekb\parser.py` |

## Ignored Or Diagnostic Detail Fields

| Source field family | Destination | Behavior | Null/zero semantics | Authoritative reference |
|---|---|---|---|---|
| Unknown `steuerName` values | Raw only in `SOURCERAW.SRCPAY` | Not written to `SOURCEAGE`; parser emits `unknown_tax_field`. | The raw value is preserved, but curated/source matrix output has no value. | `fondant\oekb\parser.py`, `tests\test_oekb_parser.py` |
| Unknown scalar keys under a known tax field | Raw only in `SOURCERAW.SRCPAY` | If not a known structural key or known category alias, parser emits `unknown_category`. | The raw value is preserved, but no source matrix column is populated. | `fondant\oekb\parser.py`, `tests\test_oekb_parser.py` |
| Malformed numeric category values | Raw only in `SOURCERAW.SRCPAY` | Parser emits `invalid_numeric_value`; `SOURCEAGE` value remains null. | Null means absent/unparsed; explicit zero parses to `Decimal("0")` and is retained. | `fondant\oekb\parser.py`, `tests\test_oekb_parser.py` |
| Structural keys `steuerName`, `anlegerKategorie`, `anlegerKat`, `kategorie`, `betrag`, `wert`, `value` | Parser control fields, raw archive | Used to identify tax field/category/value shape; not directly stored as separate source columns. | Missing structural keys can prevent parsing but remain preserved in raw payload. | `fondant\oekb\parser.py` |

## Open Questions

- Current code/docs identify the registry meanings for each `StB_*` source tax name, but they do not establish deeper tax advice semantics. Treat the registry labels as field labels, not tax interpretation.
- Stored `SOURCERAW` rows include display/control fields such as `stbFieldId`, `steuerCode`, `headerKlappbar`, and `filterNullValues`; their business meanings are not defined in the pipeline and remain raw-only.
- `StB_KeSt_Substanzgewinne_sonstige_steuerpflichtig_2` is supported by the parser registry but was not present in the sampled local raw rows. Keep it in the catalog because code and tests validate the registry, not because the current local database proves current occurrence.

## Inventory Limits

- This catalog documents consumed OeKB fields only. ECB FX, static dictionaries, final curated lineage, reporting views, and operational logs belong to later FD tickets.
- No live OeKB calls were made.
- No code, schema, tests, data, credentials, Docker configuration, or unrelated validation artifacts were changed.
