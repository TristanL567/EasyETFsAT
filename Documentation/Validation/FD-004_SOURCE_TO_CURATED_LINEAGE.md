# FD-004 Source-To-Curated Lineage

Ticket: FD-004  
Epic: Field Lineage Documentation  
Scope: source-shaped OeKB values becoming curated tax/report rows. This is documentation only.

## Evidence Used

- `Documentation\Validation\FD-001_DATABASE_FIELD_INVENTORY.md` for object and column inventory.
- `Documentation\Validation\FD-002_OEKB_SOURCE_FIELD_CATALOG.md` for OeKB report-list, raw detail, and `SOURCEAGE` source-field handling.
- `Documentation\Validation\FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md` for `TAXLIN`, `TAXCAT`, and registry-backed dictionary behavior.
- `Documentation\DATA_AND_QUERY_GUIDE.md`, `Documentation\TECHNICAL_ARCHITECTURE.md`, and `Documentation\MAINTAINER_LAYER_GUIDE.md` for layer boundaries and existing query guidance.
- `fondant\tax_registry.py`, `fondant\oekb\parser.py`, `fondant\ingestion\pipeline.py`, `fondant\db\models\tax.py`, and `fondant\db\models\sec.py`.
- `tests\test_tax_registry.py`, `tests\test_ingestion.py`, `tests\test_oekb_parser.py`, and view migration SQL where final-view behavior helps distinguish curated facts from projections.
- Read-only local PostgreSQL queries against the Docker PostgreSQL service for the `K61BVJ` / `K62BVJ` case study. No writes, live OeKB calls, live ECB calls, schema changes, or data changes were made.

## Trust Boundary

The source-to-curated boundary is the transition from source-faithful storage to normalized tax/report records:

1. `SOURCERPT` stores OeKB report-list metadata.
2. `SOURCERAW` stores full `FIN` report-detail payloads for audit.
3. `SOURCEAGE` stores parsed OeKB tax matrix values in wide source-shaped columns.
4. `_curate_report(...)` in `fondant\ingestion\pipeline.py` copies and narrows those source-shaped records into `TAXRPT`, `TAXDAT`, `TAXADJ`, `SECDIV`, and `TAXCOR`.

The curated layer does not reinterpret tax law. It gives source values stable report headers, dictionary joins, narrow facts, adjustment shortcuts, distribution events, and correction links. Broader source ingestion behavior is documented in FD-002; reference and dictionary behavior is documented in FD-003.

## Object Flow Summary

| Curated object | Source input | Transformation | Null and zero behavior |
|---|---|---|---|
| `SECMDA` | ISIN plus report-list name/currency candidates | `_upsert_security_master(...)` keeps one security row per ISIN. Name defaults to existing name or the ISIN when no source name exists; currency preserves existing value when incoming value is null. | Missing optional metadata stays null or preserves an existing value; no tax amount semantics. |
| `TAXRPT` | `SOURCERPT` | `_curate_report(...)` upserts one report header per `TAXISN + TAXOKBIDN`, copying report metadata with `SRC*` to `TAX*` field mapping. | Nullable source metadata remains nullable in `TAXRPT`; upsert preserves existing values when incoming nullable fields are null. |
| `TAXDAT` | `SOURCEAGE` plus `TAXLIN`/`TAXCAT` dictionaries | Each non-null `SOURCEAGE` metric/category amount becomes one narrow fact by report, tax line, and category. | A missing source amount is skipped and produces no `TAXDAT` row. An explicit zero is not null, so it is retained as `TAXDAT.TAXAMT = 0`. |
| `TAXADJ` | `SOURCEAGE` K61 values | Every curated `K61` value also creates or updates a `TAXADJ` row with `TAXCOD = 'AKC'`. | Same as `TAXDAT`: absent K61 facts create no adjustment row; explicit zero K61 values are retained. |
| `SECDIV` | `SOURCERPT.SRCAMS`, `SRCZFL`, and report metadata | A distribution event is upserted only when `ausschuettungsmeldung` is true and `zufluss` is present. | Missing distribution flag/date means no `SECDIV` row. `SECFLWAMT` is currently written as null because the curation path does not derive a cash amount. |
| `TAXCOR` | `SOURCERPT.SRCKIDN` plus existing `TAXRPT` rows | If a corrected OeKB report ID points to an existing old `TAXRPT` for the same ISIN, a correction link is written with `TAXRSN = 'KID'`. | Missing correction ID, missing old report, or self-link creates no `TAXCOR` row. |

## `SOURCERPT` To `TAXRPT`

`build_sourcerpt_values(...)` assembles report metadata from the OeKB report-list item and limited detail payload fallbacks. `_curate_report(...)` then copies the source row into `TAXRPT`.

| Source field | Curated field | Meaning in current docs/code |
|---|---|---|
| `SRCISN` | `TAXISN` | ISIN / security identifier. |
| `SRCOKBIDN` | `TAXOKBIDN` | OeKB report ID. |
| `SRCVRN` | `TAXVRN` | OeKB version number. |
| `SRCSTS` | `TAXSTS` | OeKB report status. |
| `SRCYEA` | `TAXYEA` | Report or tax year when available or derived. |
| `SRCMDT` | `TAXMDT` | Report/message date. |
| `SRCCCY` | `TAXCCY` | Report or fund currency. |
| `SRCISB` | `TAXISB` | ISIN/fund description from source metadata. |
| `SRCGVN`, `SRCGBS` | `TAXGVN`, `TAXGBS` | Valid-from / valid-to dates. |
| `SRCBUSYEABEG`, `SRCBUSYEAEND` | `TAXBUSYEABEG`, `TAXBUSYEAEND` | Business-year date range. |
| `SRCZFL` | `TAXZFL` | Cash-flow/distribution date. |
| `SRCJMS`, `SRCAMS`, `SRCSNW` | `TAXJMS`, `TAXAMS`, `TAXSNW` | Annual-report, distribution-report, and self-assessment flags. |
| `SRCKIDN` | `TAXKIDN` | Corrected prior OeKB report ID. |

`TAXRPT` is therefore a curated report header, not a new interpretation of the OeKB payload. It keeps the report identity and metadata close to `SOURCERPT` while giving downstream tables a stable foreign-key parent.

## Registry-Driven Tax Meaning

`fondant\tax_registry.py` is the shared registry for parser names, source columns, dictionaries, and selected view aliases:

| Registry element | Used by | Lineage role |
|---|---|---|
| `TaxLine.source_tax_names` | Parser `TAX_FIELD_MAP` | Maps OeKB `steuerName` values such as `StB_Korrekturbetrag_AGErtrag_Anschaffungskosten` to metric keys such as `korrekturbetrag_age_ak`. |
| `TaxLine.line_code` | `SOURCEAGE` column names, `TAXLIN.TAXCOD`, view pivots | Produces K-codes such as `K61` and `K62`. |
| `TaxLine.metric_key` | Parser output model and ORM attributes | Forms the source attribute part used by `_curate_report`, such as `korrekturbetrag_age_ak_bv_jur`. |
| `TaxCategory.parser_aliases` | Parser `CATEGORY_KEY_MAP` | Maps OeKB category keys such as `bvJurPerson4` to category keys such as `bv_jur`. |
| `TaxCategory.source_alias` | `SOURCEAGE` columns | Produces source suffixes such as `BVJ` and `STF`. |
| `TaxCategory.category_code` / `category_key` | `TAXCAT` | Gives curated facts a stable category dictionary join. |
| `LINE_DICTIONARY` and `CATEGORY_DICTIONARY` | `_ensure_tax_dictionaries(...)` | Upserts `TAXLIN` and `TAXCAT` rows before ingestion curates `TAXDAT`. |

Example: OeKB `steuerName = StB_Korrekturbetrag_AGErtrag_Anschaffungskosten` plus category key `bvJurPerson4` maps to metric key `korrekturbetrag_age_ak`, line code `K61`, category key `bv_jur`, and category code `BVJ`. The parser writes `SOURCEAGE.SRCK61BVJ`; `_curate_report(...)` looks up `TAXLIN.TAXCOD = 'K61'` and `TAXCAT.TAXCOD = 'BVJ'`; if the source value is non-null, it writes `TAXDAT.TAXAMT` and a matching `TAXADJ` row with `TAXCOD = 'AKC'`.

## `SOURCEAGE` To `TAXDAT`

`SOURCEAGE` is wide because it mirrors known OeKB tax lines and investor categories: `SRC{line_code}{category_code}`. `_curate_report(...)` loops over `METRIC_CODE_BY_KEY` and `CATEGORY_CODE_BY_KEY`, builds the matching ORM attribute name, and reads the amount from the `SOURCEAGE` row.

For each metric/category combination:

1. If the registry line or category dictionary row is missing, the fact is skipped.
2. If the `SOURCEAGE` value is `None`, the fact is skipped.
3. If the `SOURCEAGE` value is any `Decimal`, including `Decimal("0")`, a `TAXDAT` row is upserted with the report currency from `TAXRPT.TAXCCY`.

This is the most important null/zero boundary. `TAXDAT.TAXAMT` is non-null by schema, so an absent source value is represented by no row, not by a null amount row. Explicit zero is a real source amount and is retained.

## `K61` To `TAXADJ`

`K61` values follow the normal `TAXDAT` path and also create `TAXADJ` rows:

| Source value | Curated fact | Adjustment shortcut |
|---|---|---|
| `SOURCEAGE.SRCK61PVM` | `TAXDAT` with `TAXLIN.K61`, `TAXCAT.PVM` | `TAXADJ` with same report/category, `TAXCOD = 'AKC'`. |
| `SOURCEAGE.SRCK61PVO` | `TAXDAT` with `TAXLIN.K61`, `TAXCAT.PVO` | `AKC`. |
| `SOURCEAGE.SRCK61BVM` | `TAXDAT` with `TAXLIN.K61`, `TAXCAT.BVM` | `AKC`. |
| `SOURCEAGE.SRCK61BVO` | `TAXDAT` with `TAXLIN.K61`, `TAXCAT.BVO` | `AKC`. |
| `SOURCEAGE.SRCK61BVJ` | `TAXDAT` with `TAXLIN.K61`, `TAXCAT.BVJ` | `AKC`. |
| `SOURCEAGE.SRCK61STF` | `TAXDAT` with `TAXLIN.K61`, `TAXCAT.STF` | `AKC`; final views may display this category with the `STI` alias. |

The current docs/code label `K61` as a cost-basis adjustment and `AKC` as the adjustment code used by this shortcut. Deeper tax meaning is an open business question unless a separate tax decision record defines it.

## Distributions And Corrections

`SECDIV` is event-shaped, not amount-derived from `SOURCEAGE`. It is created only when source metadata says the report is a distribution report and supplies a `zufluss` date. The curation path writes `SECFLWTYP = 'DIST'`, `SECFLWDAT = SRCZFL`, source report ID, report year, status, and currency; `SECFLWAMT` remains null.

`TAXCOR` is a link table. `SRCKIDN` / `TAXKIDN` stores the corrected OeKB report ID. A `TAXCOR` row is written only after the old report is already present as a `TAXRPT` row for the same ISIN. The reason code is currently `KID`.

## Null Versus Explicit Zero

| Stage | Missing, blank, null, unknown, or invalid source value | Explicit zero source value |
|---|---|---|
| Parser | Missing/null/blank does not set a `ParsedTaxAge` value. Invalid numeric values produce diagnostics and are not written to `SOURCEAGE`. | Parsed by `_to_decimal(...)` as `Decimal("0")`. |
| `SOURCEAGE` | Column stays null. | Column contains numeric zero. |
| `_curate_report(...)` | `if value is None: continue`, so no `TAXDAT` and no K61 `TAXADJ` row. | Zero is not `None`, so `TAXDAT.TAXAMT = 0` is written; K61 zero also writes `TAXADJ.TAXAMT = 0`. |
| `V1_TAXDATPRE` / `V2_TAXDATEUR` | Left joins and `MAX(CASE ...)` can show null because no matching curated fact row exists. | Zero remains zero unless a later conversion rule changes display; in `V2_TAXDATEUR`, missing or zero FX rates can also produce null EUR-converted output for non-EUR rows. |

For downstream reporting, null in a final view can mean no curated fact exists for that line/category. In `V2_TAXDATEUR`, it can also mean the tax fact existed but EUR conversion could not be performed. FD-003 documents that FX distinction; FD-005 owns final reporting/API lineage.

## `K61BVJ` / `K62BVJ` Case Study

Across all local reports, the database does not show `K61BVJ` or `K62BVJ` as
globally absent fields. A SELECT-only aggregate check showed:

| Layer | Rows checked | `K61BVJ` present | `K62BVJ` present | `K61BVJ` explicit zero | `K62BVJ` explicit zero |
|---|---:|---:|---:|---:|---:|
| `SOURCEAGE` | 38 | 6 | 6 | 0 | 5 |
| `TAXDAT` | 1,774 | 6 | 6 | 0 | 5 |
| `V1_TAXDATPRE` | 38 | 6 | 6 | 0 | 5 |
| `V2_TAXDATEUR` | 38 | 6 | 6 | 0 | 5 |

This confirms two useful lineage points:

- When `SOURCEAGE.SRCK61BVJ` or `SOURCEAGE.SRCK62BVJ` is present, the curated and view layers retain it.
- The five `K62BVJ` zero values are explicit zero amounts, not absent facts. They are present in `SOURCEAGE`, `TAXDAT`, `V1_TAXDATPRE`, and `V2_TAXDATEUR`.

For the specific report reviewed during this work, `LU0380865021` /
`611854`, the absence does start before the reporting views:

| Check | Result |
|---|---|
| `SOURCEAGE.SRCK61BVJ` | null |
| `SOURCEAGE.SRCK62BVJ` | null |
| `TAXDAT` rows for `K61 / BVJ` | none |
| `TAXDAT` rows for `K62 / BVJ` | none |

So for that report, `K61BVJ` and `K62BVJ` are not FX or view-pivot issues.
They are absent source/curated facts. That differs from explicit zero values
seen for `K62BVJ` in other reports.

If a maintainer sees a null `K61BVJ` or `K62BVJ` in a final view for another report, the first check should be whether the corresponding `SOURCEAGE.SRCK61BVJ` or `SOURCEAGE.SRCK62BVJ` column is null. If it is null, the absence predates the reporting view. If `SOURCEAGE` has a non-null value but the final view is null, inspect `TAXDAT` joins and, for `V2_TAXDATEUR`, FX conversion behavior.

## Trace Recipe For One `TAXDAT` Value

To trace a selected curated amount:

1. Start with `TAXDAT.TAXRPTIDN`, `TAXLINIDN`, and `TAXCATIDN`.
2. Join `TAXRPT` to get `TAXISN`, `TAXOKBIDN`, `TAXYEA`, `TAXCCY`, and source report metadata.
3. Join `TAXLIN` for the K-code and metric key, and `TAXCAT` for the investor category code/key.
4. Translate the pair to the source column: `SOURCEAGE.SRC{TAXLIN.TAXCOD}{TAXCAT.TAXCOD}`.
5. Read `SOURCEAGE` by `SRCISN = TAXRPT.TAXISN` and `SRCOKBIDN = TAXRPT.TAXOKBIDN`.
6. If the source column is null, there should be no `TAXDAT` row for that line/category. If it is zero, the `TAXDAT` row should contain zero.
7. Use `SOURCERAW.SRCPAY` only when auditing the original detail payload or parser diagnostics; FD-002 documents raw source handling.

## Open Questions

- The current registry provides line/category labels, but it does not define deep tax advice semantics. Do not infer business meaning beyond the registry names and existing documentation.
- `TAXADJ.TAXCOD = 'AKC'` is hard-coded for `K61`; a business owner should confirm whether future adjustment codes or non-K61 adjustment lines are expected.
- `SECDIV.SECFLWAMT` is currently null because no cash amount is derived in `_curate_report(...)`; confirm whether future distribution amount curation should use OeKB source fields.
- `TAXCOR.TAXRSN = 'KID'` identifies correction-ID lineage in code, but fuller correction reason semantics are not documented.

## Scope Notes

- No implementation changes were made.
- No database schema, data, credentials, Docker configuration, tests, live OeKB calls, or live ECB calls were changed.
- Final reporting/API output lineage belongs to FD-005.
