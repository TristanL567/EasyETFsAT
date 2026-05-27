# FD-005 Reporting And API Field Lineage

Ticket: FD-005  
Epic: Field Lineage Documentation  
Scope: final reporting views and public API output fields only. This is documentation only.

## Evidence Used

- `Documentation\Validation\FD-001_DATABASE_FIELD_INVENTORY.md` for the database object and final-view field inventory.
- `Documentation\Validation\FD-002_OEKB_SOURCE_FIELD_CATALOG.md` for OeKB source metadata and parsed source matrix behavior.
- `Documentation\Validation\FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md` for `TAXLIN`, `TAXCAT`, `REFEXC`, and current FX behavior.
- `Documentation\Validation\FD-004_SOURCE_TO_CURATED_LINEAGE.md` for `SOURCEAGE` to `TAXRPT` / `TAXDAT` lineage and null-versus-zero behavior.
- `Documentation\Validation\SE-004_FOUNDATION_CATEGORY_ALIAS_DECISION.md` and `Documentation\Validation\SE-003_TAX_CODE_REGISTRY.md` for `STF` / `stiftung` / `STI` compatibility.
- `Documentation\Validation\SE-007_PUBLIC_API_CONTRACT.md` and `tests\test_api_etf.py` for public API response shape, ordering, null behavior, and numeric serialization.
- `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py`, `tests\test_tax_views_postgres.py`, `fondant\api\routes\health.py`, and `fondant\api\routes\etf.py` for code-level behavior.

This report links to prior architecture and source-to-curated explanations rather than restating them. FD-004 is the source for how source-shaped facts become curated rows; this document starts at the curated/reporting consumer surfaces.

## Consumer Surfaces

| Surface | Consumer shape | Source at read time | Why it differs |
|---|---|---|---|
| `V1_TAXDATPRE` | SQL view with one row per report and selected K-code/category columns in fund currency. | `TAXRPT`, `TAXDAT`, `TAXLIN`, `TAXCAT`. | Spreadsheet/BI convenience view; it pivots only selected values. |
| `V2_TAXDATEUR` | SQL view with the same selected values converted to EUR plus `TAXMDT` and `FXRAT`. | `V1_TAXDATPRE`, `TAXRPT`, `REFEXC`. | EUR reporting convenience view; it adds exact-date FX conversion semantics. |
| `GET /health` | Fixed JSON status object. | Route function constant. | Operational API liveness surface; not sourced from tax tables. |
| `GET /etf/{isin}/tax?year={year}` | Nested JSON reports and tax facts. | `TAXRPT`, `TAXDAT`, `TAXLIN`, `TAXCAT`. | Public API surface for curated facts; it is not the reporting view and does not do EUR conversion at request time. |

The API does not call OeKB, ECB, ingestion jobs, or `V2_TAXDATEUR` at request time. It reads already curated database rows directly.

## `V1_TAXDATPRE` Lineage

`V1_TAXDATPRE` is defined as a pivot over curated tax facts. It starts from `TAXRPT` and left joins:

- `TAXDAT` on `TAXDAT.TAXRPTIDN = TAXRPT.TAXIDN`.
- `TAXLIN` on `TAXLIN.TAXIDN = TAXDAT.TAXLINIDN`.
- `TAXCAT` on `TAXCAT.TAXIDN = TAXDAT.TAXCATIDN`.

The view groups by `TAXRPT.TAXISN`, `TAXRPT.TAXOKBIDN`, `TAXRPT.TAXYEA`, and `TAXRPT.TAXCCY`. It pivots values with `MAX(CASE WHEN TAXLIN.TAXCOD = <line> AND TAXCAT.TAXKEY = <category> THEN TAXDAT.TAXAMT END)`.

`V1_TAXDATPRE` is fund-currency output. Its amount columns are `TAXDAT.TAXAMT` values in the report/fund currency exposed as `FNDCCY` from `TAXRPT.TAXCCY`; no FX conversion occurs in `V1_TAXDATPRE`.

| `V1_TAXDATPRE` field | Source or derivation path | Null / zero semantics |
|---|---|---|
| `TAXISN` | `TAXRPT.TAXISN`, copied from `SOURCERPT.SRCISN` per FD-004. | Report identity field; not a tax amount. |
| `TAXOKBIDN` | `TAXRPT.TAXOKBIDN`, copied from `SOURCERPT.SRCOKBIDN`. | Report identity field; not a tax amount. |
| `TAXYEA` | `TAXRPT.TAXYEA`, copied or derived from source report metadata. | Nullable source metadata remains nullable. |
| `FNDCCY` | `TAXRPT.TAXCCY`, copied from `SOURCERPT.SRCCCY`. | Nullable if source/report currency is absent. |
| `K61PVM`, `K61PVO`, `K61BVM`, `K61BVO`, `K61BVJ`, `K61STI` | `TAXDAT.TAXAMT` where `TAXLIN.TAXCOD = 'K61'` and `TAXCAT.TAXKEY` is respectively `pv_mit`, `pv_ohne`, `bv_mit`, `bv_ohne`, `bv_jur`, `stiftung`. These originate from `SOURCEAGE.SRCK61PVM` through `SOURCEAGE.SRCK61STF` via FD-004 curation. | Null means no matching curated fact survived the joins/pivot. Explicit zero remains zero. |
| `K62PVM`, `K62PVO`, `K62BVM`, `K62BVO`, `K62BVJ`, `K62STI` | Same pivot pattern for `TAXLIN.TAXCOD = 'K62'`; source columns are `SOURCEAGE.SRCK62PVM` through `SOURCEAGE.SRCK62STF`. | Null means no matching curated fact survived the joins/pivot. Explicit zero remains zero. |
| `K40PVM`, `K40PVO`, `K40BVM`, `K40BVO`, `K40BVJ`, `K40STI` | Same pivot pattern for `TAXLIN.TAXCOD = 'K40'`; source columns are `SOURCEAGE.SRCK40PVM` through `SOURCEAGE.SRCK40STF`. | Null means no matching curated fact survived the joins/pivot. Explicit zero remains zero. |

The pivot uses `TAXCAT.TAXKEY = 'stiftung'` for foundation values but emits `*STI` column names for compatibility.

## `STF` To `STI` Alias Behavior

`STF`, `stiftung`, and `STI` identify the same foundation category at different layers:

| Name | Layer | Behavior |
|---|---|---|
| `STF` | Source/category code | Canonical category code in source-shaped columns such as `SOURCEAGE.SRCK61STF` and in `TAXCAT.TAXCOD`. |
| `stiftung` | Semantic category key | Registry and curated dictionary key in `TAXCAT.TAXKEY`; API `tax_fields` uses this key. |
| `STI` | Reporting-view compatibility alias | Output suffix used by `V1_TAXDATPRE` and `V2_TAXDATEUR` columns such as `K61STI`, `K62STI`, and `K40STI`. |

The `STI` suffix is intentional compatibility behavior, not a separate category. Future renaming belongs in a migration/compatibility ticket, not in lineage documentation.

## `V2_TAXDATEUR` Lineage

`V2_TAXDATEUR` inherits all selected tax values from `V1_TAXDATPRE`, joins `TAXRPT` to add report date, and left joins `REFEXC` for exact-date FX:

- `TAXRPT` join: `TAXRPT.TAXISN = V1_TAXDATPRE.TAXISN` and `TAXRPT.TAXOKBIDN = V1_TAXDATPRE.TAXOKBIDN`.
- `REFEXC` join: `REFEXC.REFCCY = V1_TAXDATPRE.FNDCCY` and `REFEXC.REFDAT = TAXRPT.TAXMDT`.
- EUR rule: when `FNDCCY = 'EUR'`, `FXRAT = 1`.
- Non-EUR rule: when an exact `REFEXC` match exists, `FXRAT = REFEXC.REFRAT`.
- Conversion rule: each selected amount is divided by `FXRAT`.
- Failure rule: if `FXRAT IS NULL OR FXRAT = 0`, converted amount output is null.

| `V2_TAXDATEUR` field | Source or derivation path | Null / zero semantics |
|---|---|---|
| `TAXISN` | Inherited from `V1_TAXDATPRE.TAXISN`. | Same identity semantics as `V1_TAXDATPRE`. |
| `TAXOKBIDN` | Inherited from `V1_TAXDATPRE.TAXOKBIDN`. | Same identity semantics as `V1_TAXDATPRE`. |
| `TAXYEA` | Inherited from `V1_TAXDATPRE.TAXYEA`. | Nullable if the source report year is absent. |
| `FNDCCY` | Inherited from `V1_TAXDATPRE.FNDCCY`. | Nullable if report currency is absent; null prevents a non-EUR FX match. |
| `TAXMDT` | `TAXRPT.TAXMDT` after joining report identity back to `TAXRPT`. | Nullable if source report date is absent; null prevents exact-date `REFEXC` matching. |
| `FXRAT` | `1` for EUR; otherwise `REFEXC.REFRAT` where `REFCCY = FNDCCY` and `REFDAT = TAXMDT`. | Null means no exact-date FX row for non-EUR or missing join inputs. Zero is retained in `FXRAT` but treated as unconvertible for amount fields. |
| `K61PVM`, `K61PVO`, `K61BVM`, `K61BVO`, `K61BVJ`, `K61STI` | `V1_TAXDATPRE.K61* / FXRAT` when `FXRAT` is non-null and nonzero. | Null can mean absent K61 fact in `V1_TAXDATPRE` or failed FX conversion. Explicit zero amount remains zero when FX is valid. |
| `K62PVM`, `K62PVO`, `K62BVM`, `K62BVO`, `K62BVJ`, `K62STI` | `V1_TAXDATPRE.K62* / FXRAT` when `FXRAT` is non-null and nonzero. | Null can mean absent K62 fact in `V1_TAXDATPRE` or failed FX conversion. Explicit zero amount remains zero when FX is valid. |
| `K40PVM`, `K40PVO`, `K40BVM`, `K40BVO`, `K40BVJ`, `K40STI` | `V1_TAXDATPRE.K40* / FXRAT` when `FXRAT` is non-null and nonzero. | Null can mean absent K40 fact in `V1_TAXDATPRE` or failed FX conversion. Explicit zero amount remains zero when FX is valid. |

Important distinction: `V1_TAXDATPRE` null amount columns indicate no matching curated fact for that line/category/report. `V2_TAXDATEUR` null amount columns can also indicate an FX problem: missing exact-date `REFEXC`, zero `FXRAT`, missing `FNDCCY`, or missing `TAXMDT`. Current `V2_TAXDATEUR` does not expose a diagnostic column that separates absent facts from FX-conversion nulls.

## `GET /health` Output Lineage

| API field | Source or derivation path | Null / zero semantics |
|---|---|---|
| `status` | Constant string returned by `fondant\api\routes\health.py`: `{"status": "ok"}`. | Always `"ok"` on HTTP 200 under the current route contract. |

`GET /health` is an application liveness response. It does not read database tables, OeKB, ECB, ingestion state, or reporting views.

## `GET /etf/{isin}/tax?year={year}` Output Lineage

The route uppercases the path ISIN, selects `TAXRPT` rows for the requested year, and falls back to `TAXRPT.TAXYEA IS NULL` only when no exact-year rows exist. Matching reports are ordered by descending `TAXRPT.TAXMDT` through the ORM field `meldg_datum`. It then selects `TAXDAT` joined to `TAXLIN` and `TAXCAT` for those report IDs and nests amounts as `metric_key -> category_key -> amount`.

| API field | Source or derivation path | Null / zero semantics |
|---|---|---|
| `isin` | Uppercased path parameter. | Not database-sourced; unknown or invalid-looking values can still be looked up and return 404 if no rows match. |
| `year` | Required query parameter constrained by OpenAPI to `1900 <= year <= 3000`. | Not database-sourced. |
| `year_fallback_null_used` | `true` only when exact-year `TAXRPT` rows are absent and null-year rows are returned. | Boolean diagnostic for report-year fallback, not a tax fact. |
| `count` | Length of returned report list. | `0` is not returned on the current success path because no reports raises 404. |
| `reports[].stm_id` | `TAXRPT.TAXOKBIDN` via ORM `stm_id`. | Report identity; not a tax amount. |
| `reports[].versions_nr` | `TAXRPT.TAXVRN` via ORM `versions_nr`. | Report metadata. |
| `reports[].status_code` | `TAXRPT.TAXSTS` via ORM `status_code`. | Nullable report metadata is serialized as JSON `null`. |
| `reports[].waehrung` | `TAXRPT.TAXCCY` via ORM `waehrung`. | Nullable report currency is serialized as JSON `null`. No API FX conversion uses this field. |
| `reports[].meldg_datum` | `TAXRPT.TAXMDT` via ORM `meldg_datum`. | Nullable report date is serialized as JSON `null`; reports are ordered by this field descending, with no explicit secondary tie-order contract. |
| `reports[].tax_fields` | Object built from `TAXDAT` joined to `TAXLIN` and `TAXCAT` for the selected `TAXRPT` IDs. Outer keys are `TAXLIN.TAXKEY`; inner keys are `TAXCAT.TAXKEY`; values are `TAXDAT.TAXAMT` converted to Python `float`. | Missing tax points are omitted, not emitted as null. Reports with no matching `TAXDAT` rows have `{}`. Explicit zero becomes JSON number `0.0` or equivalent numeric zero. |
| `reports[].tax_fields.<metric_key>` | `TAXLIN.TAXKEY`, seeded from `fondant\tax_registry.py` through `TAXLIN`. | Key exists only when at least one selected fact uses the metric. |
| `reports[].tax_fields.<metric_key>.<category_key>` | `TAXCAT.TAXKEY`, seeded from `fondant\tax_registry.py` through `TAXCAT`. Foundation is `stiftung`, not `STI`, in this API shape. | Key exists only when a selected `TAXDAT` row exists for the category. |
| `reports[].tax_fields.<metric_key>.<category_key> amount` | `TAXDAT.TAXAMT`, originally curated from non-null `SOURCEAGE.SRC{line}{category}` values per FD-004. | Serialized as a JSON number by converting `Decimal` / `Numeric` to `float`; no decimal-safe string is currently emitted. |

`GET /etf/{isin}/tax` is broader than the reporting views: it can return any curated `TAXDAT` metric/category present for the selected reports, not only `K40`, `K61`, and `K62`. It is also narrower in another sense: it does not expose `V2_TAXDATEUR.FXRAT`, `TAXMDT`-based EUR conversion diagnostics, or reporting-view `STI` aliases.

## Null And Zero Summary

| Surface | Absent curated fact | Explicit zero curated fact | FX-conversion null |
|---|---|---|---|
| `V1_TAXDATPRE` | Amount column is null because no matching `TAXDAT`/dictionary join contributed to the pivot. | Amount column remains zero. | Not applicable; no FX conversion. |
| `V2_TAXDATEUR` | Amount column is null when inherited `V1_TAXDATPRE` amount is null. | Amount remains zero when `FXRAT` is valid; becomes null only if FX is null or zero. | Amount column is null when `FXRAT` is null or zero, even if a fund-currency fact exists. |
| `GET /etf/{isin}/tax` | Missing tax point is omitted from `tax_fields`; report can have an empty `{}`. | Amount is emitted as a JSON numeric zero. | Not applicable; endpoint does not perform FX conversion. |

## Open Questions

- Should a future reporting/API surface distinguish absent facts from missing/zero FX with explicit diagnostic fields? Current `V2_TAXDATEUR` only exposes `FXRAT`, so consumers must compare against `V1_TAXDATPRE` or curated facts to disambiguate.
- Should API tax amounts eventually serialize as decimal strings instead of floats? SE-007 records current float serialization and requires a future compatibility ticket for changes.
- Should API report ordering define secondary tie behavior for equal or null `meldg_datum` values? SE-007 documents that tie order is not currently contracted.
- Should a future public API expose EUR-converted values from `V2_TAXDATEUR`, or should it continue to expose curated fund-currency `TAXDAT` facts only? Current API behavior is direct curated-table reads.
- The registry labels identify field meanings, but deeper tax advice semantics remain outside current code/docs. Do not infer additional tax meaning from K-codes or category names.

## Scope Notes

- No implementation, schema, data, test, credential, Docker, live OeKB, or live ECB changes were made.
- This ticket created only `Documentation\Validation\FD-005_REPORTING_AND_API_FIELD_LINEAGE.md`.
