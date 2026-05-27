# FD-003 Reference And FX Field Catalog

Ticket: FD-003  
Epic: Field Lineage Documentation  
Scope: documentation only; no schema, data, code, test, credential, Docker, or network behavior changes.

## Evidence And Related Documentation

This catalog extends the FD-001 inventory for reference and FX objects only:

- `Documentation\Validation\FD-001_DATABASE_FIELD_INVENTORY.md` for the complete object/column inventory and layer classification.
- `Documentation\DATA_AND_QUERY_GUIDE.md` for table purpose, query examples, and current `V2_TAXDATEUR` consumer behavior.
- `Documentation\TECHNICAL_ARCHITECTURE.md` for ingestion flow and ECB FX pipeline placement.
- `Documentation\MAINTAINER_LAYER_GUIDE.md` for layer boundaries, including the current-versus-future FX decision.
- `Documentation\Validation\SE-003_TAX_CODE_REGISTRY.md` and `fondant\tax_registry.py` for tax line/category registry meaning.
- `Documentation\Validation\SE-004_FOUNDATION_CATEGORY_ALIAS_DECISION.md` for `STF` / `stiftung` / `STI` alias separation.
- `Documentation\Validation\SE-008_FX_CONVERSION_SEMANTICS_DECISION.md` for the approved future FX behavior. Current `V2_TAXDATEUR` behavior remains exact-date matching.
- `fondant\db\models\ref.py`, `fondant\db\models\tax.py`, `fondant\ingestion\pipeline.py`, `fondant\ingestion\fx_pipeline.py`, and Alembic view migrations for source-backed field behavior.

No local PostgreSQL queries or live ECB calls were run for this ticket.

## Scope Boundary

These objects are reference/support data, not source tax facts and not final reporting outputs:

| Object | Classification | Source class | Used by final outputs |
|---|---|---|---|
| `REFEXC` | ECB FX observation table | Downloaded ECB observations persisted by the FX pipeline. | `V2_TAXDATEUR` uses it to derive `FXRAT` and EUR-converted selected tax values. |
| `REFCCY` | Static currency dictionary | Static reference dictionary table; no active seed/upsert path was found in current ingestion code. | Can describe currency codes, but current `V2_TAXDATEUR` joins directly to `REFEXC`, not `REFCCY`. |
| `REFCTR` | Static country dictionary | Static reference dictionary table; no active seed/upsert path was found in current ingestion code. | Can describe country codes used by security master data; not used by `V2_TAXDATEUR`. |
| `TAXLIN` | Static tax line dictionary | Seeded/upserted from `fondant\tax_registry.py` during OeKB ingestion. | Joins `TAXDAT` to tax line code/key and drives selected view pivots. |
| `TAXCAT` | Static investor category dictionary | Seeded/upserted from `fondant\tax_registry.py` during OeKB ingestion. | Joins `TAXDAT` to investor category code/key and drives selected view pivots. |

## Field Catalog

| Object | Field | Meaning | Source and transformation path | Null/zero semantics | Final-output role |
|---|---|---|---|---|---|
| `REFEXC` | `REFIDN` | Surrogate key. | Database generated via `IdTimestampMixin` / migration. | Not a business value. | Not exposed by reporting views. |
| `REFEXC` | `REFCRTDTS`, `REFUPDDTS` | Row creation/update timestamps. | Database defaults and upsert update timestamp. | Operational metadata only. | Not exposed by reporting views. |
| `REFEXC` | `REFDAT` | ECB reference-rate observation date. | `ECBRatePoint.rate_date` from ECB CSV client, upserted by `fondant\ingestion\fx_pipeline.py`. | Required. Missing date/currency row means no exact-date FX match in current `V2_TAXDATEUR`. | Joined to `TAXRPT.TAXMDT` through exact-date matching. |
| `REFEXC` | `REFCCY` | Three-letter currency code for the ECB observation. | `ECBRatePoint.currency_code`, normalized to uppercase in FX ingestion input. | Required. Current default FX ingestion currencies are `USD`, `GBP`, and `CHF` unless overridden. | Joined to `V1_TAXDATPRE.FNDCCY`; no `REFCCY` dictionary join is used in the view. |
| `REFEXC` | `REFRAT` | ECB reference rate stored as `NUMERIC(20,10)`. | `ECBRatePoint.rate`, upserted on unique key `REFDAT + REFCCY`. | Required by schema. In `V2_TAXDATEUR`, zero behaves as unconvertible and produces null converted values. | Becomes `FXRAT` for non-EUR exact-date matches. |
| `REFCCY` | `REFIDN`, `REFCRTDTS`, `REFUPDDTS` | Surrogate key and timestamps. | Static dictionary table metadata. | Not business values. | Not exposed by reporting views. |
| `REFCCY` | `REFCOD` | Currency code. | Static dictionary field, unique. | Required by schema. Business source for rows is unclear in current code. | Descriptive support only; not used by current `V2_TAXDATEUR`. |
| `REFCCY` | `REFNAM` | Currency name. | Static dictionary field. | Required by schema. Business source for row values is unclear in current code. | Descriptive support only. |
| `REFCCY` | `REFMUN` | Minor currency units. | Static dictionary field. | Nullable. Meaning/source should be confirmed before relying on it. | Descriptive support only. |
| `REFCTR` | `REFIDN`, `REFCRTDTS`, `REFUPDDTS` | Surrogate key and timestamps. | Static dictionary table metadata. | Not business values. | Not exposed by reporting views. |
| `REFCTR` | `REFCOD` | Country code. | Static dictionary field, unique. | Required by schema. Business source for rows is unclear in current code. | Descriptive support for country-coded data such as `SECMDA.SECCTR`. |
| `REFCTR` | `REFNDE` | German country name. | Static dictionary field. | Required by schema. | Descriptive support only. |
| `REFCTR` | `REFNEN` | English country name. | Static dictionary field. | Nullable. | Descriptive support only. |
| `TAXLIN` | `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS` | Surrogate key and timestamps. | Database metadata for seeded dictionary rows. | Not source tax facts. | `TAXDAT.TAXLINIDN` foreign key target. |
| `TAXLIN` | `TAXCOD` | Tax line code such as `K40`, `K61`, `K62`. | `TaxLine.line_code` in `fondant\tax_registry.py`; upserted by `_ensure_tax_dictionaries`. | Required and unique. Code meaning is registry-defined, not inferred from amounts. | View SQL selects `K40`, `K61`, and `K62` by this code. |
| `TAXLIN` | `TAXKEY` | Stable semantic key such as `steuerpflichtige_einkuenfte`. | `TaxLine.metric_key` in `fondant\tax_registry.py`. | Required and unique. | Used by API/curated consumers; older view logic also used keys. |
| `TAXLIN` | `TAXNDE`, `TAXNEN` | German and English line labels. | Registry names from `TaxLine.name_de` and `TaxLine.name_en`. | `TAXNDE` required; `TAXNEN` nullable in schema but current registry provides values. | Human-readable labels for tax facts. |
| `TAXLIN` | `TAXORD` | Display/order number. | `TaxLine.line_order` in registry. | Required. | Stable ordering for query output. |
| `TAXLIN` | `TAXACT` | Active flag. | Ingestion seeds current registry lines with `is_active = True`. | Required. No inactive registry rows are currently defined. | Can distinguish current from future inactive line definitions. |
| `TAXLIN` | `TAXGVN`, `TAXGBS` | Valid-from / valid-to dates. | Seed path currently writes `None`. | Nullable; current rows do not encode effective dating. | Reserved for temporal dictionary semantics. |
| `TAXCAT` | `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS` | Surrogate key and timestamps. | Database metadata for seeded dictionary rows. | Not source tax facts. | `TAXDAT.TAXCATIDN` and `TAXADJ.TAXCATIDN` foreign key target. |
| `TAXCAT` | `TAXCOD` | Investor category code such as `PVM`, `PVO`, `BVM`, `BVO`, `BVJ`, `STF`. | `TaxCategory.category_code` in registry; upserted by `_ensure_tax_dictionaries`. | Required and unique. `STF` is the canonical dictionary/source code for foundation. | Used to identify category rows; reporting views use category keys, with `STI` as foundation output alias. |
| `TAXCAT` | `TAXKEY` | Stable semantic category key such as `pv_mit` or `stiftung`. | `TaxCategory.category_key` in registry. | Required and unique. | View SQL pivots by category key. |
| `TAXCAT` | `TAXNDE`, `TAXNEN` | German and English category labels. | Registry names from `TaxCategory.name_de` and `TaxCategory.name_en`. | `TAXNDE` required; `TAXNEN` nullable in schema but current registry provides values. | Human-readable labels for investor categories. |
| `TAXCAT` | `TAXORD` | Display/order number. | `TaxCategory.category_order` in registry. | Required. | Stable ordering for category display/query output. |

## Static Dictionaries Versus Downloaded Observations

`TAXLIN` and `TAXCAT` are static dictionaries in the current application flow. The ingestion pipeline calls `_ensure_tax_dictionaries`, which upserts rows from `LINE_DICTIONARY` and `CATEGORY_DICTIONARY`; both are derived from `fondant\tax_registry.py`. These rows define how parsed OeKB tax values are named, categorized, and joined, but they are not themselves downloaded tax facts.

`REFCCY` and `REFCTR` are also static reference dictionary tables by schema and documentation. Unlike `TAXLIN` and `TAXCAT`, no active application seed path for their row contents was found in the current code reviewed for this ticket. Their intended business source remains an open question.

`REFEXC` is different: it stores downloaded ECB observations. `backfill_ecb_rates` and `fetch_latest_ecb_rates` call the ECB client, convert returned rows into `ECBRatePoint` objects, and upsert them into `REFEXC` on `REFDAT + REFCCY`. `fetch_latest_ecb_rates` requests a short lookback window because ECB observations are not published for every calendar day, then keeps the latest observation per currency for that request.

## `REFEXC` In `V2_TAXDATEUR`

Current `V2_TAXDATEUR` behavior is implemented in `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py` and confirmed by `tests\test_tax_views_postgres.py`:

1. `V1_TAXDATPRE` produces selected fund-currency values and `FNDCCY`.
2. `V2_TAXDATEUR` joins `V1_TAXDATPRE` back to `TAXRPT` by `TAXISN + TAXOKBIDN` to obtain `TAXMDT`.
3. It left joins `REFEXC` where `REFEXC.REFCCY = FNDCCY` and `REFEXC.REFDAT = TAXMDT`.
4. If `FNDCCY = 'EUR'`, `FXRAT` is `1` and no `REFEXC` row is needed.
5. For non-EUR rows, `FXRAT` is `REFEXC.REFRAT` only when an exact date/currency row exists.
6. Converted values are calculated as selected fund-currency amounts divided by `FXRAT`.
7. If `FXRAT` is `NULL` or `0`, converted selected tax values are `NULL`.

This means current null EUR output can come from missing source tax values, a missing exact-date ECB observation, or a zero FX rate. The current view does not provide a diagnostic column to distinguish those cases. `SE-008_FX_CONVERSION_SEMANTICS_DECISION.md` approves nearest-prior ECB lookup as a future behavior, but this ticket does not implement it.

## `TAXLIN`, `TAXCAT`, And `fondant\tax_registry.py`

`fondant\tax_registry.py` is the code-level registry for live tax line and category definitions. It defines:

- tax line codes, metric keys, names, order, and source OeKB tax names;
- investor category codes, keys, names, order, parser aliases, source aliases, and view aliases;
- derived maps used by the parser and dictionaries used by ingestion.

`tests\test_tax_registry.py` verifies that parser maps, seed dictionaries, parser output models, `SOURCEAGE` source columns, and reporting-view aliases remain aligned with the registry.

Important alias boundary:

- `STF` is the canonical `TAXCAT.TAXCOD` and source/category code for foundation.
- `stiftung` is the semantic category key in parser/business terminology.
- `STI` is the reporting-view compatibility alias used in columns such as `K61STI`, `K62STI`, and `K40STI`.

## Open Questions

- What authoritative source should populate `REFCCY` rows and `REFCTR` rows? The schema and docs define the tables, but no current application seed path was found.
- Should `REFCCY` be enforced or joined from `REFEXC.REFCCY` or fund/security currencies, or remain descriptive only?
- Should future reporting distinguish null tax facts from null EUR conversion caused by missing/zero FX rates? Current `V2_TAXDATEUR` does not expose that distinction.
- Should `TAXLIN.TAXGVN` / `TAXGBS` eventually encode effective dates for tax line definitions? Current ingestion seeds them as null.

