# FD-001 Database Field Inventory

Ticket: FD-001  
Epic: Field Lineage Documentation  
Scope: inventory only; no schema, data, code, test, credential, Docker, or network behavior changes.

## Evidence And Existing Documentation

This artifact reuses the existing layer explanations instead of restating detailed lineage:

- `Documentation\DATA_AND_QUERY_GUIDE.md` for table/view purpose, query strategy, and current reporting-view behavior.
- `Documentation\TECHNICAL_ARCHITECTURE.md` for final database objects and ingestion/API architecture.
- `Documentation\MAINTAINER_LAYER_GUIDE.md` for source, curated, reporting-view, API, and ingestion boundaries.
- `Documentation\STAKEHOLDER_BRIEF.md` for stakeholder-facing purpose and known gaps.
- `Documentation\AgentInstructions\FETCH_ONLY_MISSING_ISINS.md` and `Documentation\AgentInstructions\REFRESH_EXISTING_ISINS.md` for operational ingestion workflows.
- `Documentation\Validation\FIELD_LINEAGE_DOCUMENTATION_EPIC.md` for the later field-lineage ticket owners.

Schema evidence used:

- SQLAlchemy ORM metadata in `fondant\db\models\sec.py`, `fondant\db\models\tax.py`, `fondant\db\models\ref.py`, `fondant\db\models\imp.py`, and `fondant\db\base.py`.
- Final view SQL in `alembic\versions\20260419_0011_refine_v1_and_add_v2_taxdateur.py`.
- Alembic metadata convention for `alembic_version.version_num`.

Local PostgreSQL metadata could not be queried in this session because `psql` is not installed or not on PATH. No database writes were attempted. View nullable status is therefore marked `unknown (view)` where direct PostgreSQL metadata was unavailable.

## PostgreSQL Metadata Queries

These are the SELECT-only metadata queries intended for a local PostgreSQL validation pass:

```sql
SELECT
  c.table_schema,
  c.table_name AS object_name,
  t.table_type,
  c.ordinal_position,
  c.column_name,
  c.data_type,
  c.udt_name,
  c.character_maximum_length,
  c.numeric_precision,
  c.numeric_scale,
  c.is_nullable
FROM information_schema.columns c
JOIN information_schema.tables t
  ON t.table_schema = c.table_schema
 AND t.table_name = c.table_name
WHERE c.table_schema = 'public'
ORDER BY c.table_name, c.ordinal_position;
```

```sql
SELECT
  table_schema,
  table_name
FROM information_schema.views
WHERE table_schema = 'public'
ORDER BY table_name;
```

```sql
SELECT
  schemaname,
  viewname,
  definition
FROM pg_views
WHERE schemaname = 'public'
  AND viewname IN ('V1_TAXDATPRE', 'V2_TAXDATEUR')
ORDER BY viewname;
```

## Later Ticket Ownership

| Object group | Detailed semantics owner |
|---|---|
| `SOURCERPT`, `SOURCERAW`, source portions of `SOURCEAGE` | FD-002 |
| `REFEXC`, `REFCCY`, `REFCTR`, `TAXLIN`, `TAXCAT` | FD-003 |
| `TAXRPT`, `TAXDAT`, `TAXADJ`, `SECDIV`, `TAXCOR`, `SECMDA`, curated portions of `SOURCEAGE` | FD-004 |
| `V1_TAXDATPRE`, `V2_TAXDATEUR` | FD-005 |
| `IMPLOG`, `IMPERR`, `alembic_version` | FD-006 final consolidation unless a later ops-lineage ticket is added |

## Object Summary

| Object | Type | Layer classification | Initial purpose | Detailed owner |
|---|---|---|---|---|
| `SOURCERPT` | table | source report metadata | OeKB report-list metadata, one row per ISIN and OeKB report ID. | FD-002 |
| `SOURCERAW` | table | raw source archive | Full OeKB report-detail JSON payload archive. | FD-002 |
| `SOURCEAGE` | table | parsed source matrix | Parsed OeKB tax matrix in source-shaped wide columns. | FD-002 / FD-004 |
| `SECMDA` | table | curated tax/report facts | Security master rows keyed by ISIN. | FD-004 |
| `SECDIV` | table | curated tax/report facts | Distribution/cash-flow event rows curated from source reports. | FD-004 |
| `TAXRPT` | table | curated tax/report facts | Curated tax report header copied from source report metadata. | FD-004 |
| `TAXDAT` | table | curated tax/report facts | Narrow curated tax fact values by report, tax line, and investor category. | FD-004 |
| `TAXADJ` | table | curated tax/report facts | Fast-access adjustment values currently populated from selected adjustment lines. | FD-004 |
| `TAXCOR` | table | curated tax/report facts | Correction links between old and new tax reports. | FD-004 |
| `TAXLIN` | table | reference/dictionary data | Tax line dictionary seeded from the tax registry. | FD-003 |
| `TAXCAT` | table | reference/dictionary data | Investor category dictionary seeded from the tax registry. | FD-003 |
| `REFCCY` | table | reference/dictionary data | Currency reference dictionary. | FD-003 |
| `REFCTR` | table | reference/dictionary data | Country reference dictionary. | FD-003 |
| `REFEXC` | table | reference/dictionary data | ECB FX reference-rate observations. | FD-003 |
| `IMPLOG` | table | operational logging | Ingestion run status and counters. | FD-006 |
| `IMPERR` | table | operational logging | Ingestion error details. | FD-006 |
| `V1_TAXDATPRE` | view | reporting view | Selected `K61`, `K62`, and `K40` values pivoted in fund currency. | FD-005 |
| `V2_TAXDATEUR` | view | reporting view | Selected `V1_TAXDATPRE` values converted to EUR with exact-date `REFEXC` rates. | FD-005 |
| `alembic_version` | table | schema metadata | Alembic migration revision marker. | FD-006 |

## Column Inventory

| Object | Layer classification | Column | Data type | Nullable | Short initial purpose | Detailed owner |
|---|---|---|---|---|---|---|
| `alembic_version` | schema metadata | `version_num` | `VARCHAR(32)` | NO | Alembic revision identifier. | FD-006 |
| `IMPERR` | operational logging | `IMPIDN` | `INTEGER` | NO | Surrogate key. | FD-006 |
| `IMPERR` | operational logging | `IMPCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-006 |
| `IMPERR` | operational logging | `IMPUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-006 |
| `IMPERR` | operational logging | `IMPRUNIDN` | `UUID` | NO | Ingestion run UUID. | FD-006 |
| `IMPERR` | operational logging | `IMPISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-006 |
| `IMPERR` | operational logging | `IMPOKBIDN` | `INTEGER` | YES | OeKB report ID when available. | FD-006 |
| `IMPERR` | operational logging | `IMPSTG` | `VARCHAR(64)` | NO | Pipeline stage where error occurred. | FD-006 |
| `IMPERR` | operational logging | `IMPECD` | `VARCHAR(64)` | YES | Error code. | FD-006 |
| `IMPERR` | operational logging | `IMPEMS` | `TEXT` | NO | Error message. | FD-006 |
| `IMPERR` | operational logging | `IMPPAY` | `JSONB on PostgreSQL / JSON elsewhere` | YES | Error payload snapshot. | FD-006 |
| `IMPLOG` | operational logging | `IMPIDN` | `INTEGER` | NO | Surrogate key. | FD-006 |
| `IMPLOG` | operational logging | `IMPCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-006 |
| `IMPLOG` | operational logging | `IMPUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-006 |
| `IMPLOG` | operational logging | `IMPRUNIDN` | `UUID` | NO | Ingestion run UUID. | FD-006 |
| `IMPLOG` | operational logging | `IMPISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-006 |
| `IMPLOG` | operational logging | `IMPOKBIDN` | `INTEGER` | YES | OeKB report ID when available. | FD-006 |
| `IMPLOG` | operational logging | `IMPSTS` | `VARCHAR(24)` | NO | Ingestion status. | FD-006 |
| `IMPLOG` | operational logging | `IMPMSG` | `TEXT` | YES | Ingestion message. | FD-006 |
| `IMPLOG` | operational logging | `IMPRSN` | `INTEGER` | NO | Records seen count. | FD-006 |
| `IMPLOG` | operational logging | `IMPRSW` | `INTEGER` | NO | Records written count. | FD-006 |
| `IMPLOG` | operational logging | `IMPSTADTS` | `TIMESTAMP` | NO | Run start timestamp. | FD-006 |
| `IMPLOG` | operational logging | `IMPFINDTS` | `TIMESTAMP` | YES | Run finish timestamp. | FD-006 |
| `REFCCY` | reference/dictionary data | `REFIDN` | `INTEGER` | NO | Surrogate key. | FD-003 |
| `REFCCY` | reference/dictionary data | `REFCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-003 |
| `REFCCY` | reference/dictionary data | `REFUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-003 |
| `REFCCY` | reference/dictionary data | `REFCOD` | `VARCHAR(3)` | NO | Currency code. | FD-003 |
| `REFCCY` | reference/dictionary data | `REFNAM` | `VARCHAR(64)` | NO | Currency name. | FD-003 |
| `REFCCY` | reference/dictionary data | `REFMUN` | `SMALLINT` | YES | Minor currency units. | FD-003 |
| `REFCTR` | reference/dictionary data | `REFIDN` | `INTEGER` | NO | Surrogate key. | FD-003 |
| `REFCTR` | reference/dictionary data | `REFCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-003 |
| `REFCTR` | reference/dictionary data | `REFUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-003 |
| `REFCTR` | reference/dictionary data | `REFCOD` | `VARCHAR(2)` | NO | Country code. | FD-003 |
| `REFCTR` | reference/dictionary data | `REFNDE` | `VARCHAR(128)` | NO | German country name. | FD-003 |
| `REFCTR` | reference/dictionary data | `REFNEN` | `VARCHAR(128)` | YES | English country name. | FD-003 |
| `REFEXC` | reference/dictionary data | `REFIDN` | `INTEGER` | NO | Surrogate key. | FD-003 |
| `REFEXC` | reference/dictionary data | `REFCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-003 |
| `REFEXC` | reference/dictionary data | `REFUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-003 |
| `REFEXC` | reference/dictionary data | `REFDAT` | `DATE` | NO | ECB reference-rate date. | FD-003 |
| `REFEXC` | reference/dictionary data | `REFCCY` | `VARCHAR(3)` | NO | Currency code. | FD-003 |
| `REFEXC` | reference/dictionary data | `REFRAT` | `NUMERIC(20, 10)` | NO | ECB reference rate. | FD-003 |
| `SECMDA` | curated tax/report facts | `SECIDN` | `INTEGER` | NO | Surrogate key. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECNAM` | `VARCHAR(255)` | NO | Security name. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECCCY` | `VARCHAR(3)` | YES | Security currency. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECCTR` | `VARCHAR(2)` | YES | Security domicile country code. | FD-004 |
| `SECMDA` | curated tax/report facts | `SECERT` | `VARCHAR(64)` | YES | Security income/distribution type. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECIDN` | `INTEGER` | NO | Surrogate key. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECOKBIDN` | `BIGINT` | YES | OeKB report ID when available. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECFLWTYP` | `VARCHAR(24)` | NO | Distribution or cash-flow type. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECFLWDAT` | `DATE` | NO | Distribution or cash-flow date. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECFLWAMT` | `NUMERIC(20, 10)` | YES | Cash-flow amount. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECCCY` | `VARCHAR(3)` | YES | Cash-flow currency. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECYEA` | `INTEGER` | YES | Report/tax year. | FD-004 |
| `SECDIV` | curated tax/report facts | `SECSTS` | `VARCHAR(16)` | YES | Source report status code. | FD-004 |
| `SOURCERAW` | raw source archive | `SRCIDN` | `INTEGER` | NO | Surrogate key. | FD-002 |
| `SOURCERAW` | raw source archive | `SRCCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-002 |
| `SOURCERAW` | raw source archive | `SRCUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-002 |
| `SOURCERAW` | raw source archive | `SRCISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-002 |
| `SOURCERAW` | raw source archive | `SRCOKBIDN` | `BIGINT` | NO | OeKB report ID. | FD-002 |
| `SOURCERAW` | raw source archive | `SRCVRN` | `INTEGER` | NO | OeKB version number. | FD-002 |
| `SOURCERAW` | raw source archive | `SRCPAY` | `JSONB on PostgreSQL / JSON elsewhere` | NO | Full raw OeKB report-detail payload. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCIDN` | `INTEGER` | NO | Surrogate key. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCOKBIDN` | `BIGINT` | NO | OeKB report ID. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCVRN` | `INTEGER` | NO | OeKB version number. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCSTS` | `VARCHAR(16)` | YES | OeKB report status code. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCYEA` | `INTEGER` | YES | Report/tax year. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCMDT` | `DATE` | YES | Report date. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCCCY` | `VARCHAR(3)` | YES | Report currency. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCISB` | `VARCHAR(255)` | YES | ISIN description/name from source metadata. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCGVN` | `DATE` | YES | Valid-from date. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCGBS` | `DATE` | YES | Valid-to date. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCBUSYEABEG` | `DATE` | YES | Business-year begin date. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCBUSYEAEND` | `DATE` | YES | Business-year end date. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCENTDTS` | `TIMESTAMP` | YES | Source entry timestamp. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCZFL` | `DATE` | YES | Cash-flow date. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCJMS` | `BOOLEAN` | YES | Annual-report flag. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCAMS` | `BOOLEAN` | YES | Distribution-report flag. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCSNW` | `BOOLEAN` | YES | Self-assessment flag. | FD-002 |
| `SOURCERPT` | source report metadata | `SRCKIDN` | `BIGINT` | YES | Corrected OeKB report ID. | FD-002 |
| `SOURCEAGE` | parsed source matrix | `SRCIDN` | `INTEGER` | NO | Surrogate key. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCOKBIDN` | `BIGINT` | NO | OeKB report ID. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCVRN` | `INTEGER` | NO | OeKB version number. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCYEA` | `INTEGER` | YES | Report/tax year. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK40PVM` | `NUMERIC(20, 10)` | YES | K40 taxable income for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK40PVO` | `NUMERIC(20, 10)` | YES | K40 taxable income for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK40BVM` | `NUMERIC(20, 10)` | YES | K40 taxable income for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK40BVO` | `NUMERIC(20, 10)` | YES | K40 taxable income for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK40BVJ` | `NUMERIC(20, 10)` | YES | K40 taxable income for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK40STF` | `NUMERIC(20, 10)` | YES | K40 taxable income for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK11PVM` | `NUMERIC(20, 10)` | YES | K11 distributed income for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK11PVO` | `NUMERIC(20, 10)` | YES | K11 distributed income for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK11BVM` | `NUMERIC(20, 10)` | YES | K11 distributed income for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK11BVO` | `NUMERIC(20, 10)` | YES | K11 distributed income for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK11BVJ` | `NUMERIC(20, 10)` | YES | K11 distributed income for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK11STF` | `NUMERIC(20, 10)` | YES | K11 distributed income for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK12PVM` | `NUMERIC(20, 10)` | YES | K12 net correction amount for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK12PVO` | `NUMERIC(20, 10)` | YES | K12 net correction amount for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK12BVM` | `NUMERIC(20, 10)` | YES | K12 net correction amount for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK12BVO` | `NUMERIC(20, 10)` | YES | K12 net correction amount for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK12BVJ` | `NUMERIC(20, 10)` | YES | K12 net correction amount for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK12STF` | `NUMERIC(20, 10)` | YES | K12 net correction amount for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK81PVM` | `NUMERIC(20, 10)` | YES | K81 withholding tax total for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK81PVO` | `NUMERIC(20, 10)` | YES | K81 withholding tax total for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK81BVM` | `NUMERIC(20, 10)` | YES | K81 withholding tax total for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK81BVO` | `NUMERIC(20, 10)` | YES | K81 withholding tax total for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK81BVJ` | `NUMERIC(20, 10)` | YES | K81 withholding tax total for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK81STF` | `NUMERIC(20, 10)` | YES | K81 withholding tax total for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK82PVM` | `NUMERIC(20, 10)` | YES | K82 withholding tax on substance gains for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK82PVO` | `NUMERIC(20, 10)` | YES | K82 withholding tax on substance gains for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK82BVM` | `NUMERIC(20, 10)` | YES | K82 withholding tax on substance gains for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK82BVO` | `NUMERIC(20, 10)` | YES | K82 withholding tax on substance gains for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK82BVJ` | `NUMERIC(20, 10)` | YES | K82 withholding tax on substance gains for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK82STF` | `NUMERIC(20, 10)` | YES | K82 withholding tax on substance gains for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK10PVM` | `NUMERIC(20, 10)` | YES | K10 taxable substance gains for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK10PVO` | `NUMERIC(20, 10)` | YES | K10 taxable substance gains for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK10BVM` | `NUMERIC(20, 10)` | YES | K10 taxable substance gains for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK10BVO` | `NUMERIC(20, 10)` | YES | K10 taxable substance gains for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK10BVJ` | `NUMERIC(20, 10)` | YES | K10 taxable substance gains for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK10STF` | `NUMERIC(20, 10)` | YES | K10 taxable substance gains for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK55PVM` | `NUMERIC(20, 10)` | YES | K55 undistributed fund result for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK55PVO` | `NUMERIC(20, 10)` | YES | K55 undistributed fund result for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK55BVM` | `NUMERIC(20, 10)` | YES | K55 undistributed fund result for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK55BVO` | `NUMERIC(20, 10)` | YES | K55 undistributed fund result for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK55BVJ` | `NUMERIC(20, 10)` | YES | K55 undistributed fund result for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK55STF` | `NUMERIC(20, 10)` | YES | K55 undistributed fund result for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK61PVM` | `NUMERIC(20, 10)` | YES | K61 cost-basis adjustment for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK61PVO` | `NUMERIC(20, 10)` | YES | K61 cost-basis adjustment for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK61BVM` | `NUMERIC(20, 10)` | YES | K61 cost-basis adjustment for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK61BVO` | `NUMERIC(20, 10)` | YES | K61 cost-basis adjustment for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK61BVJ` | `NUMERIC(20, 10)` | YES | K61 cost-basis adjustment for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK61STF` | `NUMERIC(20, 10)` | YES | K61 cost-basis adjustment for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK62PVM` | `NUMERIC(20, 10)` | YES | K62 distribution cost-basis adjustment for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK62PVO` | `NUMERIC(20, 10)` | YES | K62 distribution cost-basis adjustment for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK62BVM` | `NUMERIC(20, 10)` | YES | K62 distribution cost-basis adjustment for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK62BVO` | `NUMERIC(20, 10)` | YES | K62 distribution cost-basis adjustment for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK62BVJ` | `NUMERIC(20, 10)` | YES | K62 distribution cost-basis adjustment for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK62STF` | `NUMERIC(20, 10)` | YES | K62 distribution cost-basis adjustment for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK36PVM` | `NUMERIC(20, 10)` | YES | K36 substance gain following years for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK36PVO` | `NUMERIC(20, 10)` | YES | K36 substance gain following years for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK36BVM` | `NUMERIC(20, 10)` | YES | K36 substance gain following years for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK36BVO` | `NUMERIC(20, 10)` | YES | K36 substance gain following years for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK36BVJ` | `NUMERIC(20, 10)` | YES | K36 substance gain following years for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK36STF` | `NUMERIC(20, 10)` | YES | K36 substance gain following years for foundation. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK21PVM` | `NUMERIC(20, 10)` | YES | K21 retained withholding taxes for private assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK21PVO` | `NUMERIC(20, 10)` | YES | K21 retained withholding taxes for private assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK21BVM` | `NUMERIC(20, 10)` | YES | K21 retained withholding taxes for business assets with option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK21BVO` | `NUMERIC(20, 10)` | YES | K21 retained withholding taxes for business assets without option. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK21BVJ` | `NUMERIC(20, 10)` | YES | K21 retained withholding taxes for business assets legal entities. | FD-002 / FD-004 |
| `SOURCEAGE` | parsed source matrix | `SRCK21STF` | `NUMERIC(20, 10)` | YES | K21 retained withholding taxes for foundation. | FD-002 / FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXIDN` | `INTEGER` | NO | Surrogate key. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXISN` | `VARCHAR(12)` | NO | ISIN identifier. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXOKBIDN` | `BIGINT` | NO | OeKB report ID. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXVRN` | `INTEGER` | NO | OeKB version number. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXSTS` | `VARCHAR(16)` | YES | Report status code. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXYEA` | `INTEGER` | YES | Report/tax year. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXMDT` | `DATE` | YES | Report date. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXCCY` | `VARCHAR(3)` | YES | Report currency. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXISB` | `VARCHAR(255)` | YES | ISIN description/name from source metadata. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXGVN` | `DATE` | YES | Valid-from date. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXGBS` | `DATE` | YES | Valid-to date. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXBUSYEABEG` | `DATE` | YES | Business-year begin date. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXBUSYEAEND` | `DATE` | YES | Business-year end date. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXZFL` | `DATE` | YES | Cash-flow date. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXJMS` | `BOOLEAN` | YES | Annual-report flag. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXAMS` | `BOOLEAN` | YES | Distribution-report flag. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXSNW` | `BOOLEAN` | YES | Self-assessment flag. | FD-004 |
| `TAXRPT` | curated tax/report facts | `TAXKIDN` | `BIGINT` | YES | Corrected OeKB report ID. | FD-004 |
| `TAXLIN` | reference/dictionary data | `TAXIDN` | `INTEGER` | NO | Surrogate key. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXCOD` | `VARCHAR(16)` | NO | Tax line code. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXKEY` | `VARCHAR(64)` | NO | Tax line semantic key. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXNDE` | `VARCHAR(255)` | NO | German tax line name. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXNEN` | `VARCHAR(255)` | YES | English tax line name. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXORD` | `SMALLINT` | NO | Tax line display/order number. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXACT` | `BOOLEAN` | NO | Active flag. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXGVN` | `DATE` | YES | Valid-from date. | FD-003 |
| `TAXLIN` | reference/dictionary data | `TAXGBS` | `DATE` | YES | Valid-to date. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXIDN` | `INTEGER` | NO | Surrogate key. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXCOD` | `VARCHAR(16)` | NO | Investor category code. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXKEY` | `VARCHAR(64)` | NO | Investor category semantic key. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXNDE` | `VARCHAR(255)` | NO | German investor category name. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXNEN` | `VARCHAR(255)` | YES | English investor category name. | FD-003 |
| `TAXCAT` | reference/dictionary data | `TAXORD` | `SMALLINT` | NO | Investor category display/order number. | FD-003 |
| `TAXDAT` | curated tax/report facts | `TAXIDN` | `INTEGER` | NO | Surrogate key. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXRPTIDN` | `INTEGER` | NO | Related `TAXRPT` key. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXOKBIDN` | `BIGINT` | NO | OeKB report ID. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXLINIDN` | `INTEGER` | NO | Related `TAXLIN` key. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXCATIDN` | `INTEGER` | NO | Related `TAXCAT` key. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXAMT` | `NUMERIC(20, 10)` | NO | Curated tax amount. | FD-004 |
| `TAXDAT` | curated tax/report facts | `TAXCCY` | `VARCHAR(3)` | YES | Amount currency. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXIDN` | `INTEGER` | NO | Surrogate key. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXRPTIDN` | `INTEGER` | NO | Related `TAXRPT` key. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXOKBIDN` | `BIGINT` | NO | OeKB report ID. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXCATIDN` | `INTEGER` | NO | Related `TAXCAT` key. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXCOD` | `VARCHAR(16)` | NO | Adjustment code. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXAMT` | `NUMERIC(20, 10)` | NO | Adjustment amount. | FD-004 |
| `TAXADJ` | curated tax/report facts | `TAXCCY` | `VARCHAR(3)` | YES | Adjustment currency. | FD-004 |
| `TAXCOR` | curated tax/report facts | `TAXIDN` | `INTEGER` | NO | Surrogate key. | FD-004 |
| `TAXCOR` | curated tax/report facts | `TAXCRTDTS` | `TIMESTAMP` | NO | Row creation timestamp. | FD-004 |
| `TAXCOR` | curated tax/report facts | `TAXUPDDTS` | `TIMESTAMP` | NO | Row update timestamp. | FD-004 |
| `TAXCOR` | curated tax/report facts | `TAXOLDRPTIDN` | `INTEGER` | NO | Corrected/old `TAXRPT` key. | FD-004 |
| `TAXCOR` | curated tax/report facts | `TAXNEWRPTIDN` | `INTEGER` | NO | Correcting/new `TAXRPT` key. | FD-004 |
| `TAXCOR` | curated tax/report facts | `TAXRSN` | `VARCHAR(32)` | YES | Correction reason code. | FD-004 |
| `V1_TAXDATPRE` | reporting view | `TAXISN` | `VARCHAR(12)` | unknown (view) | ISIN identifier from `TAXRPT`. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `TAXOKBIDN` | `BIGINT` | unknown (view) | OeKB report ID from `TAXRPT`. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `TAXYEA` | `INTEGER` | unknown (view) | Report/tax year from `TAXRPT`. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `FNDCCY` | `VARCHAR(3)` | unknown (view) | Fund/report currency from `TAXRPT.TAXCCY`. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K61PVM` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K61 amount for PVM. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K61PVO` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K61 amount for PVO. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K61BVM` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K61 amount for BVM. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K61BVO` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K61 amount for BVO. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K61BVJ` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K61 amount for BVJ. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K61STI` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K61 amount for foundation reporting alias `STI`. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K62PVM` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K62 amount for PVM. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K62PVO` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K62 amount for PVO. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K62BVM` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K62 amount for BVM. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K62BVO` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K62 amount for BVO. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K62BVJ` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K62 amount for BVJ. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K62STI` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K62 amount for foundation reporting alias `STI`. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K40PVM` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K40 amount for PVM. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K40PVO` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K40 amount for PVO. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K40BVM` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K40 amount for BVM. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K40BVO` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K40 amount for BVO. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K40BVJ` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K40 amount for BVJ. | FD-005 |
| `V1_TAXDATPRE` | reporting view | `K40STI` | `NUMERIC(20, 10)` | unknown (view) | Pivoted K40 amount for foundation reporting alias `STI`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `TAXISN` | `VARCHAR(12)` | unknown (view) | ISIN identifier from `V1_TAXDATPRE`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `TAXOKBIDN` | `BIGINT` | unknown (view) | OeKB report ID from `V1_TAXDATPRE`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `TAXYEA` | `INTEGER` | unknown (view) | Report/tax year from `V1_TAXDATPRE`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `FNDCCY` | `VARCHAR(3)` | unknown (view) | Fund/report currency from `V1_TAXDATPRE`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `TAXMDT` | `DATE` | unknown (view) | Report date from `TAXRPT.TAXMDT`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `FXRAT` | `NUMERIC(20, 10)` | unknown (view) | EUR conversion divisor from exact-date `REFEXC`, or 1 for EUR. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K61PVM` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K61 amount for PVM. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K61PVO` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K61 amount for PVO. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K61BVM` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K61 amount for BVM. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K61BVO` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K61 amount for BVO. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K61BVJ` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K61 amount for BVJ. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K61STI` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K61 amount for foundation reporting alias `STI`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K62PVM` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K62 amount for PVM. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K62PVO` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K62 amount for PVO. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K62BVM` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K62 amount for BVM. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K62BVO` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K62 amount for BVO. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K62BVJ` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K62 amount for BVJ. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K62STI` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K62 amount for foundation reporting alias `STI`. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K40PVM` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K40 amount for PVM. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K40PVO` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K40 amount for PVO. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K40BVM` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K40 amount for BVM. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K40BVO` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K40 amount for BVO. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K40BVJ` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K40 amount for BVJ. | FD-005 |
| `V2_TAXDATEUR` | reporting view | `K40STI` | `NUMERIC(20, 10)` | unknown (view) | EUR-converted K40 amount for foundation reporting alias `STI`. | FD-005 |

## Inventory Limits

- This is an inventory-first artifact. It intentionally does not define final null/zero semantics or deep tax meaning.
- `SOURCEAGE` matrix labels use the code/category names already present in existing docs; FD-002 and FD-004 own source and transformation detail.
- `V1_TAXDATPRE` and `V2_TAXDATEUR` column types and nullable status should be confirmed with the PostgreSQL metadata query above during validation because direct metadata was unavailable here.
- `alembic_version` is included as schema metadata because a migrated database normally has this Alembic table even though it is not an application model.
