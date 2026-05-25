# Data And Query Guide

## Data Available

The project stores data in three layers:

1. Source layer: OeKB report metadata, raw JSON payloads, and parsed source matrix values.
2. Curated layer: normalized security, tax report, tax line, tax category, tax value, adjustment, distribution, and correction tables.
3. Support layer: ECB FX rates, reference dictionaries, and ingestion logs/errors.

The repository also includes an ISIN universe file:

```text
Documentation/isin_storage.csv
```

Current stored ISINs in that file:

```text
IE00BMTX1Y45
LU1681044993
LU0380865021
LU0496786574
LU2009147757
IE000XZSV718
```

Actual rows in PostgreSQL depend on whether ingestion has been run. The data is fetched from OeKB and ECB at runtime; the repository does not bundle a full database dump.

## Database Connection

Default local PostgreSQL connection:

```text
Host: localhost
Port: 5432
Database: easyetfsat
User: easyetfsat
Password: easyetfsat
```

SQLAlchemy URL:

```text
postgresql+asyncpg://easyetfsat:easyetfsat@localhost:5432/easyetfsat
```

psql URL:

```text
postgresql://easyetfsat:easyetfsat@localhost:5432/easyetfsat
```

Start and initialize the database:

```bash
docker compose up -d
alembic upgrade head
```

All physical table and column names are uppercase, so use quoted identifiers in PostgreSQL SQL:

```sql
SELECT COUNT(*) FROM "TAXRPT";
```

## Table Summary

### Source Tables

| Table | Purpose | Grain |
|---|---|---|
| `SOURCERPT` | OeKB report metadata | One row per ISIN and OeKB report ID |
| `SOURCEAGE` | Parsed source tax matrix in wide format | One row per ISIN and OeKB report ID |
| `SOURCERAW` | Full raw OeKB detail JSON | One row per ISIN and OeKB report ID |

### Curated Tables

| Table | Purpose | Grain |
|---|---|---|
| `SECMDA` | Security master | One row per ISIN |
| `SECDIV` | Distribution/cash-flow event records | One row per ISIN, flow date, flow type, OeKB report ID |
| `TAXRPT` | Curated tax report header | One row per ISIN and OeKB report ID |
| `TAXLIN` | Tax line dictionary | One row per tax line |
| `TAXCAT` | Investor category dictionary | One row per category |
| `TAXDAT` | Curated tax values in narrow format | One row per report, tax line, category |
| `TAXADJ` | Fast-access tax adjustment values | One row per report, category, adjustment code |
| `TAXCOR` | Correction links between tax reports | One row per old/new report pair |

### Reference And Operations Tables

| Table | Purpose |
|---|---|
| `REFEXC` | ECB FX reference rates |
| `REFCCY` | Currency reference dictionary |
| `REFCTR` | Country reference dictionary |
| `IMPLOG` | Ingestion run log |
| `IMPERR` | Ingestion error log |

### Reporting Views

| View | Purpose |
|---|---|
| `V1_TAXDATPRE` | Pivoted selected tax values in fund currency |
| `V2_TAXDATEUR` | Same selected values converted to EUR using `REFEXC` |

`V1_TAXDATPRE` currently exposes `K61`, `K62`, and `K40` across investor categories. `V2_TAXDATEUR` adds `TAXMDT` and `FXRAT`, and divides non-EUR values by the ECB FX rate.

## Important Codes

Tax line codes:

| Code | Metric key | Meaning |
|---|---|---|
| `K40` | `steuerpflichtige_einkuenfte` | Taxable income |
| `K11` | `ag_ertraege` | Distributed income / AGErtraege |
| `K12` | `korrekturbetrag_saldiert` | Net correction amount |
| `K81` | `kest_total` | Total withholding tax |
| `K82` | `kest_substanzgewinne` | Withholding tax on substance gains |
| `K10` | `substanzgewinne_kestpfl` | Taxable substance gains |
| `K55` | `fondsergebnis_nichtausg` | Undistributed fund result |
| `K61` | `korrekturbetrag_age_ak` | Cost-basis adjustment |
| `K62` | `korrekturbetrag_aussch_ak` | Distribution cost-basis adjustment |
| `K36` | `substanzgew_folgejahre` | Substance gain in following years |
| `K21` | `quellensteuern_einbeh` | Retained withholding taxes |

Investor category codes:

| Code | Key | Meaning |
|---|---|---|
| `PVM` | `pv_mit` | Private assets with option |
| `PVO` | `pv_ohne` | Private assets without option |
| `BVM` | `bv_mit` | Business assets with option |
| `BVO` | `bv_ohne` | Business assets without option |
| `BVJ` | `bv_jur` | Business assets for legal entities |
| `STF` | `stiftung` | Foundation |

## Common SQL Queries

### 1. Check Schema Objects

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
```

```sql
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'public'
ORDER BY table_name;
```

### 2. List Available Securities

```sql
SELECT
  "SECISN",
  "SECNAM",
  "SECCCY",
  "SECCTR",
  "SECERT"
FROM "SECMDA"
ORDER BY "SECISN";
```

### 3. Show Report Coverage By ISIN

```sql
SELECT
  "TAXISN",
  MIN("TAXYEA") AS first_year,
  MAX("TAXYEA") AS last_year,
  COUNT(*) AS report_count
FROM "TAXRPT"
GROUP BY "TAXISN"
ORDER BY "TAXISN";
```

### 4. Find Latest Report Per ISIN And Year

```sql
WITH ranked AS (
  SELECT
    r.*,
    ROW_NUMBER() OVER (
      PARTITION BY r."TAXISN", r."TAXYEA"
      ORDER BY r."TAXMDT" DESC NULLS LAST, r."TAXOKBIDN" DESC
    ) AS rn
  FROM "TAXRPT" r
)
SELECT
  "TAXISN",
  "TAXYEA",
  "TAXOKBIDN",
  "TAXVRN",
  "TAXSTS",
  "TAXMDT",
  "TAXCCY",
  "TAXISB"
FROM ranked
WHERE rn = 1
ORDER BY "TAXISN", "TAXYEA";
```

### 5. Query All Tax Values For One ISIN And Year

```sql
SELECT
  r."TAXISN",
  r."TAXYEA",
  r."TAXOKBIDN",
  r."TAXMDT",
  r."TAXCCY",
  l."TAXCOD" AS tax_line_code,
  l."TAXKEY" AS tax_line_key,
  c."TAXCOD" AS category_code,
  c."TAXKEY" AS category_key,
  d."TAXAMT",
  d."TAXCCY" AS value_currency
FROM "TAXRPT" r
JOIN "TAXDAT" d
  ON d."TAXRPTIDN" = r."TAXIDN"
 AND d."TAXOKBIDN" = r."TAXOKBIDN"
JOIN "TAXLIN" l
  ON l."TAXIDN" = d."TAXLINIDN"
JOIN "TAXCAT" c
  ON c."TAXIDN" = d."TAXCATIDN"
WHERE r."TAXISN" = 'IE00BMTX1Y45'
  AND r."TAXYEA" = 2025
ORDER BY r."TAXMDT" DESC NULLS LAST, l."TAXORD", c."TAXORD";
```

### 6. Query Cost-Basis Adjustment Lines

```sql
SELECT
  r."TAXISN",
  r."TAXYEA",
  r."TAXOKBIDN",
  l."TAXCOD",
  l."TAXKEY",
  c."TAXCOD" AS category_code,
  d."TAXAMT",
  d."TAXCCY"
FROM "TAXRPT" r
JOIN "TAXDAT" d
  ON d."TAXRPTIDN" = r."TAXIDN"
JOIN "TAXLIN" l
  ON l."TAXIDN" = d."TAXLINIDN"
JOIN "TAXCAT" c
  ON c."TAXIDN" = d."TAXCATIDN"
WHERE l."TAXCOD" IN ('K61', 'K62')
ORDER BY r."TAXISN", r."TAXYEA", l."TAXCOD", c."TAXORD";
```

### 7. Use The Pivoted Fund-Currency View

```sql
SELECT
  "TAXISN",
  "TAXYEA",
  "TAXOKBIDN",
  "FNDCCY",
  "K61PVM",
  "K62PVM",
  "K40PVM"
FROM "V1_TAXDATPRE"
WHERE "TAXISN" = 'IE00BMTX1Y45'
ORDER BY "TAXYEA", "TAXOKBIDN";
```

### 8. Use The EUR-Converted View

```sql
SELECT
  "TAXISN",
  "TAXYEA",
  "TAXOKBIDN",
  "FNDCCY",
  "TAXMDT",
  "FXRAT",
  "K61PVM",
  "K62PVM",
  "K40PVM"
FROM "V2_TAXDATEUR"
WHERE "TAXISN" = 'IE00BMTX1Y45'
ORDER BY "TAXYEA", "TAXOKBIDN";
```

If `FXRAT` is `NULL`, the exact ECB date/currency observation needed for conversion is missing.

### 9. Inspect Raw OeKB Payloads

```sql
SELECT
  "SRCISN",
  "SRCOKBIDN",
  "SRCVRN",
  "SRCPAY"
FROM "SOURCERAW"
WHERE "SRCISN" = 'IE00BMTX1Y45'
ORDER BY "SRCOKBIDN";
```

PostgreSQL JSONB example:

```sql
SELECT
  "SRCISN",
  "SRCOKBIDN",
  "SRCPAY" ->> 'waehrung' AS payload_currency
FROM "SOURCERAW"
WHERE "SRCISN" = 'IE00BMTX1Y45';
```

### 10. Check Ingestion Runs And Errors

```sql
SELECT
  "IMPRUNIDN",
  "IMPISN",
  "IMPSTS",
  "IMPRSN" AS records_seen,
  "IMPRSW" AS records_written,
  "IMPSTADTS",
  "IMPFINDTS",
  "IMPMSG"
FROM "IMPLOG"
ORDER BY "IMPSTADTS" DESC;
```

```sql
SELECT
  "IMPRUNIDN",
  "IMPISN",
  "IMPSTG",
  "IMPECD",
  "IMPEMS"
FROM "IMPERR"
ORDER BY "IMPCRTDTS" DESC;
```

### 11. Check ECB FX Coverage

```sql
SELECT
  "REFCCY",
  MIN("REFDAT") AS first_rate_date,
  MAX("REFDAT") AS last_rate_date,
  COUNT(*) AS rate_count
FROM "REFEXC"
GROUP BY "REFCCY"
ORDER BY "REFCCY";
```

## API Querying

Start the API:

```bash
uvicorn fondant.api.main:app --reload
```

Health check:

```bash
curl http://127.0.0.1:8000/health
```

ETF tax lookup:

```bash
curl "http://127.0.0.1:8000/etf/IE00BMTX1Y45/tax?year=2025"
```

The API returns all matching reports for the ISIN/year ordered by descending report date. If no matching year exists but rows exist with `TAXYEA IS NULL`, the response sets:

```json
{
  "year_fallback_null_used": true
}
```

## Ingestion Commands

Fetch ISINs present in storage but missing from `SOURCERPT`:

```bash
python -m fondant.jobs.fetch_missing_isins --dry-run --show-isins
python -m fondant.jobs.fetch_missing_isins
```

Force fetch even if already present:

```bash
python -m fondant.jobs.fetch_missing_isins --force
```

Add and persist an ISIN:

```bash
python -m fondant.jobs.fetch_missing_isins --isin IE00BMTX1Y45 --persist-input
```

Refresh already ingested ISINs:

```bash
python -m fondant.jobs.refresh_existing_isins --dry-run --show-isins
python -m fondant.jobs.refresh_existing_isins
```

Backfill ECB FX rates:

```bash
python - <<'PY'
import asyncio
from fondant.ingestion.fx_pipeline import backfill_ecb_rates
print(asyncio.run(backfill_ecb_rates()))
PY
```

## Practical Query Strategy

Use these objects depending on the job:

- Fast product lookup: API endpoint.
- Analyst SQL and BI tools: `TAXRPT`, `TAXDAT`, `TAXLIN`, `TAXCAT`.
- Spreadsheet-style output: `V1_TAXDATPRE` or `V2_TAXDATEUR`.
- Audit and debugging: `SOURCERPT`, `SOURCERAW`, `SOURCEAGE`, `IMPLOG`, `IMPERR`.
- Currency checks: `REFEXC`.
- Correction analysis: `TAXCOR`.

## Notes For Data Consumers

- `TAXDAT` is the most flexible fact table because it is narrow and joins to dictionaries.
- `SOURCEAGE` is useful when comparing parser output directly to OeKB source concepts.
- `SOURCERAW` is the authoritative raw payload archive.
- `V1_TAXDATPRE` and `V2_TAXDATEUR` are convenience views, not the complete tax dataset.
- The API currently serializes amounts as floats, while database values are stored as numeric/decimal values.
