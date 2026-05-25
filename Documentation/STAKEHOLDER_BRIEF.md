# Stakeholder Brief

## What Is Implemented

EasyETFsAT is a working backend for Austrian ETF tax reporting data. It connects to the public OeKB fund information API, fetches tax reports by ISIN, stores the original payloads, parses selected Austrian tax fields, and writes them into a PostgreSQL database designed for reporting and auditability.

Implemented capabilities:

- OeKB report-list and report-detail client with rate limiting and timeout configuration.
- ISIN ingestion pipeline with idempotent upserts.
- Storage of raw OeKB JSON payloads for audit and reprocessing.
- Parsed source table with the relevant OeKB tax matrix fields.
- Curated tax data model with report headers, tax-line dictionary, investor-category dictionary, and narrow tax values.
- Cost-basis adjustment extraction for selected lines.
- Distribution event tracking when OeKB marks a report as a distribution report and provides a cash-flow date.
- Correction-chain tracking when a report points to a corrected OeKB report ID.
- ECB FX-rate ingestion for USD, GBP, and CHF by default.
- EUR conversion view for selected tax projections.
- FastAPI endpoint for querying tax data by ISIN and tax/report year.
- Alembic database migrations and automated tests for migrations, ingestion, parsing, FX ingestion, jobs, and API behavior.
- Operational jobs for fetching missing ISINs and refreshing existing ISINs.

What is not implemented yet:

- No finished stakeholder/user UI is present in the repository.
- No authentication or authorization layer is present for the API.
- No scheduler is included; ingestion jobs are run manually or by an external scheduler.
- The public API surface is small: health check plus ETF tax lookup.
- The database contents depend on running ingestion; the repository does not bundle a full production dataset.

## Practical Value

The project turns OeKB ETF tax reporting data into a queryable internal data source. Instead of manually checking OeKB report pages or handling raw JSON each time, a user can ingest a universe of ETF ISINs and query normalized tax data with SQL, a backend endpoint, or downstream reporting tools.

This is useful when a stakeholder needs:

- A repeatable way to collect Austrian ETF tax report data.
- A local data warehouse for Austrian tax-reporting fields.
- An auditable source-to-curated data trail.
- A backend that can feed spreadsheets, dashboards, portfolio tools, or tax workflows.
- A foundation for building a user-facing ETF tax lookup product.

## Use Cases

### 1. ETF Tax Lookup

Use the API or database to answer: "What Austrian tax values are available for this ETF and year?"

Typical user:

- Tax analyst.
- Investment operations team.
- Advisor preparing client reporting.
- Developer integrating ETF tax data into another tool.

Relevant interfaces:

- `GET /etf/{isin}/tax?year={year}`
- `TAXRPT`, `TAXDAT`, `TAXLIN`, `TAXCAT`
- `V1_TAXDATPRE`

### 2. Cost-Basis and Tax Adjustment Review

Use curated tax lines such as `K61` and `K62` to review cost-basis adjustments across investor categories.

Relevant data:

- `K61`: `korrekturbetrag_age_ak`
- `K62`: `korrekturbetrag_aussch_ak`
- `TAXADJ` stores fast-access adjustment records for `K61` as `AKC`.
- `V1_TAXDATPRE` pivots `K61`, `K62`, and `K40` into analysis-ready columns.

### 3. Cross-ETF and Cross-Year Analysis

Use SQL to compare tax values across ISINs, years, OeKB report events, investor categories, and currencies.

Examples:

- Coverage by ETF and year.
- Latest report per ISIN/year.
- Compare `K40`, `K61`, and `K62` across categories.
- Identify reports where a value changed after an OeKB correction.

Relevant data:

- `SECMDA`
- `TAXRPT`
- `TAXDAT`
- `TAXCOR`
- `IMPLOG`

### 4. Audit and Reconciliation

Use the raw and source layers to trace each curated value back to OeKB.

Relevant data:

- `SOURCERPT`: source report metadata.
- `SOURCERAW`: full OeKB detail payload.
- `SOURCEAGE`: parsed source matrix.
- `TAXDAT`: curated normalized values.
- `IMPLOG` and `IMPERR`: ingestion run status and errors.

This makes the project suitable for controlled internal reporting where source traceability matters.

### 5. EUR Reporting and Currency Normalization

Use ECB reference rates to convert selected tax projection values into EUR.

Relevant data:

- `REFEXC`: ECB FX observations.
- `V2_TAXDATEUR`: EUR-converted projection view.

Important practical note: `V2_TAXDATEUR` uses an exact match between OeKB report date and ECB FX date. If the report date falls on a day without an ECB observation, non-EUR converted values return `NULL` until matching rates or fallback logic are added.

## Data Currently Tracked By The Repo

The repository includes an ISIN storage file at `Documentation/isin_storage.csv`. At documentation time it contains:

- `IE00BMTX1Y45`
- `LU1681044993`
- `LU0380865021`
- `LU0496786574`
- `LU2009147757`
- `IE000XZSV718`

These are the default tracked ISINs for the missing-ISIN ingestion workflow. Actual database rows depend on whether ingestion has been run against the local PostgreSQL database.

## How To Position This To A Stakeholder

Position the project as a backend and data foundation, not as a finished end-user application.

Suggested pitch:

"This project gives us a controlled local database of Austrian ETF tax-reporting data sourced from OeKB. It stores the original OeKB payloads for auditability, normalizes the important tax values into reporting tables, adds ECB FX rates for EUR conversion, and exposes the result through SQL and a small API. It can be used immediately for analyst workflows and can also become the backend for a dashboard, portfolio tax module, or client-facing lookup tool."

Best-fit stakeholder offer:

- Start with a pilot ISIN universe.
- Run missing-ISIN ingestion.
- Validate tax outputs for selected ETFs and years.
- Connect the database to Excel, Power BI, PyCharm, DBeaver, or an internal application.
- Extend the API or add a UI if the stakeholder wants non-technical users to use it.

## Main Risks And Gaps

- Source dependency: OeKB and ECB endpoint availability and schema stability matter.
- Tax interpretation: the system stores and structures data; it should not be presented as tax advice by itself.
- API maturity: only one business endpoint is implemented.
- UI maturity: users currently query through SQL, scripts, API, or external tools.
- FX limitation: EUR conversion is exact-date based.
- Operational maturity: scheduled refresh, monitoring, auth, and deployment hardening are future work.
