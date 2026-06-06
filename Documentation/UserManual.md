# EasyETFsAT User Manual

EasyETFsAT is a web portal for working with Austrian ETF tax data. It helps you
find available fund records, run structured BusinessQuery calculations, export
results, and request data updates.

EasyETFsAT is not tax advice. It uses data loaded from OeKB and ECB sources, and
users should verify tax treatment independently before relying on any result.

## Login And Navigation

Open the deployed EasyETFsAT page and choose `Login`. Enter the username and
password provided by the operator. After login, the portal shows the signed-in
user and a `Log out` button.

The left navigation contains:

- `BusinessQuery`: create and manage tax queries.
- `Search`: find available fund data.
- `Update Data`: request an OeKB data refresh for one or more ISINs.
- `Documentation`: quick in-app help and tax field descriptions.
- `Settings`: browser preferences, especially the BusinessQuery input view.

Use `Log out` when finished.

## BusinessQuery: Add New Query

Use `BusinessQuery` -> `Add New Query` to run a new tax query.

Basic steps:

1. Enter a custom query name.
2. Enter ISINs and amounts.
3. Choose the legal entity type and subcategory.
4. Choose the tax year.
5. Select one or more tax fields.
6. Run the query, save the query rule, or export CSV after results are shown.

Legal entity choices are:

- `natural person`: choose `PA mit Option`, `PA ohne Option`, or all private
  investor categories.
- `business`: choose `BV mit Option`, `BV ohne Option`, `BV jur. Person`, or all
  business categories.
- `Stiftung`: uses the Stiftung category.

## ISIN And Amount Input

BusinessQuery supports per-ISIN amounts. The amount multiplies the selected tax
values for that ISIN.

`Table view` shows editable ISIN and amount rows. Use `+ Add row` to add another
row. Keep one ISIN per row.

`Box view` lets you paste rows such as:

```text
IE000XZSV718, 5
IE00BMTX1Y45, 10000
```

Separate the ISIN and amount with a comma, space, semicolon, or tab. The portal
checks the rows before running the query and keeps invalid rows visible so they
can be corrected.

Older saved queries may use a global amount fallback if no per-ISIN amounts were
stored.

## Tax Year Selection

The tax year selector supports:

- `All available years`: searches every loaded tax year.
- `Latest common available year`: uses the newest tax year available for all
  submitted ISINs.
- A specific year: returns data for that year only.

If no shared year exists for `Latest common available year`, the portal shows a
no-common-year message. If a specific year is selected and an ISIN has no data
for that year, the portal shows:

```text
Data for ISIN {ISIN} is not available for the selected year.
```

This message is informational. Other matching ISINs can still return results.

## Tax Field Selection

Select the tax fields you want to include in the query. The portal shows the
field code, German label, and short usage text where available. The in-app
`Documentation` page contains a longer tax field reference based on OeKB field
documentation.

## Reading Results

BusinessQuery results show the selected ISINs, legal category, tax year, tax
field, and amount source.

Result rows include:

- original/home currency: the currency of the fund or source tax data.
- original/home base value: the source tax value before applying the amount.
- original/home calculated value: the home-currency value multiplied by the
  amount.
- EUR base value: the converted EUR value before applying the amount.
- EUR calculated value: the EUR value multiplied by the amount.
- applied amount: the amount used for that ISIN.
- FX rate/date: the ECB exchange rate and date used for EUR conversion.
- OeKB release date: the report date associated with the source tax data.

If an ECB rate is unavailable for the needed date and currency, EUR values may
be blank while original/home currency values remain visible.

## CSV Export

Use `Export CSV` after running a query to download the current result set. The
CSV uses the same submitted form values, including ISINs, amounts, tax fields,
tax year, and legal category.

The CSV includes original/home currency values, EUR values, applied amount, FX
rate, and FX date. It is intended for review and downstream analysis; verify the
data before using it for tax decisions.

## BusinessQuery: Queries

Use `BusinessQuery` -> `Queries` to manage saved query rules.

From this page you can:

- view saved queries grouped by group name or under `Ungrouped`;
- run a saved query directly;
- load a saved query into `Add New Query`;
- edit saved query settings.

Saved queries belong to the signed-in user. Other users' saved queries are not
shown.

## BusinessQuery: Group BusinessQuery

Use `BusinessQuery` -> `Group BusinessQuery` to create named groups for saved
queries. Groups help organize reusable rules, for example by review period,
portfolio, or client workflow.

After creating a group, assign a saved query to it while editing that saved
query.

## Search

Use `Search` to find available fund data by ISIN or fund/security name. Results
can show:

- ISIN;
- fund or security name;
- currency;
- available tax years;
- report count;
- a link to use the ISIN in BusinessQuery.

If no fund data is loaded yet, the page shows an empty database message.

## Update Data

Use `Update Data` to request data updates for one or more ISINs.

Steps:

1. Enter one or more ISINs.
2. Choose `Update ISIN`.
3. Review the job status table.

Valid submissions are queued and processing starts automatically when the web
service can run the background task. The job table is the source of truth:

- `queued`: accepted and waiting for or starting processing.
- `running`: currently being processed.
- `success`: finished successfully.
- `failed`: processing failed; review the error detail.
- `skipped`: an active job for that ISIN already exists.
- `cancelled`: reserved for jobs stopped by a future operator action.

Successful updates make data available to `Search` and `BusinessQuery` after
the database has been updated.

## Documentation Page

Use `Documentation` for short in-app help. It explains legal categories,
BusinessQuery result columns, CSV exports, Search, tax fields, and common
selected-year messages.

## Settings

Use `Settings` -> `BusinessQuery input view` to choose the default Add New Query
input style for this browser:

- `Box view`: paste ISIN and amount rows.
- `Table view`: enter ISIN and amount rows in a table.

The choice is stored in a browser cookie. It does not change saved query rules
or database data.

## Common Messages And What To Do

`Enter at least one ISIN.`
: Add at least one ISIN before submitting.

`Enter ISIN-like values` or `Enter valid ISINs.`
: Check spelling, remove extra characters, and use uppercase ISINs.

`Enter a positive amount.`
: Enter a numeric amount greater than zero.

`Remove duplicate ISIN rows.`
: Keep each ISIN once, or combine amounts before submitting.

`Choose at least one tax field.`
: Select one or more tax fields before running the query.

`Data for ISIN {ISIN} is not available for the selected year.`
: Choose another year, use `All available years`, or request an update through
  `Update Data`.

`No tax rows matched the submitted ISINs.`
: Check the ISINs, legal category, tax fields, and tax year. The data may not be
  loaded yet.

`A saved query with this name already exists.`
: Use a different query name, or edit the existing saved query.

`A group with this name already exists.`
: Use a different group name.

`Skipped: active update job already exists.`
: Wait for the existing update job to finish, then check Search or
  BusinessQuery again.

`failed` update job status.
: Review the error detail. If needed, ask the operator to rerun or investigate
  the update.

## Known Limitations

- EasyETFsAT is not tax advice.
- Data availability depends on OeKB source data, ECB FX rates, and database
  updates.
- Missing or stale source data can lead to empty Search results or empty
  BusinessQuery results.
- Missing ECB FX data can leave EUR-converted values blank for non-EUR funds.
- Saved queries are user-specific and are not shared across users.
- Users should verify tax treatment independently before using results in
  filings, client work, or investment decisions.
