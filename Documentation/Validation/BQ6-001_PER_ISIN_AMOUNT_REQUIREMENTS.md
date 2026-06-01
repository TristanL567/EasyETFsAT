# BQ6-001 Per-ISIN Amount Requirements

Ticket: BQ6-001

Scope: documentation and architecture requirements only. This ticket defines
the target behavior for BusinessQuery per-ISIN amount entry, paste mode, `Most
recent common available year`, execution contracts, persistence implications,
CSV output, and rollout sequencing. It does not change application code,
migrations, tests, scripts, environment files, package configuration, database
schema, templates, styles, or BusinessQuery service behavior.

## Objective

BQ6 must turn the BQ5 per-ISIN notional plan into implementation-ready
requirements for a table-first BusinessQuery workflow where each submitted ISIN
can carry its own amount. The design must preserve the existing global amount
fallback so current saved queries, CSV exports, and simple ad hoc runs remain
compatible while enabling portfolio-like inputs.

The required product behavior is:

1. Users can enter ISIN and amount pairs in a compact table.
2. Users can switch to paste mode for common two-column formats and map parsed
   data back into table rows before execution.
3. Users can choose `Most recent common available year`, meaning the latest
   `TAXYEA` available for every submitted ISIN.
4. Execution can carry ordered per-ISIN positions while preserving the existing
   global amount fallback.
5. Saved-query persistence can round-trip per-ISIN positions without breaking
   existing global-amount saved queries.
6. Results and CSV output show the amount actually applied per ISIN.

## Source Context

This document builds on the current BusinessQuery requirements and handoffs:

| Source | Relevant context |
|---|---|
| `Documentation/Validation/BQ-001_SAVED_QUERY_REQUIREMENTS.md` | BusinessQuery is structured and whitelist-driven; saved queries store query name, default ISIN list, legal settings, tax year, amount, notes, owner scope, and rerun behavior. |
| `Documentation/Validation/BQ2-012_BUSINESS_QUERY_V2_FINAL_HANDOFF.md` | BusinessQuery V2 uses `V2_TAXDATHOMCCY`, supports selected tax year or `All available years`, reports selected-year missing data per ISIN, and exports CSV through the same validated execution path. |
| `Documentation/Validation/BQ4-012_BUSINESS_QUERY_MANAGEMENT_FINAL_HANDOFF.md` | Add New Query, Queries, and Group BusinessQuery are separate management surfaces; saved queries can be run, loaded, edited, grouped, and owner-scoped. |
| `Documentation/Validation/BQ5-001_BUSINESS_QUERY_LAYOUT_AND_NOTIONAL_REQUIREMENTS.md` | BQ5 identified the current global amount as a single multiplier and required future workflows to make the active notional source obvious. |
| `Documentation/Validation/BQ5-005_PER_ISIN_NOTIONAL_UX_AND_ARCHITECTURE.md` | BQ5 approved table row entry, paste-friendly input, row validation, duplicate rules, and a future `positions` request shape while preserving global amount fallback. |

## Current BusinessQuery Behavior

BusinessQuery currently uses authenticated, structured form inputs rather than
raw SQL. Users create or run saved query rules from Add New Query, manage saved
queries from Queries, and organize saved queries in Group BusinessQuery.

The current execution model includes:

- One or more submitted ISINs, normalized to uppercase.
- A selected legal entity type and subcategory mapping to whitelisted internal
  categories.
- A selected tax year, or `All available years`.
- A selected set of whitelisted tax fields.
- One global positive numeric amount multiplier.
- Service execution against the whitelisted BusinessQuery reporting view,
  currently represented by `V2_TAXDATHOMCCY` in BQ2/BQ4 handoff language.

The current global amount behavior is simple: the same amount multiplier is
applied to every submitted ISIN and every matching result row. Result rows and
CSV output expose that multiplier so users can audit the calculated values.

Saved queries currently preserve the structured rule, including default ISINs
where present, legal entity settings, selected tax year, selected tax fields,
amount, note, group assignment, owner scope, and timestamps. Loading or running
a saved query uses the same structured BusinessQuery validation and execution
path as a fresh form submission.

For tax years, BusinessQuery already distinguishes a specific selected year
from `All available years`. A specific selected year filters results and can
report per-ISIN missing-year messages. `All available years` runs without a
single-year predicate and returns all matching available tax years.

CSV export reruns the same validated query as the rendered result flow. Empty
exports return headers only, and populated exports include the current stable
identity, field, amount, currency, FX, and calculated-value columns defined by
the implemented BusinessQuery version.

## BQ5 Per-ISIN Notional Plan

BQ5-005 established the direction for multiple ISINs with different notional or
share amounts:

- The primary UI should be a structured table-like editor.
- Each row should contain one ISIN and one amount.
- Row validation should happen near the affected row.
- Blank trailing rows may be ignored, but partial rows are invalid.
- Duplicate ISINs are invalid unless a later implementation explicitly adds a
  combine action.
- Paste mode should accept comma-separated, semicolon-separated,
  tab-separated, and simple whitespace-separated two-column input.
- Parsed paste data should be previewed before replacing or appending rows.
- The backend should support a `positions` shape while keeping legacy `isins`
  plus global `amount` compatible.
- When `positions` is present and nonempty, it is the authoritative source for
  per-ISIN execution.
- When `positions` is absent, the existing global amount fallback applies to
  every submitted ISIN.
- Saved queries need optional ordered position persistence so old global amount
  saved queries still load and execute.

BQ6-001 adopts that BQ5 plan and adds requirements for exact paste behavior,
latest common available tax year semantics, result and CSV amount display, and
rollout tickets.

## Table-First Input Requirements

The primary BusinessQuery amount input must be table-first. The table is the
source of truth for execution after any paste-mode import is accepted.

Required columns:

| Column | Requirement |
|---|---|
| ISIN | Editable text input, trimmed, normalized to uppercase, required for every nonblank row, and validated at row level. |
| Amount | Editable numeric input, required for every row with an ISIN, positive, and validated using the existing BusinessQuery amount semantics unless a later ticket changes the numeric rule. |
| Status | Compact row-level validation state with field-specific messages. |
| Actions | Row add and row remove controls. Optional duplicate/combine controls must not silently alter data. |

Required controls:

- `Add row` inserts a new blank row after the current row or at the end of the
  table.
- `Remove row` removes the selected row, with at least one blank editable row
  remaining when all rows are removed.
- Blank trailing rows are ignored during execution.
- Multiple blank rows are ignored if both ISIN and amount are empty.
- A partially blank row blocks execution.
- Submission is blocked while any entered row is invalid.
- The UI shows the active amount source clearly: per-ISIN table amounts or the
  existing global amount fallback.

Row-level validation requirements:

- ISIN is required for every nonblank row.
- ISIN is normalized to uppercase before validation and execution.
- ISIN validation should use the same structural and checksum standards as the
  current or most recent application ISIN validation path.
- Amount is required for every row with an ISIN.
- Amount must be numeric and greater than zero.
- Decimal behavior must match the existing global amount behavior unless a
  future ticket explicitly introduces different share/notional semantics.
- Duplicate ISINs are invalid by default and should identify all duplicate
  rows with a message such as `Duplicate ISIN; combine amounts or remove one
  row`.
- Validation errors are shown at row level and should also be summarized at the
  form level when they block execution.

The global amount field remains available for compatibility and simple runs.
Implementations may present this as a separate mode, a fallback field, or a
global amount section, but users must not be left guessing whether table
amounts or the global amount will be used.

## Paste Mode Requirements

Paste mode must be an explicit toggle, tab, drawer, or expandable area that
allows bulk entry while keeping the table as the execution source of truth.

Supported paste formats include:

```text
AT0000A18XM4, 5
IE00B4L5Y983, 10
```

```text
AT0000A18XM4; 5
IE00B4L5Y983; 10
```

```text
AT0000A18XM4	5
IE00B4L5Y983	10
```

```text
AT0000A18XM4 5
IE00B4L5Y983 10
```

Parsing requirements:

- Parse comma-separated two-column input such as `ISIN, 5`.
- Parse semicolon-separated two-column input such as `ISIN; 5`.
- Parse tab-separated two-column input copied from spreadsheets.
- Parse simple two-column whitespace input where each nonblank line contains
  exactly one ISIN token and one amount token.
- Trim surrounding whitespace from each parsed value.
- Normalize parsed ISINs to uppercase.
- Preserve source line order.
- Preserve numeric text exactly enough to validate and submit the same amount
  the user supplied.
- Reject lines with missing ISIN, missing amount, too many columns, invalid
  ISIN, invalid amount, duplicate ISIN, or mixed delimiters that cannot be
  interpreted deterministically.
- Report parse errors by line number.

Paste mode must not execute directly from the text area. Parsed paste input
maps back into table rows before execution.

Required paste-to-table flow:

1. User opens paste mode and enters or pastes text.
2. User chooses `Parse` or equivalent.
3. The system parses every nonblank line into candidate rows with line numbers.
4. The system shows a preview containing valid and invalid parsed rows.
5. The user chooses `Replace rows` or `Append rows`.
6. Accepted rows are materialized into the table editor in parsed order.
7. Table validation runs on the resulting table.
8. Execution reads from the table rows only.

If the preview contains invalid rows, the implementation may either block
`Replace rows` and `Append rows` until errors are fixed, or allow importing
only after the user explicitly removes invalid preview rows. It must not
silently drop invalid input.

## Tax Year Semantics

BQ6 adds a new tax-year selection option:

```text
Most recent common available year
```

Definition:

`Most recent common available year` means the latest `TAXYEA` value that is
available for every submitted ISIN after ISIN normalization and duplicate
validation.

Required semantics:

- Determine available years per submitted ISIN from the same BusinessQuery data
  source used for execution, not from a stale hard-coded list.
- Exclude null `TAXYEA` values from common-year selection unless a later ticket
  explicitly defines null-year fallback for BusinessQuery common-year logic.
- Intersect the non-null `TAXYEA` sets for all submitted ISINs.
- Select the maximum year from that intersection.
- Execute the query as a specific selected tax year using that resolved year.
- Display the resolved year in the result context.
- CSV export must use the same resolved year as the rendered result flow.

Example:

| ISIN | Available `TAXYEA` values |
|---|---|
| AT0000A18XM4 | 2023, 2024, 2025 |
| IE00B4L5Y983 | 2022, 2024, 2025 |
| LU1681045370 | 2021, 2024 |

The common set is `2024`, so the resolved year is `2024`.

### No Common Tax Year

When no common tax year exists, execution must not fall back silently to `All
available years`, a partial latest year, or each ISIN's individual latest year.

Required no-common behavior:

- Block execution before querying result rows for calculated output, or return
  an empty structured result with a blocking validation-style message.
- Show a clear message such as `No common tax year is available for all
  submitted ISINs. Choose a specific year, All available years, or adjust the
  ISIN list.`
- Include enough context for troubleshooting, such as each submitted ISIN and
  its available year set when feasible.
- CSV export for a no-common submission should not produce misleading
  calculated rows. It may return headers only with the same user-visible error
  in the web flow, or block export with the same validation message.
- Saved queries using this option should remain saved even if the current data
  later has no common tax year; the failure is data-dependent at run time.

## Backend Request Shape

The future BusinessQuery input contract must support ordered per-ISIN
positions while keeping existing callers compatible.

Recommended logical request shape:

```json
{
  "query_name": "Example portfolio",
  "isins": ["AT0000A18XM4", "IE00B4L5Y983"],
  "amount": 1,
  "positions": [
    { "isin": "AT0000A18XM4", "amount": 5 },
    { "isin": "IE00B4L5Y983", "amount": 10 }
  ],
  "tax_year_filter": "most_recent_common_available_year"
}
```

Compatibility rules:

- Existing callers may continue sending `isins` plus a global `amount`.
- New callers may send `positions` with one `amount` per ISIN.
- If `positions` is present and nonempty, `positions` is authoritative for
  submitted ISIN identity, order, and per-ISIN amount.
- If `positions` is absent or empty, the backend applies the existing global
  amount fallback to every submitted ISIN.
- If both `positions` and global `amount` are present, the global amount is
  fallback metadata only and must not override explicit position amounts.
- If both `positions` and `isins` are present, their normalized ISIN sets must
  match unless a later ticket defines reconciliation behavior.
- Position order should be preserved for validation summaries, result grouping,
  saved-query reload, and user-facing displays where practical.
- Backend validation must reject invalid ISINs, duplicate position ISINs,
  missing amounts, zero amounts, negative amounts, and nonnumeric amounts.

The position shape should remain small and semantic:

| Field | Requirement |
|---|---|
| `isin` | Normalized ISIN string. |
| `amount` | Positive numeric amount using existing BusinessQuery amount semantics. |

Implementation tickets may add display-only metadata, but execution should not
depend on client-provided fund names, descriptions, or row labels.

## Execution Requirements

Execution must calculate each row using the amount applied to that row's ISIN.

Required behavior:

- When explicit positions are supplied, each result row for an ISIN uses that
  ISIN's position amount.
- When positions are absent, each result row uses the global amount fallback.
- Null base values remain null after amount multiplication.
- Legal entity, subcategory, selected tax fields, and tax-year filters remain
  structured and whitelisted.
- `Most recent common available year` resolves before result execution and
  then behaves like a specific selected year.
- Missing selected-year behavior remains per ISIN for ordinary specific-year
  runs.
- No-common-year behavior for `Most recent common available year` follows the
  blocking rule above.
- Execution must not accept raw SQL, arbitrary table names, arbitrary source
  columns, or client-provided tax-field column names.

Recommended internal normalization:

1. Normalize all user-entered table rows into ordered `positions`.
2. Validate `positions`.
3. Resolve the tax-year filter, including `Most recent common available year`.
4. Build the existing structured BusinessQuery input using positions when
   present or global amount fallback when absent.
5. Execute through the same whitelisted service path used by rendered results
   and CSV export.

## Saved-Query Persistence And Migration

Saved-query persistence must preserve backward compatibility and support
round-tripping ordered positions.

Current saved queries with only global amount data remain valid. Loading an old
saved query should continue to populate the global amount fallback and the
default ISIN list. If the UI is in per-ISIN table mode, it may expand those
ISINs into rows using the global amount, but the system must not rewrite the
saved query into explicit positions unless the user saves after making an
intentional change.

New saved queries created from table mode should persist:

| Concept | Requirement |
|---|---|
| Ordered positions | Preserve each ISIN and amount in user-entered order. |
| Global amount fallback | Preserve when used for legacy/simple runs or as compatibility metadata. |
| Tax-year filter | Persist specific year, `All available years`, or `Most recent common available year`. |
| Existing fields | Preserve query name, group, owner scope, legal entity, subcategory, selected tax fields, note, created timestamp, and updated timestamp behavior. |

Migration requirements:

- A migration is required if the current saved-query schema cannot store
  ordered per-ISIN positions and the new tax-year filter value without loss.
- The migration must be additive and backward-compatible.
- Existing saved queries must not be deleted, renamed, or rewritten as part of
  the migration unless a later ticket explicitly defines a safe data migration.
- Existing global amount saved queries must continue to load, run, edit, and
  export after migration.
- Owner scoping must apply to persisted position data exactly as it applies to
  saved queries and groups.
- Saved-query uniqueness rules must remain unchanged unless a later ticket
  explicitly changes them.

Potential storage approaches include a JSON column for ordered positions or a
child table keyed by saved-query id and row order. The implementation ticket
must choose based on the current schema, migration style, query needs, and
auditability requirements.

## Result And CSV Behavior

Rendered results must show the amount actually applied per ISIN.

Required result behavior:

- Result summaries identify whether the run used per-ISIN table amounts or the
  global amount fallback.
- Each result row includes the applied amount for that ISIN.
- The applied amount column should be labelled consistently with the current
  amount multiplier language unless product copy changes the term.
- Grouped or summarized result displays must not imply one global amount when
  per-ISIN amounts were used.
- The resolved tax year must be shown when `Most recent common available year`
  is selected.
- No-common-year messages must be visible without requiring CSV export.

CSV behavior:

- CSV export uses the same validated input, positions, tax-year resolution, and
  execution path as rendered results.
- CSV output includes the applied amount per ISIN for every calculated result
  row.
- For legacy global amount runs, the applied amount column contains the global
  amount value for every row.
- For explicit position runs, the applied amount column contains the matching
  position amount for that row's ISIN.
- CSV output should include enough tax-year context to distinguish a literal
  selected year from `Most recent common available year` resolved to that year,
  either through existing result context columns or an added filter/context
  column in a later implementation ticket.
- Empty result CSV behavior remains headers-only unless a validation-blocking
  condition such as no common tax year prevents export.

The exact CSV column name should be finalized during implementation. Preferred
names are `applied_amount` or the existing `amount_multiplier` if the
implementation keeps the current terminology and value semantics.

## Rollout Requirements

Rollout should be split so UI, parser, backend contract, persistence, execution,
CSV, and documentation changes can be reviewed independently.

Required rollout principles:

- Preserve legacy global amount behavior until per-ISIN execution is fully
  implemented and verified.
- Gate service execution changes behind validation tests before enabling UI
  paths that submit positions.
- Add saved-query migration only in the dedicated migration ticket.
- Keep CSV changes paired with rendered result behavior so exports remain
  explainable.
- Update in-app documentation only after the implemented behavior is stable.
- Include regression coverage for old saved queries and old global amount
  submissions in every implementation phase that touches those paths.

## Non-Goals

This ticket does not implement:

- Code changes.
- Alembic migrations.
- GUI implementation.
- BusinessQuery service implementation.
- Saved-query model changes.
- CSV export changes.
- Parser code.
- Test changes.
- Deployment changes.
- Raw SQL support.
- New tax-field semantics.
- Share-versus-currency notional redesign beyond the existing amount semantics.

## Follow-Up Tickets

| Ticket | Scope |
|---|---|
| BQ6-002 | Add BusinessQuery backend position input support while preserving global amount fallback. |
| BQ6-003 | Implement latest common available tax-year resolution for BusinessQuery inputs. |
| BQ6-004 | Replace the Add New Query ISIN textarea with a table-first per-ISIN amount input and paste-mode toggle. |
| BQ6-005 | Wire BusinessQuery web execution and CSV export to per-ISIN positions and latest-common-year selection. |
| BQ6-006 | Add saved-query persistence for ordered per-ISIN positions. |
| BQ6-007 | Wire saved-query save, load, edit, direct run, and export flows to persisted positions. |
| BQ6-008 | Update BusinessQuery documentation and user guidance. |
| BQ6-009 | Run focused regression readiness. |
| BQ6-010 | Create final BQ6 handoff. |

## Acceptance Criteria

- Current BusinessQuery behavior is documented.
- The BQ5 per-ISIN notional plan is referenced and adopted.
- Table-first GUI input requirements define ISIN column, amount column, row
  add/remove controls, and row-level validation.
- Paste mode is defined with supported `ISIN, 5`, `ISIN; 5`, tab-separated,
  and simple two-column whitespace input.
- Paste input maps back into table rows before execution.
- `Most recent common available year` is defined as the latest `TAXYEA`
  available for every submitted ISIN.
- No-common-tax-year behavior is explicit and does not silently fall back.
- Backend `positions` shape and global amount fallback compatibility are
  defined.
- Saved-query persistence implications and migration requirements are defined.
- Result and CSV behavior include showing the applied amount per ISIN.
- BQ6-002 through BQ6-010 follow-up tickets are listed.
- No files outside this BQ6-001 document are changed.

## Validation Commands

Run these commands for BQ6-001:

```powershell
rg -n "table|paste|positions|latest common|available year|no common|global amount|saved-query|migration|CSV" Documentation/Validation/BQ6-001_PER_ISIN_AMOUNT_REQUIREMENTS.md
git diff --check
git status --short --branch --untracked-files=all
```

## Risks And Open Questions

- The exact current saved-query storage shape should be inspected during
  BQ6-008 before choosing JSON storage or a child table for ordered positions.
- The implementation must confirm whether decimal amounts are fully supported
  by the existing global amount path before accepting decimal per-ISIN amounts.
- The data source for available-year discovery should be the same reporting
  view or service source used by BusinessQuery execution to avoid mismatches.
- Product copy should confirm whether the visible column label remains
  `amount multiplier` or changes to `applied amount`.
- If future requirements distinguish shares from currency notional, that should
  be a separate compatibility ticket because BQ6 keeps the current amount
  semantics.
