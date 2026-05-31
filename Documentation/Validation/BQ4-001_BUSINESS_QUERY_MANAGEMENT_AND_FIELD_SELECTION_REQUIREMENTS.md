# BQ4-001 BusinessQuery Management And Field Selection Requirements

Ticket: BQ4-001

Scope: documentation-only requirements definition for BQ4. This ticket does not
change application code, migrations, tests, scripts, environment files, package
configuration, or deployment configuration.

## Purpose

BQ4 refines BusinessQuery so non-technical users can manage saved query rules,
run saved queries directly, keep group setup separate from query execution, and
select one or more tax fields for focused Austrian ETF tax review.

The intended product shape is:

1. `Add New Query` remains the place to enter ISINs, choose query settings,
   run an ad hoc query, and save a reusable rule.
2. `Queries` becomes a saved-query-only page where each saved query can be
   viewed, loaded, edited, and run directly.
3. `Group BusinessQuery` becomes the separate third BusinessQuery subsection
   for creating and naming saved-query groups.
4. Tax field selection becomes a first-class saved-query setting, with one or
   more selected fields persisted and reused.
5. `V2_TAXDATHOMCCY` expands from the current limited `K40`, `K61`, and `K62`
   coverage to every active `TAX_LINES` code, including representative fields
   such as `K11`.

## Current State

BusinessQuery V2 currently uses structured, whitelist-driven query inputs rather
than raw SQL. Users work inside the authenticated application and use
BusinessQuery for owner-scoped query execution, saved-query reuse, dual-currency
results, and CSV export.

The current BusinessQuery navigation contains `Add New Query` and `Queries`:

- `Add New Query` lets users enter or paste ISINs, name a query, choose legal
  entity type, choose subcategory, choose a tax year or all available years,
  enter an amount multiplier, run the query, and save the structured rule.
- `Queries` currently combines saved-query management with saved-query group
  management. It can create groups, filter by group, view grouped saved queries,
  load saved queries into `Add New Query`, and edit saved queries.
- Saved query rows are stored in `BQSAVED` and are owner-scoped by user.
- Saved query groups are stored in `BQGROUP` and are owner-scoped by user.
- A saved query can reference a group through `BQSAVED.BQSGRPIDN`.

The current reporting source is `V2_TAXDATHOMCCY`. It exposes original or home
currency values and EUR values side by side. It also carries identity,
currency, FX rate, and FX date fields such as ISIN, OeKB report id, tax year,
fund currency, `FXRAT`, and `TAXMDT`.

Current BusinessQuery results include dual-currency values:

- original or home currency code;
- original or home currency base value;
- original or home currency calculated value after amount multiplication;
- EUR base value;
- EUR calculated value after amount multiplication;
- FX rate;
- FX date or report date context;
- selected-year missing-data messages when a specific ISIN has no data for the
  chosen year.

CSV export reruns the validated BusinessQuery input and exports the same
dual-currency result shape. The current implementation is useful, but saved
query execution still requires users to load a saved query before running it,
group creation is mixed into the saved-query list, and field coverage is still
too narrow for real tax review.

## Desired BusinessQuery Navigation

BQ4 should present three clear BusinessQuery subsections:

| Subsection | Purpose |
|---|---|
| `Add New Query` | Create, run, and save a structured BusinessQuery rule. |
| `Queries` | Manage and directly run existing saved queries only. |
| `Group BusinessQuery` | Create and list saved-query groups. |

The subsections should use user-facing language. Internal table and view names
such as `BQSAVED`, `BQGROUP`, `V2_TAXDATHOMCCY`, and `TAX_LINES` belong in
technical documentation and tests, not as primary guidance for normal users.

## Add New Query Requirements

`Add New Query` remains the creation and ad hoc execution workflow.

Required behavior:

- Users can enter one or more ISINs.
- Users can choose legal entity type, subcategory, tax year, and amount.
- Users can select one or more tax fields using friendly labels, for example
  `K11 - AG Ertraege` rather than a raw database column name.
- Field selection must be structured and whitelist-based.
- The page must reject invalid or unsupported field selections before execution.
- The default behavior must remain compatible for existing users and existing
  saved queries.
- Saving a query must persist the selected tax fields once the persistence
  ticket is implemented.
- Validation errors must preserve submitted values, including selected fields.

The user should not need to know table names, source view names, SQL, or column
suffixes to create a query.

## Queries Page Requirements

`Queries` should be a saved-query-only page. Its job is to help users find,
run, load, and edit existing saved query rules.

Required behavior:

- List owner-scoped saved queries from `BQSAVED`.
- Show each saved query with a clear name, group context, field selection
  summary, ISIN availability, tax year, legal entity type, subcategory, amount,
  and last updated context where available.
- Keep actions explicit and consistent: `Run`, `Load`, and `Edit`.
- Add a direct `Run` action for each saved query.
- Direct `Run` uses the saved query rule and its saved default ISINs.
- If a saved query has no default ISINs, direct `Run` must not silently run an
  empty query. It should show a friendly message explaining that the user must
  load or edit the query and add ISINs first.
- Preserve owner scoping. A user must not see, load, edit, run, or assign
  another user's `BQSAVED` records.
- Keep group filtering readable, but do not include group creation controls on
  the `Queries` page.
- Empty states should be plain and action-oriented, for example explaining that
  no saved queries exist yet or no saved queries match the selected group.

`Queries` is not a group setup page. It is the execution and management list for
saved query rules.

## Group BusinessQuery Requirements

`Group BusinessQuery` should be the separate third BusinessQuery subsection for
saved-query group management.

Required behavior:

- Create owner-scoped groups in `BQGROUP`.
- List existing groups owned by the authenticated user.
- Use simple group names that are meaningful to the user, such as tax review
  year, client, strategy, or review purpose.
- Handle duplicate group names with a friendly message.
- Do not run queries from this page.
- Do not duplicate group creation controls on `Queries`.
- Preserve owner scoping. A user must not see or assign another user's
  `BQGROUP` records.

This separation keeps setup work out of the saved-query execution list.

## Tax Field Selection Requirements

BusinessQuery must support selecting one or more tax fields.

Required behavior:

- Field choices come from active `TAX_LINES` metadata.
- The system must allow one selected field or multiple selected fields.
- Field selection must use stable line codes such as `K11`, `K40`, `K61`, and
  `K62` internally.
- The UI should display friendly labels that combine the code and a readable
  description.
- The service must validate selected fields through a whitelist rather than
  accepting arbitrary column names.
- Saved queries must persist selected fields so loading, editing, direct
  running, and CSV export can reuse them.
- Existing saved queries must remain usable after the persistence change.
- CSV export must use the current submitted or saved field selection that
  produced the result.

Field selection is a user-level tax review choice. It is not a raw SQL or data
model selection surface.

## V2_TAXDATHOMCCY Coverage Requirements

`V2_TAXDATHOMCCY` must expand to cover every active `TAX_LINES` code, not only
the current limited `K40`, `K61`, and `K62` set.

Required behavior:

- Include dual-currency columns for every active tax line code.
- Include both original or home currency values and EUR values.
- Preserve the existing identity and traceability fields.
- Preserve FX behavior: EUR rows use `FXRAT` of `1`; missing or invalid FX data
  should not destroy available original or home currency values.
- Keep source lineage auditable through the reporting view rather than exposing
  user-controlled SQL.
- Include representative validation for fields beyond the current set, such as
  `K11`.
- Keep the suffix coverage aligned with supported investor categories such as
  `PVM`, `PVO`, `BVM`, `BVO`, `BVJ`, and `STI`.

The expanded view is the foundation for safe field selection. BusinessQuery
should query whitelisted columns from `V2_TAXDATHOMCCY`, not construct arbitrary
source-table queries from user input.

## Low-Technical-User Behavior

BQ4 should make the workflow understandable without technical knowledge.

Required behavior:

- Use page names that match user intent: create a query, run saved queries, and
  manage groups.
- Use field labels with codes and readable names.
- Explain missing default ISINs in direct `Run` as a fixable saved-query setup
  issue.
- Avoid showing stack traces, SQL terms, or raw internal identifiers in normal
  user messages.
- Keep empty states short and specific.
- Preserve current validation messages for invalid ISINs, unsupported choices,
  and missing data where they are already user-friendly.
- Keep in-app documentation concise and focused on workflow.

## Security And Ownership Requirements

The implementation tickets that follow this requirements document must preserve
these boundaries:

- No raw SQL input from users.
- No arbitrary table, view, or column selection from users.
- Owner scoping for `BQSAVED` and `BQGROUP`.
- No cross-user saved-query or group visibility.
- Query execution must remain authenticated.
- Later implementation must keep field selection structured and whitelist-based.

## Follow-Up Ticket Plan

| Ticket | Required outcome |
|---|---|
| BQ4-002 | Expand `V2_TAXDATHOMCCY` so BusinessQuery can support all active tax fields, with representative coverage such as `K11`. |
| BQ4-003 | Extend the BusinessQuery service to support selecting one or more tax fields through validated whitelist input. |
| BQ4-004 | Add tax field multi-select controls to `Add New Query` and route selected fields into execution. |
| BQ4-005 | Persist selected tax fields in `BQSAVED` while preserving existing saved queries. |
| BQ4-006 | Wire saved-query save, load, edit, run, and CSV export flows to selected tax fields. |
| BQ4-007 | Move group creation out of `Queries` and into the separate `Group BusinessQuery` subsection. |
| BQ4-008 | Add direct `Run` action for saved queries on the `Queries` page. |
| BQ4-009 | Tighten the `Queries` page so it displays existing saved queries clearly with `Run`, `Load`, and `Edit` actions. |
| BQ4-010 | Update in-app documentation for field selection, `Queries`, and `Group BusinessQuery`. |
| BQ4-011 | Add final regression and readiness checks for BusinessQuery management changes. |
| BQ4-012 | Create the final BQ4 epic handoff. |

## Acceptance Checklist

- Requirements document exists as a standalone artifact.
- Scope is documentation-only.
- Current state mentions `Add New Query`, `Queries`, `BQSAVED`, `BQGROUP`,
  `V2_TAXDATHOMCCY`, dual-currency results, and CSV export.
- Desired state defines `Queries` as a saved-query-only page with direct `Run`.
- Desired state defines `Group BusinessQuery` as a separate group management
  subsection.
- Field selection supports one or more active `TAX_LINES` codes.
- Expanded view coverage includes active fields beyond `K40`, `K61`, and `K62`,
  including representative `K11`.
- Low-technical-user behavior is stated.
- BQ4-002 through BQ4-012 are listed as follow-up tickets.

## Validation Commands

Run:

```powershell
rg -n "Queries|Group BusinessQuery|Run|field selection|V2_TAXDATHOMCCY|BQSAVED|BQGROUP|TAX_LINES|K11" Documentation/Validation/BQ4-001_BUSINESS_QUERY_MANAGEMENT_AND_FIELD_SELECTION_REQUIREMENTS.md
git diff --check
git status --short --branch --untracked-files=all
```
