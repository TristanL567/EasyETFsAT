# BQ5-001 BusinessQuery Layout And Notional Requirements

Ticket: BQ5-001

Scope: documentation only. This ticket defines requirements for future
BusinessQuery layout, release-date display, saved/group table formatting,
documentation width, and per-ISIN notional behavior. It does not change
application code, migrations, tests, scripts, environment files, package
configuration, database schema, BusinessQuery calculations, CSV export, or
per-ISIN notional implementation.

## Objective

BQ5 must make BusinessQuery easier to read for tax-review users before
implementation begins. The next implementation tickets should keep the existing
structured BusinessQuery model, but improve how results, saved queries, group
pages, and documentation communicate source context and user-entered notional
amounts.

The target product behavior is:

1. Result descriptions show the OeKB release date sourced from `TAXRPT` /
   `TAXMDT`.
2. Result tables use a wider layout so dual-currency rows remain scannable.
3. The Documentation page uses a wider documentation layout for long
   BusinessQuery and validation text.
4. Display-only FX rate values are rounded to 4 decimals.
5. Saved query and group tables show amounts with 2 decimals and updated
   timestamps as date plus hour/minute.
6. Future multi-ISIN workflows allow different notional values per ISIN
   without weakening the existing structured validation boundary.

## Current BusinessQuery Layout

BusinessQuery currently uses authenticated, structured inputs rather than raw
SQL. Users work with saved query rules, groups, tax-year selection, legal entity
settings, field selection, amount multiplication, dual-currency result rows,
and CSV export.

The current BusinessQuery area is split into pages established by the BQ4
requirements:

| Page | Current purpose |
|---|---|
| `Add New Query` | Create, run, and save a structured BusinessQuery rule. |
| `Queries` | List, filter, load, edit, and run saved BusinessQuery rules. |
| `Group BusinessQuery` | Create and list saved-query groups. |
| `Documentation` | Explain BusinessQuery behavior and validation context to users. |

The current result layout is row-oriented. A submitted query expands matching
source data into tax-field and legal-category rows. Result rows include the
submitted ISIN, tax year, OeKB report identifier where available, selected tax
field, legal entity category, original/home currency values, EUR values, amount
multiplier, FX rate, and FX/report date context.

The current saved query layout is management-oriented. Saved rows should expose
the saved query name, group context, default ISINs where present, legal entity
settings, selected tax year, selected tax fields, amount, updated context, and
actions such as `Run`, `Load`, and `Edit`.

The current group page layout is setup-oriented. Group rows should expose group
name, ownership context implied by the authenticated session, associated saved
query context where shown, updated context where available, and group-management
actions. Group creation remains separate from direct BusinessQuery execution.

## Result Description Release Date

BusinessQuery result descriptions must show the OeKB release date for each
result context when the source data provides it.

The source release date is the reporting date already carried through the
tax-reporting lineage from `TAXRPT` as `TAXMDT`. Documentation and
implementation notes may refer to this as the OeKB release date, report release
date, or source report date, but user-facing result descriptions should use one
clear label.

Required display behavior:

- Show `OeKB release date` in the result description whenever `TAXMDT` is
  available for the result row or result group.
- Use the `TAXMDT` date associated with the source OeKB report id, tax year,
  and ISIN shown in the result row.
- If multiple rows in the visible result set have different `TAXMDT` values,
  do not collapse them into a misleading single global date. Show the release
  date at row level, grouped-result level, or in a per-ISIN/per-report
  description.
- If `TAXMDT` is unavailable, show an explicit empty value or omit only the
  date value; do not fabricate a release date from the current system time.
- Preserve the existing distinction between source report date/release context
  and FX rate value. A release date is not an FX rate.

Recommended user-facing wording:

```text
OeKB release date: YYYY-MM-DD
```

Where a result description summarizes one ISIN and one tax year, the preferred
shape is:

```text
ISIN AT0000000000, tax year 2025, OeKB release date: 2026-01-31
```

## Wider Result Table Requirements

BusinessQuery result tables need more horizontal room than compact management
tables because they combine identity fields, source traceability, original/home
currency values, EUR values, amount multiplier, FX rate, tax field labels, and
legal entity categories.

Required behavior:

- Use a wider result-table layout than the default narrow content column.
- Keep the result table readable on desktop without forcing unnecessary line
  wrapping in common columns such as ISIN, tax year, tax field, currency, FX
  rate, OeKB report id, and OeKB release date.
- Preserve horizontal scrolling where the viewport is too narrow for the full
  result width.
- Keep numeric columns right-aligned.
- Keep identity and description columns left-aligned.
- Keep table headers visible and short enough to scan.
- Avoid truncating values required for auditability, including ISIN, report id,
  `TAXMDT`, currency, and selected tax field code.
- Do not remove columns from CSV export as part of a display-width change.

This is a visual layout requirement only. Wider display must not change result
row grain, filtering, calculations, or CSV data unless a later ticket
explicitly changes those contracts.

## Wider Documentation Layout Requirements

The in-app Documentation page must support wider documentation content than a
compact form page. BusinessQuery documentation includes long field names,
lineage explanations, table examples, and validation details that are difficult
to read in a narrow column.

Required behavior:

- Use a wider Documentation layout than ordinary form pages.
- Keep prose line length readable; widening the page should provide room for
  tables and examples, not create overly long paragraph lines.
- Allow wide Markdown or HTML tables to use horizontal scrolling on smaller
  screens.
- Keep headings, paragraphs, lists, and tables visually aligned with the
  portal's existing design language.
- Do not put documentation content inside nested card layouts.
- Do not require users to download a file to read primary BusinessQuery
  documentation.

The Documentation page should remain a user-facing explanation surface, not an
implementation log.

## Display Formatting Requirements

Formatting changes in BQ5 are display-only unless a later implementation ticket
explicitly changes computation or export behavior.

### FX Rate Formatting

FX rate values shown in the BusinessQuery UI must be rounded to 4 decimals for
display. This applies to visible result tables and result descriptions that show
an FX rate.

Required behavior:

- Display `FX rate` values with 4 decimals.
- Preserve full precision for computation.
- Preserve full precision in CSV export unless a later ticket explicitly
  changes CSV semantics.
- Do not round the stored `FXRAT` value or service-layer numeric value solely
  for display.
- Handle null FX rates with the existing empty/null display convention.

Example:

| Source value | Display value |
|---:|---:|
| `1` | `1.0000` |
| `0.923456789` | `0.9235` |
| `123.4` | `123.4000` |

### Saved And Group Table Formatting

Saved-query and group management tables should optimize for scanning. Amounts
and timestamps should be consistent and compact.

Required behavior:

- Show saved-query amount values with 2 decimals.
- Show group-related amount summaries with 2 decimals when an amount is shown.
- Show updated timestamps as date plus hour/minute.
- Do not show seconds or milliseconds in saved-query or group management
  tables.
- Use the application's existing timezone convention. If no explicit timezone
  convention exists in implementation, preserve the current displayed timezone
  source and only change precision.
- Preserve full timestamp precision in persistence.

Recommended timestamp shape:

```text
YYYY-MM-DD HH:MM
```

This satisfies the required date and hour/minute display while avoiding seconds
and milliseconds.

## Multiple ISIN And Per-ISIN Notional Requirements

The current BusinessQuery amount is a single multiplier applied to all submitted
ISINs. That behavior remains valid for existing saved queries and simple ad hoc
runs. BQ5 defines the future visual design and architecture plan for multiple
ISINs with different notional values.

### Visual Design

Future multi-ISIN input should let a user assign a notional amount per ISIN
without making the simple one-amount workflow harder.

Required visual behavior:

- Preserve a simple global amount input for users who want one notional amount
  applied to every ISIN.
- Add a clear per-ISIN notional editing mode for advanced runs.
- Show one editable row per ISIN when per-ISIN notional mode is enabled.
- Each per-ISIN row should show ISIN, optional display name or fund context
  where available, notional amount, and row-level validation state.
- Make the active notional source obvious: global amount or per-ISIN amount.
- Show result rows with the notional amount actually used for that ISIN.
- If a submitted ISIN has no explicit per-ISIN notional, use a defined fallback
  rule rather than silently calculating with an ambiguous amount.
- Keep per-ISIN validation messages close to the affected ISIN row.

Recommended fallback rule for implementation:

1. Use the per-ISIN notional when supplied for the ISIN.
2. Otherwise use the global amount if present.
3. Otherwise reject execution with a clear validation message.

### Future Architecture Plan

Per-ISIN notionals should be modeled as structured input, not as free-form SQL
or a presentation-only override.

Required architecture behavior for later implementation tickets:

- Extend the BusinessQuery input contract to carry a mapping from normalized
  ISIN to notional amount.
- Keep the existing global amount field for backward compatibility.
- Normalize and validate ISIN keys before matching them to notional values.
- Validate each notional as a positive numeric amount using the same numeric
  standards as the existing amount multiplier unless a later ticket defines a
  broader rule.
- Apply the selected notional in the service calculation for the matching ISIN
  only.
- Persist per-ISIN notionals in saved queries only after a dedicated schema and
  compatibility ticket defines the storage contract.
- Include per-ISIN notional values in CSV only after a later ticket explicitly
  updates CSV semantics.
- Preserve owner scoping for saved-query notional data.
- Preserve existing saved queries by treating missing per-ISIN notional data as
  the legacy global amount behavior.

Non-goals for BQ5-001:

- No database schema design is finalized here.
- No per-ISIN notional persistence is implemented here.
- No calculation behavior changes in this documentation ticket.
- No CSV export changes in this documentation ticket.

## Follow-Up Tickets

The BQ5 implementation sequence should be split into focused tickets:

| Ticket | Purpose |
|---|---|
| BQ5-002 | Implement BusinessQuery result description changes, including OeKB release date from `TAXRPT` / `TAXMDT`. |
| BQ5-003 | Implement wider result-table layout and wider Documentation layout. |
| BQ5-004 | Implement display-only formatting for FX rate 4 decimals, saved/group amount 2 decimals, and updated date/hour/minute timestamps. |
| BQ5-005 | Document the UX and architecture plan for multiple ISINs with different notional values only. No implementation is included, and future implementation tickets require a human approval gate. |

## Acceptance Checklist

- This document is standalone and readable without opening implementation
  files.
- It mentions `TAXMDT` and the OeKB release date source requirement.
- It defines wider result-table and wider documentation layout expectations.
- It defines display-only FX rate rounding to 4 decimals while preserving full
  precision for computation and CSV unless explicitly changed later.
- It defines saved/group table display formatting for amount values with 2
  decimals and updated timestamps as date plus hour/minute.
- It defines visual design and future architecture expectations for multiple
  ISIN workflows with different per-ISIN notional values.
- It lists BQ5-002, BQ5-003, BQ5-004, and BQ5-005 as follow-up tickets.
- It makes no code, migration, test, script, environment, package, or
  deployment changes.
