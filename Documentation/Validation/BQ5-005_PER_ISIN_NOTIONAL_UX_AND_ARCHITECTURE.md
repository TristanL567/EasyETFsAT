# BQ5-005: Per-ISIN Notional UX and Architecture Plan

## Scope

This plan defines the approved UX and architecture direction for BusinessQuery inputs that support multiple ISINs with different notional values, such as 10 shares of one ISIN, 5 shares of another, and 3 shares of a third.

This is a planning document only. It does not authorize code changes, schema changes, GUI implementation, or BusinessQuery service implementation.

## UX Goal

BusinessQuery should let a user express a basket of instruments with a separate amount for each ISIN while keeping the existing simple single-amount workflow intact.

The input should feel like a compact portfolio editor rather than a free-form technical field. Users should be able to:

- Add, remove, and scan rows quickly.
- Enter one ISIN and one amount per row.
- Paste a list from a spreadsheet, email, broker export, or notes.
- See exactly which rows are valid before submitting.
- Keep using the existing global amount when every ISIN should use the same notional.

## Primary Input Design

### Structured Row-Entry Mode

The primary mode should be a table-like input with stable rows:

| Column | Behavior |
| --- | --- |
| ISIN | Uppercase text input with immediate normalization and validation. |
| Amount | Numeric input for share or notional quantity, using the existing amount semantics unless future tickets explicitly split shares from currency notional. |
| Status | Compact validation icon and message for row-level issues. |
| Actions | Add row, remove row, and optional duplicate resolution controls. |

Recommended row examples:

| ISIN | Amount |
| --- | ---: |
| AT0000A18XM4 | 10 |
| IE00B4L5Y983 | 5 |
| LU1681045370 | 3 |

Visual behavior:

- Valid rows should show a quiet success state after validation.
- Invalid rows should be highlighted at the field level, not only in a form-level banner.
- Blank trailing rows should remain visually neutral.
- The submit action should remain disabled or blocked while required entered rows are invalid.
- A summary above or below the rows should show the number of valid instruments and the total entered amount where that total is meaningful.

### Paste-Friendly Mode

Paste mode should support users who already have data in rows or columns. It can be a dedicated tab, drawer, or expandable paste area next to the structured editor.

Accepted paste formats should include:

```text
AT0000A18XM4,10
IE00B4L5Y983,5
LU1681045370,3
```

```text
AT0000A18XM4	10
IE00B4L5Y983	5
LU1681045370	3
```

```text
AT0000A18XM4 10
IE00B4L5Y983 5
LU1681045370 3
```

Paste behavior:

- Parse comma-separated, tab-separated, semicolon-separated, and simple whitespace-separated two-column input.
- Normalize ISINs to uppercase and trim surrounding whitespace.
- Preserve row order after parsing.
- Show a preview before replacing or appending to structured rows.
- Offer explicit actions: `Append rows`, `Replace rows`, and `Cancel`.
- Report parse errors by pasted line number and do not silently drop mixed valid/invalid input.

## Validation Behavior

### ISIN

- Required for every nonblank row.
- Trim whitespace and normalize to uppercase.
- Validate against the ISIN shape: two letters, nine alphanumeric characters, and one check digit.
- Prefer full ISIN check-digit validation in the implementation ticket.
- Show clear row-level messages, for example: `Invalid ISIN format` or `Invalid ISIN check digit`.

### Amount

- Required for every row with an ISIN.
- Must be numeric.
- Must be greater than zero.
- Should reject zero, negative values, blank values, and nonnumeric text.
- Decimal handling should follow existing BusinessQuery amount semantics. If current execution only supports whole shares, decimals must be rejected with a message such as `Amount must be a whole number`.
- Formatting should not change the submitted value unexpectedly. Display formatting may add grouping only after the field loses focus.

### Duplicates

- Duplicate ISINs are invalid by default because they make row-level intent ambiguous.
- The UI should identify all duplicate rows and show a message such as `Duplicate ISIN; combine amounts or remove one row`.
- A future convenience action may combine duplicate rows by summing amounts, but automatic merging should not happen silently.

### Blank Rows

- A completely blank trailing row is allowed and ignored.
- Multiple blank rows should be ignored if no other field in the row is populated.
- A partially blank row is invalid. For example, an ISIN without an amount or an amount without an ISIN should block submission.

### Mixed Valid and Invalid Input

- Mixed valid/invalid structured rows should keep valid rows visible and mark only invalid rows.
- Mixed valid/invalid paste input should show a preview containing all parsed lines, including invalid lines with line-level errors.
- Submission should be blocked until invalid entered rows are fixed or removed.
- The UI should not discard valid rows just because another row is invalid.

## Backend Architecture Direction

The future request shape should support per-ISIN amounts while preserving the existing global amount fallback.

Recommended logical shape:

```json
{
  "isins": ["AT0000A18XM4", "IE00B4L5Y983", "LU1681045370"],
  "amount": 1,
  "positions": [
    { "isin": "AT0000A18XM4", "amount": 10 },
    { "isin": "IE00B4L5Y983", "amount": 5 },
    { "isin": "LU1681045370", "amount": 3 }
  ]
}
```

Compatibility rules:

- Existing callers may continue sending `isins` plus a global `amount`.
- New callers may send `positions` with one `amount` per ISIN.
- If `positions` is present and nonempty, it should be the authoritative source for per-ISIN execution.
- If `positions` is absent, the backend should apply the existing global amount fallback to every ISIN.
- If both `positions` and global `amount` are present, global `amount` should be treated as fallback metadata only, not as an override for position amounts.
- Backend validation should reject conflicting payloads where `positions` and `isins` contain different ISIN sets unless a future implementation explicitly defines reconciliation behavior.

This design keeps old saved queries and integrations compatible while giving the service a clear path to per-ISIN notional execution.

## Saved-Query Persistence

Saved queries currently need to preserve the existing global amount fallback. Per-ISIN amounts add a new persisted concept: ordered positions.

Persistence implications:

- New saved queries should store `positions` when users enter per-ISIN amounts.
- Existing saved queries that store only `isins` and global `amount` should continue to load and execute using the global amount fallback.
- The UI should load old saved queries into the structured editor by expanding each ISIN into a row with the global amount.
- A saved query with explicit `positions` should reload with each row's own amount.
- A saved query should not lose per-ISIN amounts when edited and saved again.

Migration recommendation:

- No immediate blocking migration is required for existing saved queries if the storage layer can add optional `positions` or equivalent metadata in a backward-compatible way.
- A follow-up migration ticket is needed if saved-query storage has a rigid schema that cannot persist ordered per-ISIN amounts without schema changes.
- The migration should be additive and should not rewrite existing global amount saved queries unless necessary.

## Human Approval Gate

Before implementation tickets begin for per-ISIN notional execution, a human approver must confirm:

- The UX accepts the row-entry and paste-friendly modes described here.
- The validation rules match expected BusinessQuery behavior.
- The backend contract should use `positions` as the future authoritative per-ISIN amount shape.
- Existing global amount fallback behavior must remain supported.
- Saved-query persistence may require an additive follow-up migration.

Implementation tickets for execution behavior should not proceed until this approval is recorded in the delivery tracker or ticket comments.

## Follow-Up Implementation Tickets

1. `BQ5-006`: Build BusinessQuery row-entry UI for multiple ISINs with per-row amount validation.
2. `BQ5-007`: Add paste parser and preview flow for ISIN and amount pairs.
3. `BQ5-008`: Define and implement frontend request mapping from structured rows to `positions`.
4. `BQ5-009`: Add backend request validation for `positions` while preserving global amount fallback.
5. `BQ5-010`: Implement per-ISIN notional execution in the BusinessQuery service.
6. `BQ5-011`: Add saved-query persistence support for ordered per-ISIN positions.
7. `BQ5-012`: Add or confirm saved-query migration for optional per-ISIN position storage.
8. `BQ5-013`: Add end-to-end validation coverage for row entry, paste input, duplicates, blank rows, mixed valid/invalid input, global amount fallback, and saved-query reload.

## Approval Outcome Needed

The expected output of BQ5-005 is this approved plan. Once accepted, the next role should convert the follow-up tickets into implementation work, starting with the frontend row-entry and paste parser tickets before backend execution changes.
