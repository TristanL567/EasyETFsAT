# SE-008 FX Conversion Semantics Decision

## Decision

Approved default: use the nearest prior available ECB FX rate on or before the
tax report date for non-EUR EUR conversion.

`V2_TAXDATEUR` currently treats EUR as rate `1`, joins `REFEXC` by exact
`REFCCY` and `REFDAT = TAXMDT`, and returns null converted values when a
non-EUR exact-date FX row is missing or zero. Future SQL or API changes should
replace the exact-date-only lookup with a historical, non-forward-looking lookup:

- EUR values continue to use rate `1`.
- Non-EUR values use the latest valid `REFEXC.FXRAT` where
  `REFEXC.REFCCY = FNDCCY` and `REFEXC.REFDAT <= TAXMDT`.
- Zero, null, or absent prior rates remain unconvertible and should produce
  null converted values, ideally with diagnostics in a later implementation
  ticket.
- Future implementation must add PostgreSQL-backed tests before changing
  `V2_TAXDATEUR` behavior.

This ticket is documentation-only. It does not change `REFEXC`,
`V2_TAXDATEUR`, Alembic migrations, application code, or tests.

## Options Compared

| Option | Behavior | Assessment | Decision |
|---|---|---|---|
| Exact-date only | Convert only when `REFEXC` has the same currency and date as `TAXMDT`. | Simple and matches current view behavior, but ECB observations are absent on weekends and holidays. Consumers receive nulls that can look like missing tax data instead of missing FX data. | Rejected as the future default. |
| Previous-business-day fallback | If exact date is missing, try the prior business day. | Better for weekends, but it requires a reliable business-day and ECB holiday calendar. It still fails after longer market closures, ingestion gaps, or delayed FX backfills. | Rejected. |
| Nearest prior available rate | Use the latest valid `REFEXC` row on or before `TAXMDT`. | Handles weekends, holidays, and sparse but valid historical ECB data without using future information. It matches common reporting expectations for valuation-date conversion when same-day reference data is unavailable. | Approved default. |
| Explicit-null only | Keep null output whenever the exact-date FX row is unavailable, and rely on documentation or diagnostics to explain the null. | Preserves current data shape and avoids inferred conversion, but leaves avoidable nulls in normal weekend and holiday cases. Useful only when no prior valid rate exists. | Rejected as the default; retained as the fallback when no valid prior rate exists. |

## Consumer Impact

Consumers of EUR-converted tax values should expect fewer null converted fields
after a future implementation ticket. Non-EUR values that currently return null
only because `TAXMDT` lacks an exact ECB observation may become populated using
the most recent earlier ECB rate for the same currency.

This is a compatibility-relevant behavior change. Existing consumers that use
null converted values as a signal for "missing source tax amount" will need to
distinguish source tax nulls from FX-unavailable nulls. Future implementation
should therefore pair the fallback rule with view/API tests and, if the public
surface allows it, diagnostics that identify whether null EUR values come from
missing tax data, zero FX rates, or no valid prior `REFEXC` row.

## Implementation Boundary

Do not implement this rule inside SE-008. The approved behavior should feed the
next database/view semantics work, especially tests for:

- EUR rate `1`.
- Exact-date non-EUR conversion.
- Weekend or holiday `TAXMDT` using the nearest prior available `REFEXC` rate.
- Missing prior `REFEXC` rows returning null.
- Zero or null FX rates returning null.

