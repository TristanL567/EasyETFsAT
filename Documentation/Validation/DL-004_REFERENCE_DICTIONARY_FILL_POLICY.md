# DL-004 Reference Dictionary Fill Policy

Ticket: DL-004  
Title: Decide Reference Dictionary Fill Policy  
Scope: decision documentation only. No code, schema, migration, seed, ingestion, frontend, API, deployment, or database changes are made by this ticket.

## Decision Summary

`REFCCY` and `REFCTR` should remain empty intentionally for now. Treat both tables as reserved reference dictionary tables until a concrete consumer and authoritative source are defined.

Do not seed `REFCCY` or `REFCTR` from static lists, observed database values, OeKB-derived values, ECB data, or ad hoc local database state under the current ticket scope. Add a future implementation ticket only if a downstream feature needs human-readable currency or country metadata.

## Current State

- `REFCCY` exists as the currency reference table.
- `REFCTR` exists as the country reference table.
- Current reviewed code has no active seed/upsert path for either table.
- Existing runtime surfaces do not currently depend on either table for `BusinessQuery`, API tax output, Search, `V1_TAXDATPRE`, or `V2_TAXDATEUR`.
- `REFEXC` is the active FX data table used by `V2_TAXDATEUR`.

The important distinction is that `REFEXC` is an operational data table populated by ECB FX ingestion, while `REFCCY` and `REFCTR` are dictionary-shaped tables without an established runtime fill path or consumer contract.

## Option Comparison

| Option | Pros | Cons | Operational impact | Risk |
|---|---|---|---|---|
| Keep empty intentionally | Matches current runtime behavior; avoids inventing unowned reference data; makes empty row counts explainable; requires no code, migration, seed, or data backfill work. | Leaves human-readable currency and country names unavailable from these tables; future consumers must still define source and semantics. | Document as reserved; no deployment or database operation required. | Low. Main risk is future confusion if empty tables are not documented as intentional. |
| Seed static dictionaries | Provides predictable currency/country names and minor-unit/country-name metadata; can support future UI/API display needs. | Requires choosing authoritative sources, update cadence, localization policy, and handling obsolete or disputed codes; adds maintenance ownership before a consumer exists. | Requires seed data, seed/upsert code, tests, and possibly migration/backfill coordination. | Medium. Premature seeds can become stale or semantically wrong and may create false confidence that dictionary coverage is authoritative. |
| Populate from observed values in the current database | Produces only codes actually seen in local data; can expose coverage gaps quickly; avoids broad static dictionaries. | Observed values do not provide reliable names, minor units, German/English country labels, or authoritative validity; local database state may be incomplete or environment-specific. | Requires extraction logic, normalization rules, metadata enrichment, and repeatable refresh behavior. | Medium to high. Environment-derived dictionaries can encode accidental data gaps and are not portable across deployments. |
| Remove or defer the tables | Eliminates empty-table ambiguity; avoids maintaining unused schema. | Removing tables is a schema change and may break existing documentation, models, migrations, or future planned metadata use; deferral still needs clear documentation. | Removal would require migration and code/model cleanup. Deferral requires only documentation. | Medium if removed now because it creates unnecessary churn; low if deferred as reserved documentation policy. |

## Recommendation

Recommended policy: keep `REFCCY` and `REFCTR` empty intentionally for now.

Treat both as reserved reference dictionary tables. Do not seed them until a concrete consumer or authoritative source is defined. A future ticket may add static seed data only after it specifies:

- which runtime surface consumes the dictionaries;
- the authoritative currency and country sources;
- the exact fields required by that consumer;
- the update cadence and ownership model;
- migration, seed/upsert, and test scope.

This policy keeps current behavior aligned with the reviewed runtime: `REFEXC` remains the active FX source for `V2_TAXDATEUR`, while `REFCCY` and `REFCTR` remain descriptive dictionary placeholders with no active dependency.

## Follow-Up Scope If Seeding Is Later Approved

If human approval later chooses static or observed dictionary population, implementation should be handled in a separate ticket. That ticket should be allowed to touch code, tests, migrations, and seed paths as needed, because seeding cannot be completed as documentation-only work.

Likely implementation scope would include:

- defining authoritative currency and country source files or upstream providers;
- adding deterministic seed/upsert logic for `REFCCY` and `REFCTR`;
- adding tests for idempotency, required fields, and duplicate-code handling;
- documenting consumer behavior and empty-table semantics after the fill policy changes.

Human approval required before any implementation:
- Confirm whether REFCCY and REFCTR should remain empty/reserved.
- Confirm whether static seed data is desired.
- Confirm whether any downstream feature needs these dictionaries.
