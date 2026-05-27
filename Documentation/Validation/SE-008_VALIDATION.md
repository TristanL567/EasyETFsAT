# SE-008 Validation

## Ticket

SE-008: Decide FX Conversion Semantics

## Result

Accepted.

## Scope Check

Changed files reviewed:

- `Documentation\Validation\SE-008_FX_CONVERSION_SEMANTICS_DECISION.md`

Validator artifact added:

- `Documentation\Validation\SE-008_VALIDATION.md`

No `alembic`, `fondant`, or `tests` files changed. The worker did not stage,
commit, push, or start adjacent ticket work.

An unrelated untracked documentation file was present in the worktree during
validation and was intentionally left unstaged.

## Findings

- The decision note approves nearest prior available ECB FX rate on or before
  `TAXMDT` as the future default for non-EUR EUR conversion.
- Exact-date only, previous-business-day fallback, and explicit-null-only are
  compared and rejected as defaults.
- EUR rate `1`, missing prior `REFEXC`, zero/null rates, and consumer impact are
  stated clearly.
- Implementation is explicitly deferred to later database/view semantics work.

## Verification

```text
git status --short --branch
```

Result:

```text
## development
?? Documentation/Validation/DATABASE_LINEAGE_AND_DATA_READINESS_EPIC.md
?? Documentation/Validation/SE-008_FX_CONVERSION_SEMANTICS_DECISION.md
```

```text
rg -n "FX|REFEXC|V2_TAXDATEUR" Documentation\Validation
```

Result: passed. The search found the new SE-008 decision note and prior FX
evidence in validation artifacts.

## Decision

SE-008 is accepted and ready to commit on `development`.
