# SE-004 Foundation Category Alias Decision

## Decision

Keep the current aliases for compatibility.

`STI` is not a bug. It is the existing reporting-view compatibility alias for
the foundation investor category. Do not rename it casually. Any future cleanup
from `STI` to `STF` must be handled as an explicit migration with consumer
impact review, transitional aliases, and tests around the reporting/API
contract.

## Current Naming

| Layer | Current name | Meaning |
|---|---|---|
| Business/source category name | `stiftung` | Foundation investor category in parser/business terminology. |
| Dictionary and source code | `STF` | Canonical category code used by parser output, seeded dictionaries, and source-shaped table columns. |
| Reporting/API-facing alias | `STI` | Existing public/reporting-view alias exposed by `V1_TAXDATPRE` and `V2_TAXDATEUR` style output columns. |

## Compatibility Risk

The risk is consumer breakage, not data ambiguity. Prior validation artifacts
show that `STF`, `stiftung`, and `STI` already refer to the same foundation
category in different layers:

- `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md` records `STF` as
  the source/model/category abbreviation, `stiftung` as the business term, and
  `STI` as the view-output alias.
- `Documentation\Validation\SE-003_TAX_CODE_REGISTRY.md` records the registry
  decision that `STF` remains the source/category code, `stiftung` remains the
  API category alias, and `STI` remains the reporting-view alias.

Renaming `STI` in reporting views or API-shaped outputs could break downstream
reports, spreadsheets, dashboards, or integrations that already select those
columns. The current spelling drift is therefore intentional compatibility,
even though it remains a readability trap for maintainers.

## Recommendation

Recommended path: keep and document the aliases.

- Keep `STF` as the canonical dictionary/source category code.
- Keep `stiftung` as the human-readable business/API category name.
- Keep `STI` as the reporting-view compatibility alias.
- Treat any future `STI` removal or rename as a separate migration ticket, not
  a cleanup refactor.

