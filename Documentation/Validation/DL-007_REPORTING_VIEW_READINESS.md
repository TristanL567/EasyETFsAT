# DL-007 Reporting View Readiness

## Purpose

This note characterizes when `V1_TAXDATPRE` and `V2_TAXDATEUR` are ready for
downstream reporting. It documents read-only diagnostics only. It does not
change exact-date FX semantics, add nearest-prior FX fallback, change view
definitions, change schema, or change ingestion.

## Reporting View Shape

`V1_TAXDATPRE` has one row per curated report grouping and exposes selected
pivoted values only. The grouping is the curated report identity used by the
view: `TAXISN`, `TAXOKBIDN`, `TAXYEA`, and fund currency as `FNDCCY`.

`V1_TAXDATPRE` includes selected `K40`, `K61`, and `K62` values by legal entity
category. The current selected columns are:

```text
K61PVM, K61PVO, K61BVM, K61BVO, K61BVJ, K61STI
K62PVM, K62PVO, K62BVM, K62BVO, K62BVJ, K62STI
K40PVM, K40PVO, K40BVM, K40BVO, K40BVJ, K40STI
```

`V2_TAXDATEUR` uses `V1_TAXDATPRE`, `TAXRPT.TAXMDT`, and exact-date `REFEXC`
rates to present the same selected values converted to EUR. EUR rows use FX
rate `1`. Non-EUR rows need an exact `REFEXC` row where:

```text
REFEXC.REFCCY = report currency
REFEXC.REFDAT = TAXRPT.TAXMDT
```

The current reporting view behavior is exact-date only. This ticket must not
change exact-date FX semantics and must not add nearest-prior FX fallback.

## Readiness Interpretation

`V1_TAXDATPRE` is ready for downstream reporting when curated `TAXRPT` report
groupings expected by the consumer are present in the view, and the selected
`K40`, `K61`, and `K62` category values have the expected source facts.

`V2_TAXDATEUR` is ready when matching `V1_TAXDATPRE` rows exist, the matching
`TAXRPT` rows provide `TAXMDT`, and each non-EUR report date/currency pair has
a non-null, non-zero exact-date `REFEXC.REFRAT`. EUR rows are ready for FX
conversion with `FXRAT = 1`.

Null EUR-converted values in `V2_TAXDATEUR` can mean any of the following:

- the source tax value is missing or null in the selected `V1_TAXDATPRE` field
- exact-date FX is missing for a non-EUR `TAXRPT.TAXMDT` and report currency
- exact-date FX exists but `REFEXC.REFRAT` is zero or null
- the reporting view has no matching source row, so the expected report grouping
  is absent before conversion

These cases need different follow-up. Missing source facts point to source or
curation coverage. Missing, zero, or null exact-date FX points to `REFEXC`
coverage under the current exact-date semantics. Missing view rows point to a
view/report grouping mismatch or absent curated report.

## Read-Only Diagnostic Command

Run the read-only diagnostic from the repository root:

```powershell
py -3.10 scripts/reporting_view_readiness.py
```

The command uses configured database settings through `DATABASE_URL`, converts
async SQLAlchemy drivers to synchronous command-line drivers, and executes
SELECT/introspection only. It does not fetch OeKB data, does not fetch ECB data,
does not mutate rows, does not run migrations, and does not alter
`V1_TAXDATPRE`, `V2_TAXDATEUR`, `REFEXC`, or `TAXRPT`.

The diagnostic reports:

- `TAXRPT` row count
- `V1_TAXDATPRE` row count
- `V2_TAXDATEUR` row count
- `TAXRPT` report groupings missing from `V1_TAXDATPRE`
- `V1_TAXDATPRE` rows missing from `V2_TAXDATEUR`
- non-EUR `TAXRPT` report-date/currency combinations missing exact-date
  `REFEXC`
- non-EUR exact-date `REFEXC` rows whose `REFRAT` is null or zero
- selected `K40`/`K61`/`K62` null-value counts in `V1_TAXDATPRE` and
  `V2_TAXDATEUR`
- per-column `V2_TAXDATEUR` null reason counts for source-null, missing FX,
  and zero FX situations

Readiness gaps are printed as findings and the command exits `0` when
diagnostics can run. A non-zero exit means the schema/connection is unavailable
or an unexpected execution failure occurred.
