# SE-007 Public API Contract Note

This note records the current public contract for `/health` and
`GET /etf/{isin}/tax` as tested in `tests/test_api_etf.py`.

## `/health`

`GET /health` returns HTTP 200 with exactly:

```json
{"status": "ok"}
```

The OpenAPI schema describes the response as a string-valued object.

## `GET /etf/{isin}/tax?year={year}`

The endpoint uppercases the path ISIN before lookup. It does not currently
syntax-validate ISINs; an invalid-looking or unknown ISIN returns HTTP 404 when
no tax rows match. The required `year` query parameter is an integer constrained
by OpenAPI to `1900 <= year <= 3000`.

Successful responses include:

- `isin`: uppercased lookup value.
- `year`: requested query year.
- `year_fallback_null_used`: `true` only when no exact year rows exist and
  rows with `TAXYEA IS NULL` are returned instead.
- `count`: number of returned report objects.
- `reports`: report objects with `stm_id`, `versions_nr`, `status_code`,
  `waehrung`, `meldg_datum`, and nested `tax_fields`.

Reports are selected for the exact report year first and fall back to null-year
rows only when the exact-year result set is empty. Reports are ordered by
descending `meldg_datum`; ties and null-date placement have no explicit
secondary contract in the implementation, so consumers should not depend on tie
order.

`tax_fields` is nested as `metric_key -> category_key -> amount`. Missing tax
points are omitted, not emitted as null values. Nullable report metadata is
serialized as JSON `null`.

## Numeric Semantics

Current API tax amounts are JSON numbers produced by converting database
`Decimal`/`Numeric` values to Python `float`. The current contract therefore
accepts normal JSON numeric semantics and does not provide decimal-safe strings.
A future compatibility ticket is required before switching public tax amounts to
string serialization for exact decimal preservation.
