# Field Lineage Summary

This guide is the entry point for field-level lineage. Use the
[FD-006 comprehensive field table](Validation/FD-006_COMPREHENSIVE_FIELD_TABLE.md)
when you need to find a database field, understand what it means, and follow it
from source or reference data through curated tables, reporting views, or API
output.

The field table is a searchable index, not a replacement for the detailed
validation notes. It keeps each row concise and links to the owner artifact for
deeper source evidence, SQL behavior, parser rules, and open questions.

## How To Trace A Field

1. Search the field table for the object and field name.
2. Read the row's source, transformation path, final-output usage, and
   null/zero semantics.
3. Open the detailed-owner artifact when the row points to a broader rule,
   example, or open question.
4. For tax amounts, distinguish absent values from explicit zero before
   checking final views:
   - In `SOURCEAGE`, null means the source value was absent, blank, unknown, or
     invalid; explicit zero is retained.
   - In `TAXDAT`, missing source facts are represented by no row; explicit zero
     is stored as zero.
   - In `V1_TAXDATPRE`, null means no matching curated fact contributed to the
     pivot.
   - In `V2_TAXDATEUR`, null can also mean FX conversion failed because the
     exact-date `REFEXC` rate is missing, null, or zero.

## Layer Distinctions

| Layer | Objects or surface | What it means |
|---|---|---|
| Source | `SOURCERPT`, `SOURCERAW`, `SOURCEAGE` | OeKB report metadata, full raw detail payloads, and parsed source-shaped tax matrix values. |
| Curated | `SECMDA`, `SECDIV`, `TAXRPT`, `TAXDAT`, `TAXADJ`, `TAXCOR` | Normalized report headers, security data, tax facts, adjustments, distributions, and correction links. |
| Reference | `REFEXC`, `REFCCY`, `REFCTR`, `TAXLIN`, `TAXCAT` | ECB FX observations and static dictionaries for currencies, countries, tax lines, and investor categories. |
| Reporting views | `V1_TAXDATPRE`, `V2_TAXDATEUR` | Spreadsheet/BI projections over selected `K40`, `K61`, and `K62` facts; `V2` adds EUR conversion. |
| API | `/health`, `GET /etf/{isin}/tax` | HTTP outputs documented in FD-005 and SE-007; the tax endpoint reads curated `TAXRPT`/`TAXDAT` rows directly, not the reporting views. |

## Detailed References

- [FD-001 database field inventory](Validation/FD-001_DATABASE_FIELD_INVENTORY.md)
- [FD-002 OeKB source field catalog](Validation/FD-002_OEKB_SOURCE_FIELD_CATALOG.md)
- [FD-003 reference and FX field catalog](Validation/FD-003_REFERENCE_AND_FX_FIELD_CATALOG.md)
- [FD-004 source-to-curated lineage](Validation/FD-004_SOURCE_TO_CURATED_LINEAGE.md)
- [FD-005 reporting and API field lineage](Validation/FD-005_REPORTING_AND_API_FIELD_LINEAGE.md)
- [Maintainer layer guide](MAINTAINER_LAYER_GUIDE.md)
- [Data and query guide](DATA_AND_QUERY_GUIDE.md)

Known unclear semantics stay in the table as open questions. Do not infer tax,
FX, adjustment, correction, distribution, or API compatibility behavior beyond
the linked documentation.
