# EasyETFsAT Documentation

This folder contains stakeholder-facing and technical documentation for the EasyETFsAT project.

## Recommended Reading Order

1. [Stakeholder Brief](STAKEHOLDER_BRIEF.md)
   - Short explanation of what the project does, who can use it, and how to position it.
2. [Technical Architecture](TECHNICAL_ARCHITECTURE.md)
   - How the backend, ingestion jobs, API, database, migrations, and tests work.
3. [Maintainer Layer Guide](MAINTAINER_LAYER_GUIDE.md)
   - Where source-shaped storage, curated tax rows, reporting views, API output, and ingestion jobs each own changes.
4. [Data and Query Guide](DATA_AND_QUERY_GUIDE.md)
   - Which data is stored, how tables relate, and practical SQL/API examples.
5. [Field Lineage Summary](FIELD_LINEAGE_SUMMARY.md)
   - How to use the comprehensive field table and trace fields from source to final output.

## Existing Operational Files

- [isin_storage.csv](isin_storage.csv): current ISIN universe used by the missing-ISIN ingestion job.
- [AgentInstructions/FETCH_ONLY_MISSING_ISINS.md](AgentInstructions/FETCH_ONLY_MISSING_ISINS.md): operational runbook for new ISINs.
- [AgentInstructions/REFRESH_EXISTING_ISINS.md](AgentInstructions/REFRESH_EXISTING_ISINS.md): operational runbook for refreshing already ingested ISINs.
- [Usage/Test_Example.xlsx](Usage/Test_Example.xlsx): sample spreadsheet artifact in the repository.

## One-Sentence Project Summary

EasyETFsAT is a Python and PostgreSQL backend that ingests public Austrian ETF tax data from OeKB, normalizes it into an auditable database, adds ECB FX reference rates, and exposes the tax values through SQL and a small FastAPI service.
