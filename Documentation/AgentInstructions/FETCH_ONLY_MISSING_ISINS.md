# Agent Instruction: Fetch Only Missing ISINs

Goal: ingest new ISINs from our storage without re-fetching ISINs that are already in `SOURCERPT`.

## Steps
1. Ensure the ISIN storage file exists and is up to date:
   - `Documentation/isin_storage.csv`
2. Run dry-run first:
   - `python -m fondant.jobs.fetch_missing_isins --dry-run --show-isins`
3. Execute ingestion for missing ISINs only:
   - `python -m fondant.jobs.fetch_missing_isins`

## Optional
- Add and persist one new ISIN while running:
  - `python -m fondant.jobs.fetch_missing_isins --isin IE00BMTX1Y45 --persist-input --dry-run`
- Limit batch size:
  - `python -m fondant.jobs.fetch_missing_isins --limit 10`
