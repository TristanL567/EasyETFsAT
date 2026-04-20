# Agent Instruction: Refresh Existing ISINs

Goal: re-fetch all ISINs that already exist in `SOURCERPT` to detect and ingest OeKB data changes.

## Steps
1. Run dry-run first:
   - `python -m fondant.jobs.refresh_existing_isins --dry-run --show-isins`
2. Run full refresh:
   - `python -m fondant.jobs.refresh_existing_isins`

## Optional
- Refresh only selected ISIN(s):
  - `python -m fondant.jobs.refresh_existing_isins --isin IE00BMTX1Y45 --isin LU0380865021`
- Limit batch size:
  - `python -m fondant.jobs.refresh_existing_isins --limit 25`
