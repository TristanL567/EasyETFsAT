# Operator Instruction: Date Stored Reports by Their Zufluss

Goal: one-time correction so every stored OeKB report is dated by its Zufluss
(OeKB's Meldedatum, the day OeKB published the report), not by the day it was
entered (`eintragezeit`). Background: GitHub issues #4 and #6.

The parser dates newly fetched reports correctly. This job corrects the rows
already stored, because `TAXRPT` is rebuilt only when a report's payload or
version changes.

Only the operator decides when `--apply` runs on the live database. Agents
never run it there.

## What the job changes
- `SOURCERPT`: `SRCMDT` and `SRCYEA` from `SRCZFL`.
- `TAXRPT`: `TAXMDT` and `TAXYEA` from `TAXZFL`.
- `SOURCEAGE.SRCYEA` and `SECDIV.SECYEA` of the same report: the corrected year.
- Only reports with a stored Zufluss whose date or year differs. Reports without
  a Zufluss are left alone. A second run finds nothing to move.
- `V2_TAXDATHOMCCY` keeps its 138 columns; only `TAXMDT`, `TAXYEA`, `FXRAT`
  and the `*_EUR` values change.

## Steps
1. Before the change, run the first check query below (read-only). Its rows are
   every report the correction will move.
2. Run the dry run, which writes nothing:
   - `python -m fondant.jobs.correct_report_dates`
   - Each `SOURCERPT`/`TAXRPT` line shows `old date/year -> new date/year`.
   - Each `missing_fx` line is a non-EUR report with no ECB rate on its new date.
     Its `FXRAT` and every `*_EUR` column would be NULL, and EasyRep refuses
     such rows. Fill rates first with `fx_pipeline.backfill_ecb_rates` (see
     README, FX Pipeline), then run the dry run again.
3. With the operator's decision, apply:
   - `python -m fondant.jobs.correct_report_dates --apply`
4. Run the three check queries below.

## Check queries (read-only)

```sql
-- Expect no rows: every report with a Zufluss is dated by it.
SELECT "TAXOKBIDN", "TAXISN", "TAXYEA", "TAXMDT", "TAXZFL" FROM "TAXRPT"
WHERE "TAXZFL" IS NOT NULL AND "TAXMDT" IS DISTINCT FROM "TAXZFL";

-- Expect TAXMDT 2025-10-27 and, for the two dollar funds, FXRAT 1.1640.
SELECT "TAXOKBIDN", "TAXISN", "TAXYEA", "TAXMDT", "FXRAT" FROM "V2_TAXDATHOMCCY"
WHERE "TAXOKBIDN" IN (626766, 626718, 626704);

-- Expect no rows: no non-EUR report without a rate on its date.
SELECT "TAXOKBIDN", "TAXISN", "FNDCCY", "TAXMDT" FROM "V2_TAXDATHOMCCY"
WHERE "FNDCCY" <> 'EUR' AND "FXRAT" IS NULL;
```

EasyRep picks up the corrected rows on its next "Refresh fund tax reports".
