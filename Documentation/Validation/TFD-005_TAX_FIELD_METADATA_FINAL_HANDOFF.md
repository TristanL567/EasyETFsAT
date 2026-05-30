# TFD-005 Tax Field Metadata Final Handoff

Ticket: TFD-005  
Purpose: final validation and deployment handoff for tax field metadata.

## Epic Summary

- TFD-001 sourced tax field descriptions from official OeKB tax field documentation and produced the human-approved source review.
- TFD-002 added nullable `TAXLIN` metadata columns: `TAXDSC`, `TAXUSE`, and `TAXSRC`.
- TFD-003 exposed the approved descriptions in the authenticated `/app/documentation` page.
- TFD-004 added `fondant.jobs.refresh_tax_dictionaries` so existing databases can refresh static tax dictionaries without running OeKB ingestion.

## Database Migration Requirement

Existing databases need the Alembic migration through revision `20260419_0012`.

Run:

```powershell
alembic upgrade head
```

This applies the migration that adds nullable `TAXDSC`, `TAXUSE`, and `TAXSRC` columns to `TAXLIN`.

## Metadata Refresh Requirement

After the migration, existing databases should refresh static tax dictionaries so current `TAXLIN` rows receive metadata.

Local Windows command:

```powershell
py -3.10 -m fondant.jobs.refresh_tax_dictionaries
```

Render shell command:

```sh
python -m fondant.jobs.refresh_tax_dictionaries
```

The refresh job uses the registry-backed dictionary path, commits the update, and does not fetch OeKB data.

## Verification SQL

Confirm all active tax lines have metadata:

```sql
SELECT
  COUNT(*) AS active_tax_lines,
  COUNT("TAXDSC") AS with_description,
  COUNT("TAXUSE") AS with_usage,
  COUNT("TAXSRC") AS with_source
FROM "TAXLIN"
WHERE "TAXACT" = true;
```

Inspect the populated rows:

```sql
SELECT "TAXCOD", "TAXNDE", "TAXDSC", "TAXUSE", "TAXSRC"
FROM "TAXLIN"
ORDER BY "TAXORD";
```

Expected result:

- `active_tax_lines`: 11
- `with_description`: 11
- `with_usage`: 11
- `with_source`: 11

## Scope Confirmation

TFD-005 was a handoff-documentation ticket only. It did not intend code, tests,
migrations, frontend, `.env`, deployment, or runtime behavior edits.

No changes were intended to:

- BusinessQuery calculations.
- CSV export.
- Public API behavior.
- OeKB ingestion semantics.
- Render config.
- Authentication.

## Residual Risks

- OeKB field list versions may change later and require a reviewed metadata refresh.
- Descriptions are explanatory and are not tax advice.
- Existing deployed databases need both the migration and the refresh job before `TAXLIN` metadata is populated.
