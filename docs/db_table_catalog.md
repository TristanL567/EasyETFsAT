# DB Table Catalog

This catalog is the authoritative field list for the refactored source and curated architecture.

## SOURCERPT
- Purpose: Source report metadata, one row per OeKB report event.
- Grain: `SRCISN + SRCOKBIDN`.
- Primary key: `SRCIDN`.
- Unique key: `SRCISN, SRCOKBIDN`.
- Fields:
  - `SRCIDN` integer, PK.
  - `SRCCRTDTS` datetime tz.
  - `SRCUPDDTS` datetime tz.
  - `SRCISN` varchar(12), not null.
  - `SRCOKBIDN` bigint, not null.
  - `SRCVRN` integer, not null.
  - `SRCSTS` varchar(16).
  - `SRCYEA` integer.
  - `SRCMDT` date.
  - `SRCCCY` varchar(3).
  - `SRCISB` varchar(255).
  - `SRCGVN` date.
  - `SRCGBS` date.
  - `SRCBUSYEABEG` date.
  - `SRCBUSYEAEND` date.
  - `SRCENTDTS` datetime.
  - `SRCZFL` date.
  - `SRCJMS` boolean.
  - `SRCAMS` boolean.
  - `SRCSNW` boolean.
  - `SRCKIDN` bigint.

## SOURCEAGE
- Purpose: Parsed source tax matrix (wide, source-faithful).
- Grain: `SRCISN + SRCOKBIDN`.
- Primary key: `SRCIDN`.
- Unique key: `SRCISN, SRCOKBIDN`.
- Foreign key: `(SRCISN, SRCOKBIDN)` -> `SOURCERPT(SRCISN, SRCOKBIDN)`.
- Core fields:
  - `SRCIDN`, `SRCCRTDTS`, `SRCUPDDTS`.
  - `SRCISN`, `SRCOKBIDN`, `SRCVRN`, `SRCYEA`.
  - Metric/category columns: `SRCK11PVM ... SRCK21STF`, plus `SRCK40PVM ... SRCK40STF`, `SRCK62PVM ... SRCK62STF`.

## SOURCERAW
- Purpose: Raw source JSON payload.
- Grain: `SRCISN + SRCOKBIDN`.
- Primary key: `SRCIDN`.
- Unique key: `SRCISN, SRCOKBIDN`.
- Foreign key: `(SRCISN, SRCOKBIDN)` -> `SOURCERPT(SRCISN, SRCOKBIDN)`.
- Fields:
  - `SRCIDN`, `SRCCRTDTS`, `SRCUPDDTS`.
  - `SRCISN`, `SRCOKBIDN`, `SRCVRN`.
  - `SRCPAY` json/jsonb.

## SECMDA
- Purpose: Security master record per ISIN.
- Grain: `SECISN`.
- Primary key: `SECIDN`.
- Unique key: `SECISN`.
- Fields:
  - `SECIDN`, `SECCRTDTS`, `SECUPDDTS`.
  - `SECISN`, `SECNAM`, `SECCCY`, `SECCTR`, `SECERT`.

## SECDIV
- Purpose: Security cash-flow/distribution events.
- Grain: `SECISN + SECFLWDAT + SECFLWTYP + SECOKBIDN`.
- Primary key: `SECIDN`.
- Unique key: `SECISN, SECFLWDAT, SECFLWTYP, SECOKBIDN`.
- Foreign key: `SECISN` -> `SECMDA(SECISN)`.
- Fields:
  - `SECIDN`, `SECCRTDTS`, `SECUPDDTS`.
  - `SECISN`, `SECOKBIDN`, `SECFLWTYP`, `SECFLWDAT`.
  - `SECFLWAMT`, `SECCCY`, `SECYEA`, `SECSTS`.

## TAXRPT
- Purpose: Curated tax report event.
- Grain: `TAXISN + TAXOKBIDN`.
- Primary key: `TAXIDN`.
- Unique keys: `TAXISN, TAXOKBIDN` and `TAXIDN, TAXOKBIDN`.
- Foreign key: `TAXISN` -> `SECMDA(SECISN)`.
- Fields:
  - `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS`.
  - `TAXISN`, `TAXOKBIDN`, `TAXVRN`, `TAXSTS`, `TAXYEA`, `TAXMDT`.
  - `TAXCCY`, `TAXISB`, `TAXGVN`, `TAXGBS`.
  - `TAXBUSYEABEG`, `TAXBUSYEAEND`, `TAXZFL`.
  - `TAXJMS`, `TAXAMS`, `TAXSNW`, `TAXKIDN`.

## TAXLIN
- Purpose: Tax line dictionary.
- Grain: tax line code.
- Primary key: `TAXIDN`.
- Unique keys: `TAXCOD`, `TAXKEY`.
- Fields:
  - `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS`.
  - `TAXCOD`, `TAXKEY`, `TAXNDE`, `TAXNEN`, `TAXORD`, `TAXACT`, `TAXGVN`, `TAXGBS`.

## TAXCAT
- Purpose: Investor category dictionary.
- Grain: category code.
- Primary key: `TAXIDN`.
- Unique keys: `TAXCOD`, `TAXKEY`.
- Fields:
  - `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS`.
  - `TAXCOD`, `TAXKEY`, `TAXNDE`, `TAXNEN`, `TAXORD`.

## TAXDAT
- Purpose: Curated tax values in narrow/vertical format.
- Grain: `TAXRPTIDN + TAXLINIDN + TAXCATIDN`.
- Primary key: `TAXIDN`.
- Unique key: `TAXRPTIDN, TAXLINIDN, TAXCATIDN`.
- Foreign keys:
  - `(TAXRPTIDN, TAXOKBIDN)` -> `TAXRPT(TAXIDN, TAXOKBIDN)`.
  - `TAXLINIDN` -> `TAXLIN(TAXIDN)`.
  - `TAXCATIDN` -> `TAXCAT(TAXIDN)`.
- Fields:
  - `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS`.
  - `TAXRPTIDN`, `TAXOKBIDN`, `TAXLINIDN`, `TAXCATIDN`.
  - `TAXAMT`, `TAXCCY`.

## TAXADJ
- Purpose: Fast-access cost-basis adjustments.
- Grain: `TAXRPTIDN + TAXCATIDN + TAXCOD`.
- Primary key: `TAXIDN`.
- Unique key: `TAXRPTIDN, TAXCATIDN, TAXCOD`.
- Foreign keys:
  - `(TAXRPTIDN, TAXOKBIDN)` -> `TAXRPT(TAXIDN, TAXOKBIDN)`.
  - `TAXCATIDN` -> `TAXCAT(TAXIDN)`.
- Fields:
  - `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS`.
  - `TAXRPTIDN`, `TAXOKBIDN`, `TAXCATIDN`.
  - `TAXCOD`, `TAXAMT`, `TAXCCY`.

## TAXCOR
- Purpose: Correction-chain links between curated tax reports.
- Grain: old/new report pair.
- Primary key: `TAXIDN`.
- Unique key: `TAXOLDRPTIDN, TAXNEWRPTIDN`.
- Foreign keys:
  - `TAXOLDRPTIDN` -> `TAXRPT(TAXIDN)`.
  - `TAXNEWRPTIDN` -> `TAXRPT(TAXIDN)`.
- Fields:
  - `TAXIDN`, `TAXCRTDTS`, `TAXUPDDTS`.
  - `TAXOLDRPTIDN`, `TAXNEWRPTIDN`, `TAXRSN`.

## REFEXC
- Purpose: ECB reference FX rates for currency conversion into EUR.
- Grain: `REFDAT + REFCCY`.
- Primary key: `REFIDN`.
- Unique key: `REFDAT, REFCCY`.
- Fields:
  - `REFIDN`, `REFCRTDTS`, `REFUPDDTS`.
  - `REFDAT` (date), `REFCCY` (3-letter currency), `REFRAT` (NUMERIC(20,10)).

## V1_TAXDATPRE (View)
- Purpose: pre-aggregated tax projection view per report event.
- Output columns:
  - `TAXISN`, `TAXOKBIDN`, `TAXYEA`, `FNDCCY`.
  - `K61PVM`, `K61PVO`, `K61BVM`, `K61BVO`, `K61BVJ`, `K61STI`.
  - `K62PVM`, `K62PVO`, `K62BVM`, `K62BVO`, `K62BVJ`, `K62STI`.
  - `K40PVM`, `K40PVO`, `K40BVM`, `K40BVO`, `K40BVJ`, `K40STI`.

## V2_TAXDATEUR (View)
- Purpose: EUR-converted tax projection view based on `V1_TAXDATPRE`.
- FX logic:
  - Join `TAXRPT` via `TAXISN + TAXOKBIDN` to get `TAXMDT`.
  - Join `REFEXC` via `REFCCY = FNDCCY` and `REFDAT = TAXMDT` (exact date match).
  - For `FNDCCY = EUR`, conversion rate is `1`.
  - Converted value = source value / FX rate.
- Output columns:
  - `TAXISN`, `TAXOKBIDN`, `TAXYEA`, `FNDCCY`, `TAXMDT`, `FXRAT`.
  - `K61PVM`, `K61PVO`, `K61BVM`, `K61BVO`, `K61BVJ`, `K61STI` (EUR).
  - `K62PVM`, `K62PVO`, `K62BVM`, `K62BVO`, `K62BVJ`, `K62STI` (EUR).
  - `K40PVM`, `K40PVO`, `K40BVM`, `K40BVO`, `K40BVJ`, `K40STI` (EUR).

## Reference and Ops Tables
- `REFCCY`, `REFCTR`: reference dictionaries.
- `IMPLOG`, `IMPERR`: unchanged ingestion observability tables.
