# DB Naming Dictionary

## Naming Rules
- Physical names are uppercase.
- Field naming pattern: `[DOM][TOK][TOK...]`.
- `DOM` is always 3 letters.
- Standard token width is 3 letters.
- Numeric tax-line tokens (`K11`, `K61`, etc.) are allowed.
- Maximum field-name length is 12.

## Domain Prefixes
- `SEC`: Security master and cash-flow domain.
- `SRC`: Source ingestion domain.
- `TAX`: Curated tax domain.
- `REF`: Reference domain.
- `IMP`: Ingestion operations domain.

## Core Tokens
- `IDN`: Internal identifier.
- `ISN`: ISIN.
- `OKB`: OeKB.
- `COD`: Code.
- `KEY`: Canonical key string.
- `NAM`: Name.
- `NDE`: Name (German).
- `NEN`: Name (English).
- `CCY`: Currency.
- `DAT`: Date.
- `RAT`: Rate.
- `CTR`: Country.
- `ERT`: Ertragstyp.
- `STS`: Status.
- `VRN`: Version number.
- `YEA`: Year.
- `MDT`: Meldedatum.
- `GVN`: Valid from.
- `GBS`: Valid to.
- `ISB`: ISIN description.
- `BUS`: Business period marker.
- `BEG`: Begin.
- `END`: End.
- `ENT`: Eintragezeit.
- `ZFL`: Zufluss.
- `JMS`: Jahresmeldung flag.
- `AMS`: Ausschuettungsmeldung flag.
- `SNW`: Selbstnachweis flag.
- `KID`: Korrigierte Melde-ID.
- `AMT`: Amount.
- `FLW`: Cash flow.
- `TYP`: Type.
- `ACT`: Active flag.
- `ORD`: Order.
- `RSN`: Reason.
- `PAY`: Payload.

## Category Tokens
- `PVM`: `pv_mit`.
- `PVO`: `pv_ohne`.
- `BVM`: `bv_mit`.
- `BVO`: `bv_ohne`.
- `BVJ`: `bv_jur`.
- `STF`: `stiftung`.
- `STI`: stiftung alias used in `V1_TAXDATPRE` output columns.

## Metric Tokens
- `K40`: `steuerpflichtige_einkuenfte`.
- `K11`: `ag_ertraege`.
- `K12`: `korrekturbetrag_saldiert`.
- `K81`: `kest_total`.
- `K82`: `kest_substanzgewinne`.
- `K10`: `substanzgewinne_kestpfl`.
- `K55`: `fondsergebnis_nichtausg`.
- `K61`: `korrekturbetrag_age_ak`.
- `K62`: `korrekturbetrag_aussch_ak`.
- `K36`: `substanzgew_folgejahre`.
- `K21`: `quellensteuern_einbeh`.

## Table Set
- Source layer: `SOURCERPT`, `SOURCEAGE`, `SOURCERAW`.
- Curated layer: `SECMDA`, `SECDIV`, `TAXRPT`, `TAXDAT`, `TAXADJ`, `TAXLIN`, `TAXCAT`, `TAXCOR`.
- Support layer: `REFCCY`, `REFCTR`, `IMPLOG`, `IMPERR`.
- FX layer: `REFEXC` (ECB exchange rates).

## Legacy Mapping
- `TAXRPT` (legacy source) -> `SOURCERPT`.
- `TAXAGE` -> `SOURCEAGE`.
- `TAXRAW` -> `SOURCERAW`.
- `TAXLST` fields -> merged into `SOURCERPT`.

## Examples
- `SOURCERPT.SRCOKBIDN`
- `SOURCERPT.SRCBUSYEABEG`
- `SOURCEAGE.SRCK81PVM`
- `TAXRPT.TAXYEA`
- `TAXDAT.TAXRPTIDN`
- `TAXADJ.TAXAMT`
- `SECDIV.SECFLWDAT`
- `REFEXC.REFDAT`
- `REFEXC.REFRAT`
