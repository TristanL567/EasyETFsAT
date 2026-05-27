# SE-003 Tax Code Registry

`fondant/tax_registry.py` is the authoritative code-level registry for live tax
line codes, investor category codes, parser aliases, API aliases, source-table
aliases, and reporting-view aliases.

The current foundation-category decision is explicit in the registry:

- `STF` remains the source/category code used by parser output, seed
  dictionaries, and source-table columns.
- `stiftung` remains the API category alias emitted through curated dictionary
  keys.
- `STI` remains the existing reporting-view alias for compatibility.

Consistency tests in `tests/test_tax_registry.py` compare the registry with
parser maps, seed dictionaries, parser output models, and `SOURCEAGE` source
columns. Migration tests derive expected `V1_TAXDATPRE` aliases from the same
registry without changing Alembic migrations.
