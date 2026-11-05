# Google Cloud Spanner Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("spanner")
```

Provides dual SQL catalog packs (`googlesql` and `postgresql`) introspecting `INFORMATION_SCHEMA` tables, columns, indexes (`STORING`, `SEARCH`, `VECTOR`), foreign keys, parent-child interleaving, and row deletion policies. See the [Google Cloud Spanner Guide](../../../databases/spanner.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `spanner` |
| **Default Schema** | `None` |
| **Version Pattern** | `.*` |
| **DDL Strategy** | AST synthesis with `INTERLEAVE IN PARENT` & `ROW DELETION POLICY` (`GoogleSQL` & `PostgreSQL` modes) |
| **Catalog `.sql` Files** | `5` files (8 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_cte` | `True` | `bigint` | `INT64` |
| `supports_for_update` | `False` | `bignumeric` | `NUMERIC` |
| `supports_generators` | `False` | `blob` | `BYTES(MAX)` |
| `supports_index_clustering` | `True` | `boolean` | `BOOL` |
| `supports_interleaved_tables` | `True` | `float` | `FLOAT64` |
| `supports_json` | `True` | `float32` | `FLOAT32` |
| `supports_on_conflict` | `True` | `integer` | `INT64` |
| `supports_returning` | `True` | `json` | `JSON` |
| `supports_schemas` | `True` | `numeric` | `NUMERIC` |
| `supports_skip_locked` | `False` | `text` | `STRING(MAX)` |
| `supports_transactions` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_update_from` | `False` | `tokenlist` | `TOKENLIST` |
| `supports_upsert` | `True` | `uuid` | `BYTES(16)` |
| `supports_window_functions` | `True` | `varchar` | `STRING({length})` |

## Adapter Data Dictionary Classes

- [`SpannerSyncDataDictionary`](../../adapters/spanner.md)
- [`SpannerAsyncDataDictionary`](../../adapters/spanner.md)

## Catalog SQL Queries

The `spanner` dialect pack defines 8 named queries across `5` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/spanner/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/spanner/sql/columns.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/spanner/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/spanner/sql/indexes.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/spanner/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/spanner/sql/version.sql` | `current` (1 named query) | *none* |
