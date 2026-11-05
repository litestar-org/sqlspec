# DuckDB Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("duckdb")
```

Introspects DuckDB table functions (`duckdb_tables()`, `duckdb_columns()`, `duckdb_indexes()`, `duckdb_constraints()`, `duckdb_views()`, `duckdb_sequences()`, `duckdb_types()`, `duckdb_extensions()`). See the [DuckDB Guide](../../../databases/duckdb.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `duckdb` |
| **Default Schema** | `None` |
| **Version Pattern** | `v?(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `duckdb_*().sql` catalog functions + AST synthesis |
| **Catalog `.sql` Files** | `17` files (24 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `LIST` |
| `supports_cte` | `True` | `bigint` | `BIGINT` |
| `supports_for_update` | `False` | `blob` | `BLOB` |
| `supports_json` | `True` | `boolean` | `BOOLEAN` |
| `supports_maps` | `True` | `float` | `DOUBLE` |
| `supports_on_conflict` | `True` | `integer` | `INTEGER` |
| `supports_prepared_statements` | `True` | `json` | `JSON` |
| `supports_schemas` | `True` | `map` | `MAP` |
| `supports_skip_locked` | `False` | `struct` | `STRUCT` |
| `supports_structs` | `True` | `text` | `TEXT` |
| `supports_transactions` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_update_from` | `True` | `uuid` | `UUID` |
| `supports_uuid` | `True` | `varchar` | `TEXT` |
| `supports_window_functions` | `True` | - | - |
| `supports_returning` | `>= 0.8.0` | - | - |
| `supports_upsert` | `>= 0.8.0` | - | - |

## Adapter Data Dictionary Classes

- [`DuckDBDataDictionary`](../../adapters/duckdb.md)
- [`AdbcDataDictionary`](../../adapters/adbc.md)
- [`ArrowOdbcDataDictionary`](../../adapters/arrow-odbc.md)

## Catalog SQL Queries

The `duckdb` dialect pack defines 24 named queries across `17` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/duckdb/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/constraints.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `databases.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/databases.sql` | `list` (1 named query) | *none* |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/ddl.sql` | `by_object`, `by_schema` (2 named queries) | `:object_name`, `:object_type`, `:schema_name` |
| `dependencies.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/dependencies.sql` | `by_schema` (1 named query) | `:schema_name` |
| `extensions.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/extensions.sql` | `list` (1 named query) | *none* |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `functions.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/functions.sql` | `by_schema` (1 named query) | `:schema_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/schemas.sql` | `list` (1 named query) | *none* |
| `sequences.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/sequences.sql` | `by_schema` (1 named query) | `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/system.sql` | `logs`, `memory`, `settings` (3 named queries) | *none* |
| `tables.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `types.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/types.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/duckdb/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
