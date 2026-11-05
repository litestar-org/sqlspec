# PostgreSQL Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("postgres")
```

Introspects `pg_catalog` (`pg_class`, `pg_attribute`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_type`, `pg_extension`) and `information_schema` across `asyncpg`, `psycopg`, and `psqlpy`. See the [PostgreSQL Guide](../../../databases/postgres/index.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `postgres` |
| **Default Schema** | `"public"` |
| **Version Pattern** | `PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?` |
| **DDL Strategy** | AST synthesis + `pg_get_*def` catalog functions |
| **Catalog `.sql` Files** | `21` files (26 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_for_update` | `True` | `bigint` | `BIGINT` |
| `supports_on_conflict` | `True` | `blob` | `BYTEA` |
| `supports_prepared_statements` | `True` | `boolean` | `BOOLEAN` |
| `supports_schemas` | `True` | `float` | `DOUBLE PRECISION` |
| `supports_transactions` | `True` | `integer` | `INTEGER` |
| `supports_update_from` | `True` | `json` | `JSONB` |
| `supports_uuid` | `True` | `text` | `TEXT` |
| `supports_cte` | `>= 8.4.0` | `timestamp` | `TIMESTAMP WITH TIME ZONE` |
| `supports_json` | `>= 9.2.0` | `uuid` | `UUID` |
| `supports_jsonb` | `>= 9.4.0` | `varchar` | `VARCHAR({length})` |
| `supports_partitioning` | `>= 10.0.0` | - | - |
| `supports_returning` | `>= 8.2.0` | - | - |
| `supports_skip_locked` | `>= 9.5.0` | - | - |
| `supports_upsert` | `>= 9.5.0` | - | - |
| `supports_window_functions` | `>= 8.4.0` | - | - |

## Adapter Data Dictionary Classes

- [`AsyncpgDataDictionary`](../../adapters/asyncpg.md)
- [`PsycopgSyncDataDictionary`](../../adapters/psycopg.md)
- [`PsycopgAsyncDataDictionary`](../../adapters/psycopg.md)
- [`PsqlpyDataDictionary`](../../adapters/psqlpy.md)

## Catalog SQL Queries

The `postgres` dialect pack defines 26 named queries across `21` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/postgres/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `comments.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/comments.sql` | `by_schema` (1 named query) | `:schema_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/constraints.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/ddl.sql` | `by_object` (1 named query) | `:object_name`, `:object_type`, `:schema_name` |
| `dependencies.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/dependencies.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `extensions.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/extensions.sql` | `list` (1 named query) | *none* |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `materialized_views.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/materialized_views.sql` | `by_schema` (1 named query) | `:schema_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `partitions.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/partitions.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `privileges.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/privileges.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/routines.sql` | `by_schema` (1 named query) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/schemas.sql` | `list` (1 named query) | *none* |
| `sequences.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/sequences.sql` | `by_schema` (1 named query) | `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/system.sql` | `pg_stat_statements`, `settings`, `table_stats` (3 named queries) | `:limit`, `:schema_name`, `:table_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `triggers.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/triggers.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `types.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/types.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/postgres/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
