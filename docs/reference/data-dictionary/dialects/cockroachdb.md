# CockroachDB Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("cockroachdb")
```

Introspects `crdb_internal` (`create_statements`, `tables`) and `pg_catalog` compatibility views across `cockroach_asyncpg` and `cockroach_psycopg`. See the [CockroachDB Guide](../../../databases/cockroachdb.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `cockroachdb` |
| **Default Schema** | `"public"` |
| **Version Pattern** | `CockroachDB (?:CCL )?v(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `crdb_internal.create_statement` + AST fallback |
| **Catalog `.sql` Files** | `15` files (18 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_crdb_internal_metadata` | `False` | `bigint` | `INT8` |
| `supports_for_update` | `True` | `blob` | `BYTES` |
| `supports_on_conflict` | `True` | `boolean` | `BOOL` |
| `supports_prepared_statements` | `True` | `float` | `FLOAT8` |
| `supports_schemas` | `True` | `integer` | `INT8` |
| `supports_skip_locked` | `True` | `json` | `JSONB` |
| `supports_transactions` | `True` | `text` | `STRING` |
| `supports_update_from` | `True` | `timestamp` | `TIMESTAMPTZ` |
| `supports_uuid` | `True` | `uuid` | `UUID` |
| `supports_cte` | `>= 19.1.0` | `varchar` | `VARCHAR({length})` |
| `supports_json` | `>= 20.1.0` | - | - |
| `supports_returning` | `>= 20.1.0` | - | - |
| `supports_upsert` | `>= 19.2.0` | - | - |
| `supports_window_functions` | `>= 19.1.0` | - | - |

## Adapter Data Dictionary Classes

- [`CockroachAsyncpgDataDictionary`](../../adapters/cockroach-asyncpg.md)
- [`CockroachPsycopgSyncDataDictionary`](../../adapters/cockroach-psycopg.md)
- [`CockroachPsycopgAsyncDataDictionary`](../../adapters/cockroach-psycopg.md)

## Catalog SQL Queries

The `cockroachdb` dialect pack defines 18 named queries across `15` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/cockroachdb/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `comments.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/comments.sql` | `by_schema` (1 named query) | `:schema_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/constraints.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `crdb_internal.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/crdb_internal.sql` | `ranges` (1 named query) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/ddl.sql` | `by_object` (1 named query) | `:object_name`, `:object_type`, `:schema_name` |
| `dependencies.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/dependencies.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `privileges.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/privileges.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/schemas.sql` | `list` (1 named query) | *none* |
| `sequences.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/sequences.sql` | `by_schema` (1 named query) | `:schema_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/cockroachdb/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
