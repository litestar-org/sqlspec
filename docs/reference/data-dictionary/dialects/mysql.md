# MySQL Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("mysql")
```

Introspects `information_schema` (`TABLES`, `COLUMNS`, `STATISTICS`, `KEY_COLUMN_USAGE`, `REFERENTIAL_CONSTRAINTS`, `CHECK_CONSTRAINTS`, `ROUTINES`, `TRIGGERS`) across `asyncmy`, `aiomysql`, `pymysql`, and `mysqlconnector`. See the [MySQL Guide](../../../databases/mysql.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `mysql` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `SHOW CREATE` + `information_schema` synthesis |
| **Catalog `.sql` Files** | `18` files (27 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `BIGINT` |
| `supports_for_update` | `True` | `blob` | `BLOB` |
| `supports_on_conflict` | `False` | `boolean` | `TINYINT(1)` |
| `supports_prepared_statements` | `True` | `float` | `DOUBLE` |
| `supports_returning` | `False` | `integer` | `INT` |
| `supports_schemas` | `True` | `json` | `JSON` |
| `supports_sequences` | `False` | `text` | `TEXT` |
| `supports_system_versioned_tables` | `False` | `timestamp` | `TIMESTAMP` |
| `supports_transactions` | `True` | `uuid` | `VARCHAR(36)` |
| `supports_update_from` | `False` | `varchar` | `VARCHAR({length})` |
| `supports_upsert` | `True` | - | - |
| `supports_uuid` | `False` | - | - |
| `supports_check_constraints` | `>= 8.0.16` | - | - |
| `supports_cte` | `>= 8.0.1` | - | - |
| `supports_events` | `>= 5.1.6` | - | - |
| `supports_generated_columns` | `>= 5.7.0` | - | - |
| `supports_histograms` | `>= 8.0.2` | - | - |
| `supports_invisible_columns` | `>= 8.0.23` | - | - |
| `supports_invisible_indexes` | `>= 8.0.0` | - | - |
| `supports_json` | `>= 5.7.8` | - | - |
| `supports_resource_groups` | `>= 8.0.3` | - | - |
| `supports_roles` | `>= 8.0.0` | - | - |
| `supports_skip_locked` | `>= 8.0.1` | - | - |
| `supports_window_functions` | `>= 8.0.2` | - | - |

## Adapter Data Dictionary Classes

- [`AsyncmyDataDictionary`](../../adapters/asyncmy.md)
- [`AiomysqlDataDictionary`](../../adapters/aiomysql.md)
- [`PyMysqlDataDictionary`](../../adapters/pymysql.md)
- [`MysqlConnectorSyncDataDictionary`](../../adapters/mysqlconnector.md)
- [`MysqlConnectorAsyncDataDictionary`](../../adapters/mysqlconnector.md)

## Catalog SQL Queries

The `mysql` dialect pack defines 27 named queries across `18` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/mysql/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `components.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/components.sql` | `list` (1 named query) | *none* |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/constraints.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/ddl.sql` | `event_by_name`, `function_by_name`, `procedure_by_name`, `table_by_name`, `trigger_by_name`, `view_by_name` (6 named queries) | *none* |
| `events.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/events.sql` | `by_schema` (1 named query) | `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `partitions.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/partitions.sql` | `by_schema` (1 named query) | `:schema_name` |
| `plugins.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/plugins.sql` | `list` (1 named query) | *none* |
| `privileges.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/privileges.sql` | `by_schema` (1 named query) | `:object_name`, `:schema_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/routines.sql` | `by_schema` (1 named query) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/schemas.sql` | `list` (1 named query) | `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/system.sql` | `performance_schema_tables`, `sys_schema_table_statistics` (2 named queries) | `:schema_name` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `triggers.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/triggers.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/mysql/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
