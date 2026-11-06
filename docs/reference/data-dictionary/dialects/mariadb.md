# MariaDB Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("mariadb")
```

Extends MySQL `information_schema` catalog queries with MariaDB native `SEQUENCE` objects (`TABLE_TYPE = 'SEQUENCE'`), native `UUID` / `VECTOR` types, and MariaDB version detection. See the [MariaDB Guide](../../../databases/mysql.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `mariadb` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `SHOW CREATE` + MariaDB sequence & constraint reflection |
| **Catalog `.sql` Files** | `17` files (27 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `BIGINT` |
| `supports_for_update` | `True` | `blob` | `BLOB` |
| `supports_invisible_columns` | `False` | `boolean` | `TINYINT(1)` |
| `supports_invisible_indexes` | `False` | `float` | `DOUBLE` |
| `supports_on_conflict` | `False` | `integer` | `INT` |
| `supports_prepared_statements` | `True` | `json` | `JSON` |
| `supports_resource_groups` | `False` | `text` | `TEXT` |
| `supports_returning` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_schemas` | `True` | `uuid` | `VARCHAR(36)` |
| `supports_transactions` | `True` | `varchar` | `VARCHAR({length})` |
| `supports_update_from` | `False` | - | - |
| `supports_upsert` | `True` | - | - |
| `supports_uuid` | `False` | - | - |
| `supports_check_constraints` | `>= 10.2.1` | - | - |
| `supports_cte` | `>= 10.2.1` | - | - |
| `supports_events` | `>= 5.1.6` | - | - |
| `supports_generated_columns` | `>= 10.2.0` | - | - |
| `supports_json` | `>= 10.2.7` | - | - |
| `supports_roles` | `>= 10.0.5` | - | - |
| `supports_sequences` | `>= 10.3.0` | - | - |
| `supports_skip_locked` | `>= 10.6.0` | - | - |
| `supports_system_versioned_tables` | `>= 10.3.0` | - | - |
| `supports_vectors` | `>= 11.7.0` | - | - |
| `supports_window_functions` | `>= 10.2.0` | - | - |

## Catalog SQL Queries

The `mariadb` dialect pack defines 27 named queries across `17` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/mariadb/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/columns.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/constraints.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/ddl.sql` | `event_by_name`, `function_by_name`, `procedure_by_name`, `sequence_by_name`, `table_by_name`, `trigger_by_name`, `view_by_name` (7 named queries) | *none* |
| `events.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/events.sql` | `by_schema` (1 named query) | `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/indexes.sql` | `by_schema`, `by_table` (2 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/objects.sql` | `by_schema` (1 named query) | `:schema_name` |
| `partitions.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/partitions.sql` | `by_schema` (1 named query) | `:schema_name` |
| `plugins.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/plugins.sql` | `list` (1 named query) | *none* |
| `privileges.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/privileges.sql` | `by_schema` (1 named query) | `:schema_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/routines.sql` | `by_schema` (1 named query) | `:schema_name` |
| `schemas.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/schemas.sql` | `list` (1 named query) | `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/system.sql` | `performance_schema_tables` (1 named query) | *none* |
| `tables.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/tables.sql` | `by_schema` (1 named query) | `:schema_name`, `:table_name` |
| `triggers.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/triggers.sql` | `by_schema` (1 named query) | `:schema_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/version.sql` | `current`, `version` (2 named queries) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/mariadb/sql/views.sql` | `by_schema` (1 named query) | `:schema_name` |
