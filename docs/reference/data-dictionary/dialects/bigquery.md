# Google BigQuery Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("bigquery")
```

Introspects dataset-scoped `INFORMATION_SCHEMA` views (`TABLES`, `COLUMNS`, `COLUMN_FIELD_PATHS`, `TABLE_OPTIONS`, `TABLE_CONSTRAINTS`, `KEY_COLUMN_USAGE`, `CONSTRAINT_COLUMN_USAGE`, `VIEWS`, `ROUTINES`). See the [Google BigQuery Guide](../../../databases/bigquery.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `bigquery` |
| **Default Schema** | `None` |
| **Version Pattern** | `.*` |
| **DDL Strategy** | Native `INFORMATION_SCHEMA.TABLES.ddl` + AST fallback |
| **Catalog `.sql` Files** | `14` files (30 named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_clustering` | `True` | `bigint` | `INT64` |
| `supports_cte` | `True` | `bignumeric` | `BIGNUMERIC` |
| `supports_for_update` | `False` | `blob` | `BYTES` |
| `supports_geography` | `True` | `boolean` | `BOOL` |
| `supports_json` | `True` | `float` | `FLOAT64` |
| `supports_on_conflict` | `False` | `geography` | `GEOGRAPHY` |
| `supports_partitioning` | `True` | `integer` | `INT64` |
| `supports_prepared_statements` | `True` | `json` | `JSON` |
| `supports_returning` | `False` | `numeric` | `NUMERIC` |
| `supports_schemas` | `True` | `struct` | `STRUCT` |
| `supports_skip_locked` | `False` | `text` | `STRING` |
| `supports_structs` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_transactions` | `True` | `uuid` | `STRING` |
| `supports_update_from` | `True` | `varchar` | `STRING` |
| `supports_upsert` | `True` | - | - |
| `supports_uuid` | `False` | - | - |
| `supports_vectors` | `True` | - | - |
| `supports_window_functions` | `True` | - | - |

## Adapter Data Dictionary Classes

- [`BigQueryDataDictionary`](../../adapters/bigquery.md)

## Catalog SQL Queries

The `bigquery` dialect pack defines 30 named queries across `14` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/bigquery/`):

| Domain Pack | Source File | Named Queries | Bind Parameters |
| --- | --- | --- | --- |
| `columns.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/columns.sql` | `by_dataset`, `field_paths_by_dataset`, `by_table`, `by_schema` (4 named queries) | `:schema_name`, `:table_name` |
| `constraints.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/constraints.sql` | `by_dataset` (1 named query) | `:schema_name`, `:table_name` |
| `datasets.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/datasets.sql` | `list` (1 named query) | `:dataset_name` |
| `ddl.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/ddl.sql` | `by_object`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `dependencies.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/dependencies.sql` | `hints_by_dataset` (1 named query) | `:schema_name` |
| `foreign_keys.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/foreign_keys.sql` | `by_table`, `by_schema` (2 named queries) | `:schema_name`, `:table_name` |
| `indexes.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/indexes.sql` | `search_by_dataset`, `vector_by_dataset`, `by_table`, `by_schema` (4 named queries) | `:schema_name`, `:table_name` |
| `objects.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/objects.sql` | `by_dataset` (1 named query) | `:schema_name`, `:table_name` |
| `privileges.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/privileges.sql` | `by_dataset` (1 named query) | `:object_name`, `:schema_name` |
| `routines.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/routines.sql` | `by_dataset`, `options_by_dataset`, `parameters_by_dataset` (3 named queries) | `:routine_name`, `:schema_name` |
| `system.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/system.sql` | `jobs_by_region`, `reservations_by_region`, `sessions_by_region` (3 named queries) | `:created_after`, `:is_active`, `:limit`, `:reservation_name`, `:state`, `:user_email` |
| `tables.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/tables.sql` | `options_by_dataset`, `partitions_by_dataset`, `storage_by_dataset`, `by_schema` (4 named queries) | `:schema_name`, `:table_name` |
| `version.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/version.sql` | `current` (1 named query) | *none* |
| `views.sql` | `sqlspec/data_dictionary/dialects/bigquery/sql/views.sql` | `by_dataset`, `materialized_by_dataset` (2 named queries) | `:schema_name`, `:view_name` |
