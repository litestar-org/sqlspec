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
| **Catalog `.sql` Files** | `14` files (`30` named queries) |

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
| `supports_window_functions` | `True` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.bigquery.data_dictionary.BigQueryDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `bigquery` dialect pack defines `30` named queries across `14` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/bigquery/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_dataset`, `field_paths_by_dataset`, `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `constraints.sql` | `by_dataset` | `:schema_name`, `:table_name` |
| `datasets.sql` | `list` | `:dataset_name` |
| `ddl.sql` | `by_object`, `by_schema` | `:schema_name`, `:table_name` |
| `dependencies.sql` | `hints_by_dataset` | `:schema_name` |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `indexes.sql` | `search_by_dataset`, `vector_by_dataset`, `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `objects.sql` | `by_dataset` | `:schema_name`, `:table_name` |
| `privileges.sql` | `by_dataset` | `:object_name`, `:schema_name` |
| `routines.sql` | `by_dataset`, `options_by_dataset`, `parameters_by_dataset` | `:routine_name`, `:schema_name` |
| `system.sql` | `jobs_by_region`, `reservations_by_region`, `sessions_by_region` | `:created_after`, `:is_active`, `:limit`, `:reservation_name`, `:state`, `:user_email` |
| `tables.sql` | `options_by_dataset`, `partitions_by_dataset`, `storage_by_dataset`, `by_schema` | `:schema_name`, `:table_name` |
| `version.sql` | `current` | *none* |
| `views.sql` | `by_dataset`, `materialized_by_dataset` | `:schema_name`, `:view_name` |

??? code "`columns.sql` (4 named queries)"

    ```sql
    -- name: by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        ordinal_position,
        is_nullable,
        data_type,
        is_generated,
        generation_expression,
        is_stored,
        is_hidden,
        is_partitioning_column,
        clustering_ordinal_position,
        column_default,
        rounding_mode
    FROM {columns_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name, ordinal_position;

    -- name: field_paths_by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        column_name,
        field_path,
        data_type,
        description,
        collation_name,
        rounding_mode
    FROM {field_paths_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name, field_path;

    -- name: by_table
    -- dialect: bigquery
    SELECT
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM {schema_prefix}INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = :table_name
      AND (:schema_name IS NULL OR table_schema = :schema_name)
    ORDER BY ordinal_position;

    -- name: by_schema
    -- dialect: bigquery
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM {schema_prefix}INFORMATION_SCHEMA.COLUMNS
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
    ORDER BY table_name, ordinal_position;
    ```

??? code "`constraints.sql` (1 named queries)"

    ```sql
    -- name: by_dataset
    -- dialect: bigquery
    SELECT
        tc.constraint_catalog,
        tc.constraint_schema,
        tc.constraint_name,
        tc.table_name,
        tc.constraint_type,
        kcu.column_name,
        kcu.ordinal_position,
        COALESCE(pk_kcu.table_name, ccu.table_name) AS referenced_table_name,
        COALESCE(pk_kcu.column_name, ccu.column_name) AS referenced_column_name,
        rc.match_option,
        rc.update_rule,
        rc.delete_rule
    FROM {table_constraints_table} AS tc
    LEFT JOIN {key_column_usage_table} AS kcu
      ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name
    LEFT JOIN {referential_constraints_table} AS rc
      ON tc.constraint_catalog = rc.constraint_catalog
      AND tc.constraint_schema = rc.constraint_schema
      AND tc.constraint_name = rc.constraint_name
    LEFT JOIN {key_column_usage_table} AS pk_kcu
      ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
      AND rc.unique_constraint_name = pk_kcu.constraint_name
      AND COALESCE(kcu.position_in_unique_constraint, kcu.ordinal_position) = pk_kcu.ordinal_position
    LEFT JOIN {constraint_column_usage_table} AS ccu
      ON tc.constraint_catalog = ccu.constraint_catalog
      AND tc.constraint_schema = ccu.constraint_schema
      AND tc.constraint_name = ccu.constraint_name
      AND ccu.column_name = kcu.column_name
    WHERE (:schema_name IS NULL OR tc.constraint_schema = :schema_name)
      AND (:table_name IS NULL OR tc.table_name = :table_name)
    ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;
    ```

??? code "`datasets.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: bigquery
    SELECT
        s.catalog_name AS project_id,
        s.schema_name AS dataset_name,
        s.location,
        ARRAY_AGG(STRUCT(so.option_name, so.option_type, so.option_value) ORDER BY so.option_name) AS options
    FROM {schemata_table} AS s
    LEFT JOIN {schemata_options_table} AS so
      ON s.catalog_name = so.catalog_name
      AND s.schema_name = so.schema_name
    WHERE (:dataset_name IS NULL OR s.schema_name = :dataset_name)
    GROUP BY s.catalog_name, s.schema_name, s.location
    ORDER BY s.catalog_name, s.schema_name;
    ```

??? code "`ddl.sql` (2 named queries)"

    ```sql
    -- name: by_object
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type,
        table_name AS object_name,
        CASE table_type
            WHEN 'VIEW' THEN 'view'
            WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
            ELSE 'table'
        END AS object_type,
        ddl,
        'native' AS fidelity
    FROM {tables_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name;

    -- name: by_schema
    -- dialect: bigquery
    SELECT
        table_catalog AS catalog_name,
        table_schema AS schema_name,
        table_name AS object_name,
        CASE table_type
            WHEN 'VIEW' THEN 'view'
            WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
            ELSE 'table'
        END AS object_type,
        ddl,
        'native' AS fidelity
    FROM {schema_prefix}INFORMATION_SCHEMA.TABLES
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND ddl IS NOT NULL
    UNION ALL
    SELECT
        routine_catalog AS catalog_name,
        routine_schema AS schema_name,
        routine_name AS object_name,
        CASE UPPER(COALESCE(routine_type, 'FUNCTION'))
            WHEN 'PROCEDURE' THEN 'procedure'
            ELSE 'function'
        END AS object_type,
        ddl,
        'native' AS fidelity
    FROM {schema_prefix}INFORMATION_SCHEMA.ROUTINES
    WHERE (:schema_name IS NULL OR routine_schema = :schema_name)
      AND ddl IS NOT NULL
    ORDER BY schema_name, object_type, object_name;
    ```

??? code "`dependencies.sql` (1 named queries)"

    ```sql
    -- name: hints_by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name AS object_name,
        table_type AS object_type,
        ddl AS definition_text
    FROM {tables_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND ddl IS NOT NULL
    UNION ALL
    SELECT
        routine_catalog AS table_catalog,
        routine_schema AS table_schema,
        routine_name AS object_name,
        routine_type AS object_type,
        routine_definition AS definition_text
    FROM {routines_table}
    WHERE (:schema_name IS NULL OR routine_schema = :schema_name)
      AND routine_definition IS NOT NULL
    ORDER BY table_schema, object_name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: bigquery
    SELECT
        kcu.table_name,
        kcu.column_name,
        pk_kcu.table_name AS referenced_table_name,
        pk_kcu.column_name AS referenced_column_name,
        kcu.constraint_name,
        kcu.table_schema,
        pk_kcu.table_schema AS referenced_table_schema
    FROM {kcu_table} kcu
    JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
    JOIN {kcu_table} pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND kcu.ordinal_position = pk_kcu.ordinal_position
    WHERE kcu.table_name = :table_name
      AND (:schema_name IS NULL OR kcu.table_schema = :schema_name);

    -- name: by_schema
    -- dialect: bigquery
    SELECT
        kcu.table_name,
        kcu.column_name,
        pk_kcu.table_name AS referenced_table_name,
        pk_kcu.column_name AS referenced_column_name,
        kcu.constraint_name,
        kcu.table_schema,
        pk_kcu.table_schema AS referenced_table_schema
    FROM {kcu_table} kcu
    JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
    JOIN {kcu_table} pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND kcu.ordinal_position = pk_kcu.ordinal_position
    WHERE (:schema_name IS NULL OR kcu.table_schema = :schema_name);
    ```

??? code "`indexes.sql` (4 named queries)"

    ```sql
    -- name: search_by_dataset
    -- dialect: bigquery
    SELECT
        si.index_catalog AS schema_name,
        si.table_name,
        si.index_name,
        ARRAY_AGG(sic.index_field_path ORDER BY sic.index_field_path) AS columns,
        FALSE AS is_unique,
        FALSE AS is_primary
    FROM {search_indexes_table} AS si
    LEFT JOIN {search_index_columns_table} AS sic
      ON si.index_catalog = sic.index_catalog
      AND si.index_schema = sic.index_schema
      AND si.table_name = sic.table_name
      AND si.index_name = sic.index_name
    WHERE (:schema_name IS NULL OR si.index_schema = :schema_name)
      AND (:table_name IS NULL OR si.table_name = :table_name)
    GROUP BY si.index_catalog, si.table_name, si.index_name
    ORDER BY si.table_name, si.index_name;

    -- name: vector_by_dataset
    -- dialect: bigquery
    SELECT
        vi.index_catalog AS schema_name,
        vi.table_name,
        vi.index_name,
        ARRAY_AGG(vic.index_field_path ORDER BY vic.index_field_path) AS columns,
        FALSE AS is_unique,
        FALSE AS is_primary
    FROM {vector_indexes_table} AS vi
    LEFT JOIN {vector_index_columns_table} AS vic
      ON vi.index_catalog = vic.index_catalog
      AND vi.index_schema = vic.index_schema
      AND vi.table_name = vic.table_name
      AND vi.index_name = vic.index_name
    WHERE (:schema_name IS NULL OR vi.index_schema = :schema_name)
      AND (:table_name IS NULL OR vi.table_name = :table_name)
    GROUP BY vi.index_catalog, vi.table_name, vi.index_name
    ORDER BY vi.table_name, vi.index_name;

    -- name: by_table
    -- dialect: bigquery
    SELECT
        NULL AS index_name,
        NULL AS table_name,
        NULL AS is_unique,
        NULL AS is_primary,
        NULL AS columns
    WHERE FALSE;

    -- name: by_schema
    -- dialect: bigquery
    SELECT
        NULL AS index_name,
        NULL AS table_name,
        NULL AS is_unique,
        NULL AS is_primary,
        NULL AS columns
    WHERE FALSE;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type,
        creation_time,
        ddl
    FROM {tables_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name;
    ```

??? code "`privileges.sql` (1 named queries)"

    ```sql
    -- name: by_dataset
    -- dialect: bigquery
    SELECT
        object_catalog,
        object_schema,
        object_name,
        object_type,
        privilege_type,
        grantee
    FROM {object_privileges_table}
    WHERE (:schema_name IS NULL OR object_schema = :schema_name)
      AND (:object_name IS NULL OR object_name = :object_name)
    ORDER BY object_schema, object_name, privilege_type, grantee;
    ```

??? code "`routines.sql` (3 named queries)"

    ```sql
    -- name: by_dataset
    -- dialect: bigquery
    SELECT
        routine_catalog,
        routine_schema,
        routine_name,
        routine_type,
        data_type,
        routine_body,
        routine_definition,
        external_language,
        is_deterministic,
        security_type,
        created,
        last_altered
    FROM {routines_table}
    WHERE (:schema_name IS NULL OR routine_schema = :schema_name)
      AND (:routine_name IS NULL OR routine_name = :routine_name)
    ORDER BY routine_schema, routine_name;

    -- name: options_by_dataset
    -- dialect: bigquery
    SELECT
        specific_catalog,
        specific_schema,
        specific_name,
        option_name,
        option_type,
        option_value
    FROM {routine_options_table}
    WHERE (:schema_name IS NULL OR specific_schema = :schema_name)
      AND (:routine_name IS NULL OR specific_name = :routine_name)
    ORDER BY specific_schema, specific_name, option_name;

    -- name: parameters_by_dataset
    -- dialect: bigquery
    SELECT
        specific_catalog,
        specific_schema,
        specific_name,
        ordinal_position,
        parameter_mode,
        is_result,
        parameter_name,
        data_type,
        parameter_default
    FROM {parameters_table}
    WHERE (:schema_name IS NULL OR specific_schema = :schema_name)
      AND (:routine_name IS NULL OR specific_name = :routine_name)
    ORDER BY specific_schema, specific_name, ordinal_position;
    ```

??? code "`system.sql` (3 named queries)"

    ```sql
    -- name: jobs_by_region
    -- dialect: bigquery
    SELECT
        project_id,
        project_number,
        user_email,
        job_id,
        job_type,
        statement_type,
        priority,
        creation_time,
        start_time,
        end_time,
        state,
        reservation_id,
        total_bytes_processed,
        total_slot_ms,
        cache_hit,
        error_result,
        query
    FROM {jobs_table}
    WHERE (:user_email IS NULL OR user_email = :user_email)
      AND (:state IS NULL OR state = :state)
      AND (:created_after IS NULL OR creation_time >= :created_after)
    ORDER BY creation_time DESC
    LIMIT :limit;

    -- name: reservations_by_region
    -- dialect: bigquery
    SELECT
        project_id,
        project_number,
        reservation_name,
        slot_capacity,
        ignore_idle_slots,
        edition,
        autoscale,
        concurrency,
        creation_time,
        update_time
    FROM {reservations_table}
    WHERE (:reservation_name IS NULL OR reservation_name = :reservation_name)
    ORDER BY reservation_name;

    -- name: sessions_by_region
    -- dialect: bigquery
    SELECT
        project_id,
        project_number,
        user_email,
        session_id,
        creation_time,
        expiration_time,
        is_active
    FROM {sessions_table}
    WHERE (:user_email IS NULL OR user_email = :user_email)
      AND (:is_active IS NULL OR is_active = :is_active)
    ORDER BY creation_time DESC
    LIMIT :limit;
    ```

??? code "`tables.sql` (4 named queries)"

    ```sql
    -- name: options_by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        option_name,
        option_type,
        option_value
    FROM {table_options_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name, option_name;

    -- name: partitions_by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        partition_id,
        total_rows,
        total_logical_bytes,
        last_modified_time,
        storage_tier
    FROM {partitions_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name, partition_id;

    -- name: storage_by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        total_rows,
        total_partitions,
        total_logical_bytes,
        active_logical_bytes,
        long_term_logical_bytes,
        total_physical_bytes,
        active_physical_bytes,
        long_term_physical_bytes,
        time_travel_physical_bytes,
        storage_last_modified_time
    FROM {table_storage_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_schema, table_name;

    -- name: by_schema
    -- dialect: bigquery
    WITH RECURSIVE dependency_tree AS (
        SELECT
            t.table_name,
            0 AS level,
            [t.table_name] AS path
        FROM {tables_table} t
        WHERE t.table_type = 'BASE TABLE'
          AND (:schema_name IS NULL OR t.table_schema = :schema_name)
          AND NOT EXISTS (
              SELECT 1
              FROM {kcu_table} kcu
              JOIN {rc_table} rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
              JOIN {kcu_table} pk_kcu
                ON rc.unique_constraint_name = pk_kcu.constraint_name
                AND rc.unique_constraint_schema = pk_kcu.constraint_schema
                AND kcu.ordinal_position = pk_kcu.ordinal_position
              WHERE kcu.table_name = t.table_name
                AND pk_kcu.table_name <> t.table_name
                AND (:schema_name IS NULL OR kcu.table_schema = :schema_name)
          )

        UNION ALL

        SELECT
            kcu.table_name,
            dt.level + 1,
            ARRAY_CONCAT(dt.path, [kcu.table_name])
        FROM {kcu_table} kcu
        JOIN {rc_table} rc
          ON kcu.constraint_name = rc.constraint_name
          AND kcu.constraint_schema = rc.constraint_schema
        JOIN {kcu_table} pk_kcu
          ON rc.unique_constraint_name = pk_kcu.constraint_name
          AND rc.unique_constraint_schema = pk_kcu.constraint_schema
          AND kcu.ordinal_position = pk_kcu.ordinal_position
        JOIN dependency_tree dt ON pk_kcu.table_name = dt.table_name
        WHERE pk_kcu.table_name <> kcu.table_name
          AND kcu.table_name NOT IN UNNEST(dt.path)
          AND (:schema_name IS NULL OR kcu.table_schema = :schema_name)
    ),
    dependency_levels AS (
        SELECT
            table_name,
            MAX(level) AS dependency_level
        FROM dependency_tree
        GROUP BY table_name
    )
    SELECT
        t.table_name,
        COALESCE(dl.dependency_level, 0) AS dependency_level,
        COALESCE(dl.dependency_level, 0) AS level
    FROM {tables_table} t
    LEFT JOIN dependency_levels dl ON dl.table_name = t.table_name
    WHERE t.table_type = 'BASE TABLE'
      AND (:schema_name IS NULL OR t.table_schema = :schema_name)
    ORDER BY dependency_level, t.table_name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: bigquery
    SELECT NULL AS version;
    ```

??? code "`views.sql` (2 named queries)"

    ```sql
    -- name: by_dataset
    -- dialect: bigquery
    SELECT
        v.table_catalog,
        v.table_schema,
        v.table_name,
        v.view_definition,
        v.check_option,
        v.use_standard_sql
    FROM {views_table} AS v
    WHERE (:schema_name IS NULL OR v.table_schema = :schema_name)
      AND (:view_name IS NULL OR v.table_name = :view_name)
    ORDER BY v.table_schema, v.table_name;

    -- name: materialized_by_dataset
    -- dialect: bigquery
    SELECT
        table_catalog,
        table_schema,
        table_name,
        last_refresh_time,
        refresh_watermark,
        replica_source_catalog,
        replica_source_schema,
        replica_source_name,
        replication_status,
        replication_error
    FROM {materialized_views_table}
    WHERE (:schema_name IS NULL OR table_schema = :schema_name)
      AND (:view_name IS NULL OR table_name = :view_name)
    ORDER BY table_schema, table_name;
    ```
