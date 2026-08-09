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
          JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
          WHERE kcu.table_name = t.table_name
            AND (:schema_name IS NULL OR kcu.table_schema = :schema_name)
      )

    UNION ALL

    SELECT
        kcu.table_name,
        dt.level + 1,
        ARRAY_CONCAT(dt.path, [kcu.table_name])
    FROM {kcu_table} kcu
    JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
    JOIN {kcu_table} pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND kcu.ordinal_position = pk_kcu.ordinal_position
    JOIN dependency_tree dt ON pk_kcu.table_name = dt.table_name
    WHERE kcu.table_name NOT IN UNNEST(dt.path)
      AND (:schema_name IS NULL OR kcu.table_schema = :schema_name)
)
SELECT DISTINCT table_name
FROM dependency_tree
ORDER BY level, table_name;
