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
