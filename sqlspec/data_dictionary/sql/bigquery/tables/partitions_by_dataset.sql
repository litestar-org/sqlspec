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
