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
