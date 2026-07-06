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
