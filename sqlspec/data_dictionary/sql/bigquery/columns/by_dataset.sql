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
