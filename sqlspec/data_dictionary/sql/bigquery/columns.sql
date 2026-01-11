-- name: columns_by_table
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

-- name: columns_by_schema
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
