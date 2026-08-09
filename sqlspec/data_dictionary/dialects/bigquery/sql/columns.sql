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
