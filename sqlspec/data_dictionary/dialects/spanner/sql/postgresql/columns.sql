-- name: by_schema
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    is_generated,
    generation_expression
FROM information_schema.columns
WHERE (:schema_name::text IS NULL OR table_schema = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name, ordinal_position;

-- name: by_table
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    is_generated,
    generation_expression
FROM information_schema.columns
WHERE (:schema_name::text IS NULL OR table_schema = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name, ordinal_position;
