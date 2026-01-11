-- name: columns_by_table
-- dialect: spanner
SELECT
    column_name,
    spanner_type AS data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = :table_name
  AND (:schema_name IS NULL OR table_schema = :schema_name)
ORDER BY ordinal_position;

-- name: columns_by_schema
-- dialect: spanner
SELECT
    table_name,
    column_name,
    spanner_type AS data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE (:schema_name IS NULL OR table_schema = :schema_name)
ORDER BY table_name, ordinal_position;
