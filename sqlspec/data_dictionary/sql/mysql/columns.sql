-- name: columns_by_table
-- dialect: mysql
SELECT
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_name = :table_name
  AND table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY ordinal_position;

-- name: columns_by_schema
-- dialect: mysql
SELECT
    table_name,
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY table_name, ordinal_position;
