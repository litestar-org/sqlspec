-- name: foreign_keys_by_table
-- dialect: mysql
SELECT
    table_name,
    column_name,
    referenced_table_name,
    referenced_column_name,
    constraint_name,
    table_schema,
    referenced_table_schema
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
  AND table_name = :table_name
  AND table_schema = COALESCE(:schema_name, DATABASE());

-- name: foreign_keys_by_schema
-- dialect: mysql
SELECT
    table_name,
    column_name,
    referenced_table_name,
    referenced_column_name,
    constraint_name,
    table_schema,
    referenced_table_schema
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
  AND table_schema = COALESCE(:schema_name, DATABASE());
