-- name: foreign_keys_by_table
-- dialect: mysql
SELECT
    table_name AS `table_name`,
    column_name AS `column_name`,
    referenced_table_name AS `referenced_table`,
    referenced_column_name AS `referenced_column`,
    constraint_name AS `constraint_name`,
    table_schema AS `schema`,
    referenced_table_schema AS `referenced_schema`
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
  AND table_name = :table_name
  AND table_schema = COALESCE(:schema_name, DATABASE());

-- name: foreign_keys_by_schema
-- dialect: mysql
SELECT
    table_name AS `table_name`,
    column_name AS `column_name`,
    referenced_table_name AS `referenced_table`,
    referenced_column_name AS `referenced_column`,
    constraint_name AS `constraint_name`,
    table_schema AS `schema`,
    referenced_table_schema AS `referenced_schema`
FROM information_schema.key_column_usage
WHERE referenced_table_name IS NOT NULL
  AND table_schema = COALESCE(:schema_name, DATABASE());
