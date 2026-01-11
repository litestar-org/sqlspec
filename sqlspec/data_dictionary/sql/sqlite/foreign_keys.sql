-- name: foreign_keys_by_table
-- dialect: sqlite
SELECT
    '{table_label}' AS table_name,
    fk."from" AS column_name,
    fk."table" AS referenced_table_name,
    fk."to" AS referenced_column_name,
    fk.id AS constraint_name
FROM pragma_foreign_key_list({table_name}) AS fk;

-- name: foreign_keys_by_schema
-- dialect: sqlite
SELECT
    m.name AS table_name,
    fk."from" AS column_name,
    fk."table" AS referenced_table_name,
    fk."to" AS referenced_column_name,
    fk.id AS constraint_name
FROM {schema_prefix}sqlite_schema m
JOIN pragma_foreign_key_list(m.name) AS fk
WHERE m.type = 'table'
  AND m.name NOT LIKE 'sqlite_%';
