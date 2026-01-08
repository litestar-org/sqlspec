-- name: foreign_keys_by_table
-- dialect: sqlite
SELECT
    fk.*
FROM pragma_foreign_key_list({table_name}) AS fk;

-- name: foreign_keys_by_schema
-- dialect: sqlite
SELECT
    m.name AS table_name,
    fk.*
FROM {schema_prefix}sqlite_schema m
JOIN pragma_foreign_key_list(m.name) AS fk
WHERE m.type = 'table'
  AND m.name NOT LIKE 'sqlite_%';
