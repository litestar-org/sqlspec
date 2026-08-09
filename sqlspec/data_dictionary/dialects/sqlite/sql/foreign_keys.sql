-- name: by_schema
-- dialect: sqlite
SELECT
    tl.schema AS schema_name,
    tl.name AS table_name,
    fk."from" AS column_name,
    fk."table" AS referenced_table_name,
    fk."to" AS referenced_column_name,
    fk.id AS constraint_name,
    fk.seq AS ordinal_position,
    fk.on_update,
    fk.on_delete,
    fk.match
FROM pragma_table_list AS tl
JOIN pragma_foreign_key_list(tl.name, COALESCE(:schema_name, 'main')) AS fk
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
ORDER BY tl.name, fk.id, fk.seq;

-- name: by_table
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    :table_name AS table_name,
    fk."from" AS column_name,
    fk."table" AS referenced_table_name,
    fk."to" AS referenced_column_name,
    fk.id AS constraint_name,
    fk.seq AS ordinal_position,
    fk.on_update,
    fk.on_delete,
    fk.match
FROM pragma_foreign_key_list(:table_name, :schema_name) AS fk
ORDER BY fk.id, fk.seq;
