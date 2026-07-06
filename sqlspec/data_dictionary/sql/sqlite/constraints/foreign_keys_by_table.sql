-- name: foreign_keys_by_table
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
