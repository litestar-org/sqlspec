-- name: by_table
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    table_name,
    column_name,
    column_index AS ordinal_position,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length AS max_length,
    numeric_precision,
    numeric_scale,
    comment,
    internal
FROM duckdb_columns()
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND table_name = :table_name
  AND NOT internal
ORDER BY column_index;
