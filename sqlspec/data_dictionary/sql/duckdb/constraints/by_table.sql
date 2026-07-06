-- name: by_table
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    table_name,
    constraint_name,
    constraint_type,
    constraint_text,
    expression,
    constraint_column_names,
    referenced_table,
    referenced_column_names
FROM duckdb_constraints()
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND table_name = :table_name
ORDER BY constraint_index;
