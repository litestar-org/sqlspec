-- name: by_table
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    table_name,
    index_name,
    is_unique,
    is_primary,
    expressions AS columns,
    comment,
    tags,
    sql AS native_sql
FROM duckdb_indexes()
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND table_name = :table_name
ORDER BY index_name;
