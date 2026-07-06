-- name: by_schema
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
ORDER BY table_name, index_name;
