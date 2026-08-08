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

-- name: indexes_by_table
-- dialect: duckdb
SELECT
    NULL AS index_name,
    NULL AS table_name,
    NULL AS is_unique,
    NULL AS is_primary,
    NULL AS columns
WHERE FALSE;

-- name: indexes_by_schema
-- dialect: duckdb
SELECT
    NULL AS index_name,
    NULL AS table_name,
    NULL AS is_unique,
    NULL AS is_primary,
    NULL AS columns
WHERE FALSE;
