-- name: by_schema
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    type_name,
    logical_type,
    labels
FROM duckdb_types()
WHERE NOT internal
  AND schema_name = COALESCE(:schema_name, current_schema())
ORDER BY type_name;
