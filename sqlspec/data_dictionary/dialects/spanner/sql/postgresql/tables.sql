-- name: by_schema
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE (:schema_name::text IS NULL OR table_schema = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name;

-- name: by_table
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE (:schema_name::text IS NULL OR table_schema = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name;
