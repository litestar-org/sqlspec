-- name: by_schema
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM information_schema.tables
WHERE table_schema = :schema_name
ORDER BY table_schema, table_name;
