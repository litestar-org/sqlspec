-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_schema::text AS schema_name,
    table_name::text AS object_name,
    'table'::text AS object_type,
    NULL::text AS comment
FROM information_schema.tables
WHERE table_schema = :schema_name
ORDER BY table_schema, table_name;
