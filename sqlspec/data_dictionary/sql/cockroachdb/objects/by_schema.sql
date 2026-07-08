-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_catalog::text AS catalog_name,
    table_schema::text AS schema_name,
    table_name::text AS object_name,
    CASE table_type
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        ELSE table_type::text
    END AS object_type,
    table_type::text AS native_object_type
FROM information_schema.tables
WHERE table_schema = :schema_name
ORDER BY table_schema, table_name;
