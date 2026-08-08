-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_schema::text AS schema_name,
    table_name::text AS view_name,
    view_definition::text AS definition,
    check_option::text AS check_option,
    is_updatable::text AS is_updatable
FROM information_schema.views
WHERE table_schema = :schema_name
ORDER BY table_schema, table_name;
