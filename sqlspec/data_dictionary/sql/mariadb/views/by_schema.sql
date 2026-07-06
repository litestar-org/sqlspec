-- name: by_schema
-- dialect: mysql
SELECT
    table_schema AS schema_name,
    table_name AS view_name,
    view_definition,
    check_option,
    is_updatable,
    definer,
    security_type,
    character_set_client,
    collation_connection
FROM information_schema.views
WHERE table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY table_schema, table_name;
