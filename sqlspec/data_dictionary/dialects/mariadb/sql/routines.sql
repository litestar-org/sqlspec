-- name: by_schema
-- dialect: mysql
SELECT
    routine_schema AS schema_name,
    routine_name,
    routine_type,
    data_type,
    routine_definition,
    external_language,
    is_deterministic,
    sql_data_access,
    security_type,
    created,
    last_altered,
    definer,
    character_set_client,
    collation_connection,
    database_collation
FROM information_schema.routines
WHERE routine_schema = COALESCE(:schema_name, DATABASE())
ORDER BY routine_schema, routine_name;
