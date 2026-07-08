-- name: by_schema
-- dialect: mysql
SELECT
    trigger_schema AS schema_name,
    trigger_name,
    event_manipulation,
    event_object_schema,
    event_object_table,
    action_order,
    action_condition,
    action_statement,
    action_orientation,
    action_timing,
    sql_mode,
    definer,
    character_set_client,
    collation_connection,
    database_collation
FROM information_schema.triggers
WHERE trigger_schema = COALESCE(:schema_name, DATABASE())
ORDER BY trigger_schema, event_object_table, action_timing, event_manipulation, action_order;
