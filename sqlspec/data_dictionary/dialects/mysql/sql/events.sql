-- name: by_schema
-- dialect: mysql
SELECT
    event_schema AS schema_name,
    event_name,
    definer,
    time_zone,
    event_body,
    event_definition,
    event_type,
    execute_at,
    interval_value,
    interval_field,
    sql_mode,
    starts,
    ends,
    status,
    on_completion,
    created,
    last_altered,
    last_executed,
    event_comment,
    originator,
    character_set_client,
    collation_connection,
    database_collation
FROM information_schema.events
WHERE event_schema = COALESCE(:schema_name, DATABASE())
ORDER BY event_schema, event_name;
