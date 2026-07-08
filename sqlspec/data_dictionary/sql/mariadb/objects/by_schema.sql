-- name: by_schema
-- dialect: mysql
SELECT
    table_schema AS object_schema,
    table_name AS object_name,
    CASE
        WHEN table_type = 'SEQUENCE' THEN 'SEQUENCE'
        ELSE table_type
    END AS object_type,
    engine,
    table_comment AS comment
FROM information_schema.tables
WHERE table_schema = COALESCE(:schema_name, DATABASE())
UNION ALL
SELECT
    sequence_schema AS object_schema,
    sequence_name AS object_name,
    'SEQUENCE' AS object_type,
    NULL AS engine,
    NULL AS comment
FROM information_schema.sequences
WHERE sequence_schema = COALESCE(:schema_name, DATABASE())
UNION ALL
SELECT
    routine_schema AS object_schema,
    routine_name AS object_name,
    routine_type AS object_type,
    NULL AS engine,
    routine_comment AS comment
FROM information_schema.routines
WHERE routine_schema = COALESCE(:schema_name, DATABASE())
UNION ALL
SELECT
    trigger_schema AS object_schema,
    trigger_name AS object_name,
    'TRIGGER' AS object_type,
    NULL AS engine,
    NULL AS comment
FROM information_schema.triggers
WHERE trigger_schema = COALESCE(:schema_name, DATABASE())
UNION ALL
SELECT
    event_schema AS object_schema,
    event_name AS object_name,
    'EVENT' AS object_type,
    NULL AS engine,
    event_comment AS comment
FROM information_schema.events
WHERE event_schema = COALESCE(:schema_name, DATABASE())
ORDER BY object_schema, object_name, object_type;
