-- name: by_schema
-- dialect: snowflake
WITH target_schema AS (
    SELECT COALESCE(:schema_name, CURRENT_SCHEMA()) AS schema_name
)
SELECT
    t.table_catalog AS "catalog_name",
    t.table_schema AS "schema_name",
    t.table_name AS "object_name",
    CASE UPPER(t.table_type)
        WHEN 'BASE TABLE' THEN 'table'
        WHEN 'TABLE' THEN 'table'
        WHEN 'VIEW' THEN 'view'
        WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
        WHEN 'EXTERNAL TABLE' THEN 'external_table'
        WHEN 'EVENT TABLE' THEN 'event_table'
        WHEN 'DYNAMIC TABLE' THEN 'dynamic_table'
        ELSE LOWER(REPLACE(t.table_type, ' ', '_'))
    END AS "object_type",
    t.table_owner AS "owner_name",
    t.comment AS "comment"
FROM information_schema.tables t
JOIN target_schema ts ON t.table_schema = ts.schema_name
UNION ALL
SELECT
    s.sequence_catalog AS "catalog_name",
    s.sequence_schema AS "schema_name",
    s.sequence_name AS "object_name",
    'sequence' AS "object_type",
    s.sequence_owner AS "owner_name",
    s.comment AS "comment"
FROM information_schema.sequences s
JOIN target_schema ts ON s.sequence_schema = ts.schema_name
ORDER BY 2, 4, 3;
