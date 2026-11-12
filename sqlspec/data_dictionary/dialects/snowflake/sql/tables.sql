-- name: by_schema
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_schema AS "table_schema",
    table_name AS "table_name",
    table_type AS "table_type",
    0 AS "dependency_level",
    0 AS "level"
FROM information_schema.tables
WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE', 'EVENT TABLE', 'DYNAMIC TABLE', 'EXTERNAL TABLE', 'HYBRID TABLE', 'ICEBERG TABLE')
  AND UPPER(table_schema) = UPPER(COALESCE(:schema_name, CURRENT_SCHEMA()))
ORDER BY table_schema, table_name;

-- name: by_table
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_schema AS "table_schema",
    table_name AS "table_name",
    table_type AS "table_type",
    0 AS "dependency_level",
    0 AS "level"
FROM information_schema.tables
WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE', 'EVENT TABLE', 'DYNAMIC TABLE', 'EXTERNAL TABLE', 'HYBRID TABLE', 'ICEBERG TABLE')
  AND UPPER(table_schema) = UPPER(COALESCE(:schema_name, CURRENT_SCHEMA()))
  AND UPPER(table_name) = UPPER(:table_name);
