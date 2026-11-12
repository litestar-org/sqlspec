-- name: by_schema
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_schema AS "table_schema",
    table_name AS "table_name",
    table_type AS "table_type",
    is_transient AS "is_transient",
    clustering_key AS "clustering_key",
    row_count AS "row_count",
    bytes AS "bytes",
    retention_time AS "retention_time",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment",
    0 AS "dependency_level",
    0 AS "level"
FROM information_schema.tables
WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE', 'EVENT TABLE', 'DYNAMIC TABLE', 'EXTERNAL TABLE', 'HYBRID TABLE', 'ICEBERG TABLE')
  AND table_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
ORDER BY table_schema, table_name;

-- name: by_database
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_schema AS "table_schema",
    table_name AS "table_name",
    table_type AS "table_type",
    is_transient AS "is_transient",
    clustering_key AS "clustering_key",
    row_count AS "row_count",
    bytes AS "bytes",
    retention_time AS "retention_time",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment",
    0 AS "dependency_level",
    0 AS "level"
FROM information_schema.tables
WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE', 'EVENT TABLE', 'DYNAMIC TABLE', 'EXTERNAL TABLE', 'HYBRID TABLE', 'ICEBERG TABLE')
  AND UPPER(table_schema) <> 'INFORMATION_SCHEMA'
ORDER BY table_schema, table_name;

-- name: by_table
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_schema AS "table_schema",
    table_name AS "table_name",
    table_type AS "table_type",
    is_transient AS "is_transient",
    clustering_key AS "clustering_key",
    row_count AS "row_count",
    bytes AS "bytes",
    retention_time AS "retention_time",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment",
    0 AS "dependency_level",
    0 AS "level"
FROM information_schema.tables
WHERE UPPER(table_type) IN ('BASE TABLE', 'TABLE', 'EVENT TABLE', 'DYNAMIC TABLE', 'EXTERNAL TABLE', 'HYBRID TABLE', 'ICEBERG TABLE')
  AND table_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
  AND table_name = :table_name;
