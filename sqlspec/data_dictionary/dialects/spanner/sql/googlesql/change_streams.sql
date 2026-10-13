-- name: list
-- dialect: spanner
SELECT
    CHANGE_STREAM_CATALOG AS change_stream_catalog,
    CHANGE_STREAM_SCHEMA AS change_stream_schema,
    CHANGE_STREAM_NAME AS change_stream_name,
    ALL_TABLES AS all_tables,
    CHANGE_STREAM_FOR_CLAUSE AS change_stream_for_clause
FROM INFORMATION_SCHEMA.CHANGE_STREAMS
WHERE (CAST(:schema_name AS STRING) IS NULL OR CHANGE_STREAM_SCHEMA = :schema_name)
  AND (CAST(:change_stream_name AS STRING) IS NULL OR CHANGE_STREAM_NAME = :change_stream_name)
ORDER BY CHANGE_STREAM_SCHEMA, CHANGE_STREAM_NAME;

-- name: tables
-- dialect: spanner
SELECT
    CHANGE_STREAM_CATALOG AS change_stream_catalog,
    CHANGE_STREAM_SCHEMA AS change_stream_schema,
    CHANGE_STREAM_NAME AS change_stream_name,
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS table_schema,
    TABLE_NAME AS table_name,
    ALL_COLUMNS AS all_columns,
    COLUMN_NAMES AS column_names
FROM INFORMATION_SCHEMA.CHANGE_STREAM_TABLES
WHERE (CAST(:schema_name AS STRING) IS NULL OR CHANGE_STREAM_SCHEMA = :schema_name)
  AND (CAST(:change_stream_name AS STRING) IS NULL OR CHANGE_STREAM_NAME = :change_stream_name)
ORDER BY CHANGE_STREAM_SCHEMA, CHANGE_STREAM_NAME, TABLE_NAME;
