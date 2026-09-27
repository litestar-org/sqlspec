-- name: list
-- dialect: spanner
SELECT
    change_stream_catalog,
    change_stream_schema,
    change_stream_name,
    all_tables,
    change_stream_for_clause
FROM information_schema.change_streams
WHERE (:schema_name::text IS NULL OR change_stream_schema = :schema_name)
  AND (:change_stream_name::text IS NULL OR change_stream_name = :change_stream_name)
ORDER BY change_stream_schema, change_stream_name;

-- name: tables
-- dialect: spanner
SELECT
    change_stream_catalog,
    change_stream_schema,
    change_stream_name,
    table_catalog,
    table_schema,
    table_name,
    all_columns,
    column_names
FROM information_schema.change_stream_tables
WHERE (:schema_name::text IS NULL OR change_stream_schema = :schema_name)
  AND (:change_stream_name::text IS NULL OR change_stream_name = :change_stream_name)
ORDER BY change_stream_schema, change_stream_name, table_name;
