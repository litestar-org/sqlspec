-- name: performance_schema_tables
-- dialect: mysql
SELECT
    table_schema,
    table_name,
    table_rows,
    avg_row_length,
    data_length,
    index_length
FROM information_schema.tables
WHERE table_schema = 'performance_schema'
ORDER BY table_name;

-- name: sys_schema_table_statistics
-- dialect: mysql
SELECT
    table_schema,
    table_name,
    rows_fetched,
    rows_inserted,
    rows_updated,
    rows_deleted
FROM sys.schema_table_statistics
WHERE :schema_name IS NULL OR table_schema = :schema_name
ORDER BY table_schema, table_name;
