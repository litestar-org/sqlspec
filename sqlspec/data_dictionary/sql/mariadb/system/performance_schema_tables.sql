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
