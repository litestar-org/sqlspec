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
