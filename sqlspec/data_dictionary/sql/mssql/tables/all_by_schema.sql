-- name: all_by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    'BASE TABLE' AS table_type,
    DB_NAME() AS table_catalog,
    t.create_date,
    t.modify_date,
    CAST(t.is_memory_optimized AS BIT) AS is_memory_optimized,
    CAST(t.temporal_type AS INT) AS temporal_type,
    t.temporal_type_desc
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, t.name;
