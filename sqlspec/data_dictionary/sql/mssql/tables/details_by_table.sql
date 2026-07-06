-- name: details_by_table
-- dialect: mssql
SELECT
    DB_NAME() AS catalog_name,
    s.name AS schema_name,
    t.name AS table_name,
    'BASE TABLE' AS table_type,
    t.create_date,
    t.modify_date,
    CAST(t.is_memory_optimized AS BIT) AS is_memory_optimized,
    CAST(t.durability AS INT) AS durability,
    t.durability_desc,
    CAST(t.temporal_type AS INT) AS temporal_type,
    t.temporal_type_desc,
    history_schema.name AS history_schema_name,
    history_table.name AS history_table_name
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
LEFT JOIN sys.tables AS history_table ON t.history_table_id = history_table.object_id
LEFT JOIN sys.schemas AS history_schema ON history_table.schema_id = history_schema.schema_id
WHERE s.name = :schema_name
  AND t.name = :table_name
ORDER BY s.name, t.name;
