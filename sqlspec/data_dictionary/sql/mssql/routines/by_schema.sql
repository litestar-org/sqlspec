-- name: by_schema
-- dialect: mssql
SELECT
    DB_NAME() AS catalog_name,
    s.name AS schema_name,
    p.name AS routine_name,
    p.type AS routine_type,
    p.type_desc,
    sm.definition,
    p.create_date,
    p.modify_date
FROM sys.procedures AS p
INNER JOIN sys.schemas AS s ON p.schema_id = s.schema_id
LEFT JOIN sys.sql_modules AS sm ON p.object_id = sm.object_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND p.is_ms_shipped = 0
ORDER BY s.name, p.name;
