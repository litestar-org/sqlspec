-- name: by_schema
-- dialect: mssql
SELECT
    DB_NAME() AS catalog_name,
    s.name AS schema_name,
    o.name AS object_name,
    o.type AS object_type,
    o.type_desc,
    o.create_date,
    o.modify_date,
    CAST(o.is_ms_shipped AS BIT) AS is_system_object
FROM sys.objects AS o
INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND o.is_ms_shipped = 0
ORDER BY s.name, o.name;
