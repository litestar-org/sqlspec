-- name: by_schema
-- dialect: mssql
SELECT
    DB_NAME() AS catalog_name,
    s.name AS schema_name,
    o.name AS object_name,
    o.type AS object_type,
    o.type_desc,
    sm.definition,
    OBJECT_DEFINITION(o.object_id) AS object_definition,
    CAST(sm.uses_ansi_nulls AS BIT) AS uses_ansi_nulls,
    CAST(sm.uses_quoted_identifier AS BIT) AS uses_quoted_identifier,
    CAST(sm.is_schema_bound AS BIT) AS is_schema_bound
FROM sys.sql_modules AS sm
INNER JOIN sys.objects AS o ON sm.object_id = o.object_id
INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND o.is_ms_shipped = 0
ORDER BY s.name, o.name;
