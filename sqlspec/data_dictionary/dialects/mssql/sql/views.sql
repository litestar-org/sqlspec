-- name: by_schema
-- dialect: mssql
SELECT
    DB_NAME() AS catalog_name,
    s.name AS schema_name,
    v.name AS view_name,
    CAST(v.is_ms_shipped AS BIT) AS is_system_object,
    sm.definition,
    CAST(sm.uses_ansi_nulls AS BIT) AS uses_ansi_nulls,
    CAST(sm.uses_quoted_identifier AS BIT) AS uses_quoted_identifier,
    v.create_date,
    v.modify_date
FROM sys.views AS v
INNER JOIN sys.schemas AS s ON v.schema_id = s.schema_id
LEFT JOIN sys.sql_modules AS sm ON v.object_id = sm.object_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND v.is_ms_shipped = 0
ORDER BY s.name, v.name;
