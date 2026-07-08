-- name: parameters_by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    o.name AS routine_name,
    p.parameter_id,
    p.name AS parameter_name,
    typ.name AS data_type,
    p.max_length,
    p.precision AS numeric_precision,
    p.scale AS numeric_scale,
    CAST(p.is_output AS BIT) AS is_output,
    CAST(p.has_default_value AS BIT) AS has_default_value,
    CONVERT(NVARCHAR(MAX), p.default_value) AS default_value
FROM sys.parameters AS p
INNER JOIN sys.objects AS o ON p.object_id = o.object_id
INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
INNER JOIN sys.types AS typ ON p.user_type_id = typ.user_type_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND o.is_ms_shipped = 0
ORDER BY s.name, o.name, p.parameter_id;
