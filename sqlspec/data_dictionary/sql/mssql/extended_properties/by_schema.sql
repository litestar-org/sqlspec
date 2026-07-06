-- name: by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    o.name AS object_name,
    o.type_desc AS object_type,
    c.name AS column_name,
    ep.name AS property_name,
    CONVERT(NVARCHAR(MAX), ep.value) AS property_value,
    ep.class_desc
FROM sys.extended_properties AS ep
LEFT JOIN sys.objects AS o ON ep.major_id = o.object_id
LEFT JOIN sys.schemas AS s ON o.schema_id = s.schema_id
LEFT JOIN sys.columns AS c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, o.name, c.name, ep.name;
