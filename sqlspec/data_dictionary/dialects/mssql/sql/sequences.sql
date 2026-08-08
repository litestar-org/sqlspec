-- name: by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    seq.name AS sequence_name,
    typ.name AS data_type,
    CONVERT(NVARCHAR(128), seq.start_value) AS start_value,
    CONVERT(NVARCHAR(128), seq.increment) AS increment_value,
    CONVERT(NVARCHAR(128), seq.minimum_value) AS minimum_value,
    CONVERT(NVARCHAR(128), seq.maximum_value) AS maximum_value,
    CAST(seq.is_cycling AS BIT) AS is_cycling,
    CAST(seq.is_cached AS BIT) AS is_cached,
    seq.cache_size
FROM sys.sequences AS seq
INNER JOIN sys.schemas AS s ON seq.schema_id = s.schema_id
INNER JOIN sys.types AS typ ON seq.user_type_id = typ.user_type_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, seq.name;
