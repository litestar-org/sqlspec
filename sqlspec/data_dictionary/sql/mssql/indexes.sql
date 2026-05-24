-- name: indexes_by_table
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    CAST(i.is_unique AS BIT) AS is_unique,
    CAST(i.is_primary_key AS BIT) AS is_primary,
    STUFF((
        SELECT ',' + c2.name
        FROM sys.index_columns ic2
        INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE ic2.object_id = i.object_id
          AND ic2.index_id = i.index_id
          AND ic2.is_included_column = 0
        ORDER BY ic2.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS columns
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = :schema_name
  AND t.name = :table_name
  AND i.name IS NOT NULL
ORDER BY i.name;

-- name: indexes_by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    CAST(i.is_unique AS BIT) AS is_unique,
    CAST(i.is_primary_key AS BIT) AS is_primary,
    STUFF((
        SELECT ',' + c2.name
        FROM sys.index_columns ic2
        INNER JOIN sys.columns c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
        WHERE ic2.object_id = i.object_id
          AND ic2.index_id = i.index_id
          AND ic2.is_included_column = 0
        ORDER BY ic2.key_ordinal
        FOR XML PATH(''), TYPE
    ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS columns
FROM sys.indexes i
INNER JOIN sys.tables t ON i.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
  AND i.name IS NOT NULL
ORDER BY s.name, t.name, i.name;
