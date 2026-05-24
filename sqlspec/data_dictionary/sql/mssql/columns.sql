-- name: columns_by_table
-- dialect: mssql
SELECT
    s.name AS schema_name,
    tab.name AS table_name,
    c.column_id AS ordinal_position,
    c.name AS column_name,
    typ.name AS data_type,
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
    OBJECT_DEFINITION(c.default_object_id) AS column_default,
    c.max_length,
    c.precision AS numeric_precision,
    c.scale AS numeric_scale,
    CAST(CASE WHEN pk_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_primary,
    CAST(CASE WHEN unique_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_unique
FROM sys.columns c
INNER JOIN sys.types typ ON c.user_type_id = typ.user_type_id
INNER JOIN sys.tables tab ON c.object_id = tab.object_id
INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
OUTER APPLY (
    SELECT TOP 1 ic.column_id
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE i.object_id = tab.object_id
      AND i.is_primary_key = 1
      AND ic.column_id = c.column_id
) pk_cols
OUTER APPLY (
    SELECT TOP 1 ic.column_id
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE i.object_id = tab.object_id
      AND i.is_unique = 1
      AND ic.column_id = c.column_id
) unique_cols
WHERE s.name = :schema_name
  AND tab.name = :table_name
ORDER BY c.column_id;

-- name: columns_by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    tab.name AS table_name,
    c.column_id AS ordinal_position,
    c.name AS column_name,
    typ.name AS data_type,
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
    OBJECT_DEFINITION(c.default_object_id) AS column_default,
    c.max_length,
    c.precision AS numeric_precision,
    c.scale AS numeric_scale,
    CAST(CASE WHEN pk_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_primary,
    CAST(CASE WHEN unique_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_unique
FROM sys.columns c
INNER JOIN sys.types typ ON c.user_type_id = typ.user_type_id
INNER JOIN sys.tables tab ON c.object_id = tab.object_id
INNER JOIN sys.schemas s ON tab.schema_id = s.schema_id
OUTER APPLY (
    SELECT TOP 1 ic.column_id
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE i.object_id = tab.object_id
      AND i.is_primary_key = 1
      AND ic.column_id = c.column_id
) pk_cols
OUTER APPLY (
    SELECT TOP 1 ic.column_id
    FROM sys.indexes i
    INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
    WHERE i.object_id = tab.object_id
      AND i.is_unique = 1
      AND ic.column_id = c.column_id
) unique_cols
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, tab.name, c.column_id;
