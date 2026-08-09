-- name: all_by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    'BASE TABLE' AS table_type,
    DB_NAME() AS table_catalog,
    t.create_date,
    t.modify_date,
    CAST(t.is_memory_optimized AS BIT) AS is_memory_optimized,
    CAST(t.temporal_type AS INT) AS temporal_type,
    t.temporal_type_desc
FROM sys.tables AS t
INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, t.name;

-- name: by_schema
-- dialect: mssql
WITH dependency_tree AS (
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        t.object_id,
        0 AS level,
        CAST(CONCAT('|', t.object_id, '|') AS NVARCHAR(MAX)) AS path
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND NOT EXISTS (
          SELECT 1
          FROM sys.foreign_keys AS fk
          WHERE fk.parent_object_id = t.object_id
      )

    UNION ALL

    SELECT
        child_schema.name AS schema_name,
        child_table.name AS table_name,
        child_table.object_id,
        dependency_tree.level + 1 AS level,
        CAST(CONCAT(dependency_tree.path, child_table.object_id, '|') AS NVARCHAR(MAX)) AS path
    FROM sys.foreign_keys AS fk
    INNER JOIN sys.tables AS child_table ON fk.parent_object_id = child_table.object_id
    INNER JOIN sys.schemas AS child_schema ON child_table.schema_id = child_schema.schema_id
    INNER JOIN dependency_tree ON fk.referenced_object_id = dependency_tree.object_id
    WHERE (:schema_name IS NULL OR child_schema.name = :schema_name)
      AND CHARINDEX(CONCAT('|', child_table.object_id, '|'), dependency_tree.path) = 0
)
SELECT
    schema_name,
    table_name,
    'BASE TABLE' AS table_type,
    DB_NAME() AS table_catalog,
    MAX(level) AS level
FROM dependency_tree
GROUP BY schema_name, table_name
ORDER BY level, schema_name, table_name;

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

-- name: names_by_schema
-- dialect: mssql
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.create_date,
    t.modify_date
FROM sys.tables t
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, t.name;
