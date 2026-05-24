-- name: tables_by_schema
-- dialect: mssql
WITH dependency_tree AS (
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        t.object_id,
        0 AS level,
        CAST(CONCAT('|', t.object_id, '|') AS NVARCHAR(MAX)) AS path
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND NOT EXISTS (
          SELECT 1
          FROM sys.foreign_keys fk
          WHERE fk.parent_object_id = t.object_id
      )

    UNION ALL

    SELECT
        child_schema.name AS schema_name,
        child_table.name AS table_name,
        child_table.object_id,
        dependency_tree.level + 1 AS level,
        CAST(CONCAT(dependency_tree.path, child_table.object_id, '|') AS NVARCHAR(MAX)) AS path
    FROM sys.foreign_keys fk
    INNER JOIN sys.tables child_table ON fk.parent_object_id = child_table.object_id
    INNER JOIN sys.schemas child_schema ON child_table.schema_id = child_schema.schema_id
    INNER JOIN dependency_tree ON fk.referenced_object_id = dependency_tree.object_id
    WHERE (:schema_name IS NULL OR child_schema.name = :schema_name)
      AND CHARINDEX(CONCAT('|', child_table.object_id, '|'), dependency_tree.path) = 0
)
SELECT
    schema_name,
    table_name,
    MAX(level) AS level
FROM dependency_tree
GROUP BY schema_name, table_name
ORDER BY level, schema_name, table_name;

-- name: all_tables_by_schema
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
