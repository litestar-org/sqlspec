-- name: tables_by_schema
-- dialect: mysql
WITH RECURSIVE dependency_tree AS (
    SELECT
        table_name,
        0 AS level,
        CAST(table_name AS CHAR(4000)) AS path
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema = COALESCE(:schema_name, DATABASE())
      AND NOT EXISTS (
          SELECT 1
          FROM information_schema.key_column_usage kcu
          WHERE kcu.table_name = t.table_name
            AND kcu.table_schema = t.table_schema
            AND kcu.referenced_table_name IS NOT NULL
      )

    UNION ALL

    SELECT
        kcu.table_name,
        dt.level + 1,
        CONCAT(dt.path, ',', kcu.table_name)
    FROM information_schema.key_column_usage kcu
    JOIN dependency_tree dt ON kcu.referenced_table_name = dt.table_name
    WHERE kcu.table_schema = COALESCE(:schema_name, DATABASE())
      AND kcu.referenced_table_name IS NOT NULL
      AND NOT FIND_IN_SET(kcu.table_name, dt.path)
)
SELECT DISTINCT table_name
FROM dependency_tree
ORDER BY level, table_name;
