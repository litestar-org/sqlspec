-- name: tables_by_schema
-- dialect: postgres
WITH RECURSIVE dependency_tree AS (
    SELECT
        t.table_name::text,
        0 AS level,
        ARRAY[t.table_name::text] AS path
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema = :schema_name
      AND NOT EXISTS (
          SELECT 1
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
          WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = t.table_name
            AND tc.table_schema = t.table_schema
      )

    UNION ALL

    SELECT
        tc.table_name::text,
        dt.level + 1,
        dt.path || tc.table_name::text
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
    JOIN dependency_tree dt
      ON ccu.table_name = dt.table_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = :schema_name
      AND ccu.table_schema = :schema_name
      AND NOT (tc.table_name = ANY(dt.path))
)
SELECT DISTINCT table_name, level
FROM dependency_tree
ORDER BY level, table_name;
