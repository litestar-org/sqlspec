-- name: by_schema
-- dialect: duckdb
WITH RECURSIVE dependency_tree AS (
    SELECT
        t.table_name,
        0 AS level,
        [t.table_name] AS path
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema = COALESCE(:schema_name, current_schema())
      AND NOT EXISTS (
          SELECT 1
          FROM information_schema.key_column_usage kcu
          JOIN information_schema.referential_constraints rc
            ON kcu.constraint_name = rc.constraint_name
            AND kcu.constraint_schema = rc.constraint_schema
          JOIN information_schema.key_column_usage pk_kcu
            ON rc.unique_constraint_name = pk_kcu.constraint_name
            AND rc.unique_constraint_schema = pk_kcu.constraint_schema
          WHERE kcu.table_name = t.table_name
            AND kcu.table_schema = t.table_schema
            AND pk_kcu.table_name <> t.table_name
      )

    UNION ALL

    SELECT
        kcu.table_name,
        dt.level + 1,
        list_append(dt.path, kcu.table_name)
    FROM information_schema.key_column_usage kcu
    JOIN information_schema.referential_constraints rc
      ON kcu.constraint_name = rc.constraint_name
      AND kcu.constraint_schema = rc.constraint_schema
    JOIN information_schema.key_column_usage pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
    JOIN dependency_tree dt ON dt.table_name = pk_kcu.table_name
    WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
      AND pk_kcu.table_name <> kcu.table_name
      AND NOT list_contains(dt.path, kcu.table_name)
),
dependency_levels AS (
    SELECT
        table_name,
        MAX(level) AS dependency_level
    FROM dependency_tree
    GROUP BY table_name
)
SELECT
    t.table_name,
    COALESCE(dl.dependency_level, 0) AS level,
    COALESCE(dl.dependency_level, 0) AS dependency_level
FROM information_schema.tables t
LEFT JOIN dependency_levels dl ON dl.table_name = t.table_name
WHERE t.table_type = 'BASE TABLE'
  AND t.table_schema = COALESCE(:schema_name, current_schema())
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY level, t.table_name;
