-- name: by_schema
-- dialect: sqlanywhere
WITH RECURSIVE dependency_tree (table_id, schema_name, table_name, dependency_level) AS (
    SELECT
        t.table_id,
        u.user_name AS schema_name,
        t.table_name AS table_name,
        0 AS dependency_level
    FROM SYS.SYSTAB t
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    WHERE t.table_type = 1
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
      AND NOT EXISTS (
          SELECT 1
          FROM SYS.SYSFKEY fk
          WHERE fk.foreign_table_id = t.table_id
            AND fk.primary_table_id <> t.table_id
      )
    UNION ALL
    SELECT
        child.table_id,
        u.user_name AS schema_name,
        child.table_name AS table_name,
        parent.dependency_level + 1 AS dependency_level
    FROM SYS.SYSFKEY fk
    JOIN SYS.SYSTAB child ON child.table_id = fk.foreign_table_id
    JOIN SYS.SYSUSER u ON child.creator = u.user_id
    JOIN dependency_tree parent ON parent.table_id = fk.primary_table_id
    WHERE child.table_type = 1
      AND fk.primary_table_id <> fk.foreign_table_id
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR child.table_name = :table_name)
      AND parent.dependency_level < 20
),
dependency_levels AS (
    SELECT
        table_id,
        MAX(dependency_level) AS dependency_level
    FROM dependency_tree
    GROUP BY table_id
)
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    'BASE TABLE' AS table_type,
    COALESCE(dl.dependency_level, 0) AS dependency_level,
    COALESCE(dl.dependency_level, 0) AS level
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON t.creator = u.user_id
LEFT JOIN dependency_levels dl ON dl.table_id = t.table_id
WHERE t.table_type = 1
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY dependency_level, u.user_name, t.table_name;

-- name: all_by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    CASE t.table_type
        WHEN 1 THEN 'BASE TABLE'
        WHEN 21 THEN 'VIEW'
        ELSE 'TABLE'
    END AS table_type,
    0 AS dependency_level,
    0 AS level,
    t.count AS estimated_row_count,
    NULL AS table_comment
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON t.creator = u.user_id
WHERE t.table_type IN (1, 21)
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY u.user_name, t.table_name;

-- name: names_by_schema
-- dialect: sqlanywhere
SELECT
    t.table_name AS table_name
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON t.creator = u.user_id
WHERE t.table_type = 1
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
ORDER BY t.table_name;
