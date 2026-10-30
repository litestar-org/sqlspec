-- name: by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    'BASE TABLE' AS table_type,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM SYS.SYSFKEY fk
            WHERE fk.foreign_table_id = t.table_id
              AND fk.primary_table_id <> t.table_id
        ) THEN 1
        ELSE 0
    END AS dependency_level,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM SYS.SYSFKEY fk
            WHERE fk.foreign_table_id = t.table_id
              AND fk.primary_table_id <> t.table_id
        ) THEN 1
        ELSE 0
    END AS level
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON t.creator = u.user_id
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
