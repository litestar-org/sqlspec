-- name: by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    i.index_name AS constraint_name,
    CASE i.index_category
        WHEN 1 THEN 'PRIMARY KEY'
        WHEN 2 THEN 'FOREIGN KEY'
        ELSE 'UNIQUE'
    END AS constraint_type,
    NULL AS check_clause,
    'Y' AS is_enforced
FROM SYS.SYSIDX i
JOIN SYS.SYSTAB t ON i.table_id = t.table_id
JOIN SYS.SYSUSER u ON t.creator = u.user_id
WHERE t.table_type = 1
  AND (i.index_category IN (1, 2) OR i."unique" IN (1, 2))
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY u.user_name, t.table_name, i.index_name;

-- name: by_table
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    i.index_name AS constraint_name,
    CASE i.index_category
        WHEN 1 THEN 'PRIMARY KEY'
        WHEN 2 THEN 'FOREIGN KEY'
        ELSE 'UNIQUE'
    END AS constraint_type,
    NULL AS check_clause,
    'Y' AS is_enforced
FROM SYS.SYSIDX i
JOIN SYS.SYSTAB t ON i.table_id = t.table_id
JOIN SYS.SYSUSER u ON t.creator = u.user_id
WHERE t.table_type = 1
  AND (i.index_category IN (1, 2) OR i."unique" IN (1, 2))
  AND t.table_name = :table_name
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
ORDER BY i.index_name;
