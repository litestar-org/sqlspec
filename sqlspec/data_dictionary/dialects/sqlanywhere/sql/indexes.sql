-- name: by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    i.index_name AS index_name,
    c.column_name AS column_name,
    ic.sequence + 1 AS column_position,
    CASE WHEN i."unique" IN (1, 2) THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN i.index_category = 1 THEN 1 ELSE 0 END AS is_primary
FROM SYS.SYSIDX i
JOIN SYS.SYSTAB t ON i.table_id = t.table_id
JOIN SYS.SYSUSER u ON t.creator = u.user_id
JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
JOIN SYS.SYSTABCOL c ON ic.table_id = c.table_id AND ic.column_id = c.column_id
WHERE t.table_type = 1
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY u.user_name, t.table_name, i.index_name, ic.sequence;

-- name: by_table
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    i.index_name AS index_name,
    c.column_name AS column_name,
    ic.sequence + 1 AS column_position,
    CASE WHEN i."unique" IN (1, 2) THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN i.index_category = 1 THEN 1 ELSE 0 END AS is_primary
FROM SYS.SYSIDX i
JOIN SYS.SYSTAB t ON i.table_id = t.table_id
JOIN SYS.SYSUSER u ON t.creator = u.user_id
JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
JOIN SYS.SYSTABCOL c ON ic.table_id = c.table_id AND ic.column_id = c.column_id
WHERE t.table_type = 1
  AND t.table_name = :table_name
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
ORDER BY i.index_name, ic.sequence;
