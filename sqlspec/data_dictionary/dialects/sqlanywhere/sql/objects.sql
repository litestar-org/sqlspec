-- name: by_schema
-- dialect: sqlanywhere
SELECT
    u.user_name AS schema_name,
    t.table_name AS object_name,
    CASE t.table_type
        WHEN 1 THEN 'TABLE'
        WHEN 21 THEN 'VIEW'
        ELSE 'OBJECT'
    END AS object_type,
    NULL AS created,
    NULL AS remarks
FROM SYS.SYSTAB t
JOIN SYS.SYSUSER u ON t.creator = u.user_id
WHERE t.table_type IN (1, 21)
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:object_name IS NULL OR t.table_name = :object_name)
ORDER BY u.user_name, object_type, t.table_name;
