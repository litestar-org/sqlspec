-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    table_name,
    NULL AS column_name,
    table_type AS object_type,
    comments
FROM all_tab_comments
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR table_name = :object_name)
UNION ALL
SELECT
    owner AS schema_name,
    table_name,
    column_name,
    'COLUMN' AS object_type,
    comments
FROM all_col_comments
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR table_name = :object_name)
ORDER BY schema_name, table_name, column_name;
