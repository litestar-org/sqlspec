-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    table_name AS object_name,
    NULL AS column_name,
    grantor,
    grantee,
    privilege,
    grantable,
    hierarchy,
    type,
    common,
    inherited
FROM all_tab_privs
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR table_name = :object_name)
UNION ALL
SELECT
    owner AS schema_name,
    table_name AS object_name,
    column_name,
    grantor,
    grantee,
    privilege,
    grantable,
    NULL AS hierarchy,
    'COLUMN' AS type,
    common,
    inherited
FROM all_col_privs
WHERE owner = COALESCE(:schema_name, USER)
  AND (:object_name IS NULL OR table_name = :object_name)
ORDER BY schema_name, object_name, column_name, grantee, privilege;
