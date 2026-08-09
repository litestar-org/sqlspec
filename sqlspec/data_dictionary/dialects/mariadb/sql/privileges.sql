-- name: by_schema
-- dialect: mysql
SELECT
    grantee,
    table_catalog,
    table_schema AS schema_name,
    table_name,
    privilege_type,
    is_grantable,
    NULL AS column_name
FROM information_schema.table_privileges
WHERE table_schema = COALESCE(:schema_name, DATABASE())
UNION ALL
SELECT
    grantee,
    table_catalog,
    table_schema AS schema_name,
    table_name,
    privilege_type,
    is_grantable,
    column_name
FROM information_schema.column_privileges
WHERE table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY schema_name, table_name, column_name, grantee, privilege_type;
