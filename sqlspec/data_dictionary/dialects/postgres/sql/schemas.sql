-- name: list
-- dialect: postgres
SELECT
    current_database()::text AS catalog_name,
    n.oid::text AS schema_oid,
    n.nspname::text AS schema_name,
    r.rolname::text AS owner_name,
    pg_catalog.obj_description(n.oid, 'pg_namespace')::text AS comment
FROM pg_catalog.pg_namespace n
LEFT JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner
WHERE n.nspname NOT LIKE 'pg\_%'
  AND n.nspname <> 'information_schema'
ORDER BY n.nspname;
