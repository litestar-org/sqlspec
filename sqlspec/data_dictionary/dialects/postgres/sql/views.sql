-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS view_name,
    c.oid::text AS object_oid,
    pg_catalog.pg_get_viewdef(c.oid, true)::text AS definition,
    r.rolname::text AS owner_name,
    pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
  AND c.relkind = 'v'
ORDER BY n.nspname, c.relname;
