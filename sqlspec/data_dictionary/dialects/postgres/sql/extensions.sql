-- name: list
-- dialect: postgres
SELECT
    ext.extname::text AS extension_name,
    ext.extversion::text AS extension_version,
    n.nspname::text AS schema_name,
    ext.oid::text AS extension_oid,
    pg_catalog.obj_description(ext.oid, 'pg_extension')::text AS comment
FROM pg_catalog.pg_extension ext
JOIN pg_catalog.pg_namespace n ON n.oid = ext.extnamespace
ORDER BY ext.extname;
