-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS object_name,
    'table'::text AS object_type,
    d.description::text AS comment,
    c.oid::text AS object_oid
FROM pg_catalog.pg_description d
JOIN pg_catalog.pg_class c ON c.oid = d.objoid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :schema_name
  AND d.objsubid = 0
UNION ALL
SELECT
    n.nspname::text AS schema_name,
    c.relname::text || '.' || a.attname::text AS object_name,
    'column'::text AS object_type,
    d.description::text AS comment,
    c.oid::text AS object_oid
FROM pg_catalog.pg_description d
JOIN pg_catalog.pg_class c ON c.oid = d.objoid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
WHERE n.nspname = :schema_name
  AND d.objsubid > 0
ORDER BY schema_name, object_type, object_name;
