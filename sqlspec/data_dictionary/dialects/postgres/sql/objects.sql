-- name: by_schema
-- dialect: postgres
SELECT
    current_database()::text AS catalog_name,
    n.nspname::text AS schema_name,
    c.relname::text AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'p' THEN 'partitioned_table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        WHEN 'S' THEN 'sequence'
        WHEN 'f' THEN 'foreign_table'
        ELSE c.relkind::text
    END AS object_type,
    c.oid::text AS object_oid,
    r.rolname::text AS owner_name,
    c.relispartition AS is_partition,
    pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
WHERE n.nspname = :schema_name
  AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
ORDER BY n.nspname, object_type, c.relname;
