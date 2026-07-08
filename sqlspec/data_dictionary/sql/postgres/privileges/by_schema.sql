-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS object_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'p' THEN 'partitioned_table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        WHEN 'S' THEN 'sequence'
        ELSE c.relkind::text
    END AS object_type,
    grant_row.grantee::text AS grantee,
    grant_row.privilege_type::text AS privilege_type,
    grant_row.is_grantable AS is_grantable
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))) AS acl
LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
CROSS JOIN LATERAL (
    SELECT
        COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
        acl.privilege_type,
        acl.is_grantable
) AS grant_row
WHERE n.nspname = :schema_name
  AND (:object_name::text IS NULL OR c.relname = :object_name)
ORDER BY n.nspname, c.relname, grant_row.grantee, grant_row.privilege_type;
