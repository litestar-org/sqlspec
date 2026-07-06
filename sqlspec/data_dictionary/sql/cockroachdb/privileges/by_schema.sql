-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_schema::text AS schema_name,
    table_name::text AS object_name,
    grantee::text AS grantee,
    privilege_type::text AS privilege_type,
    is_grantable::text AS is_grantable,
    with_hierarchy::text AS with_hierarchy
FROM information_schema.table_privileges
WHERE table_schema = :schema_name
  AND (:object_name::text IS NULL OR table_name = :object_name)
ORDER BY table_schema, table_name, grantee, privilege_type;
