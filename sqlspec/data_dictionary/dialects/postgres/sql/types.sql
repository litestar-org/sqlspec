-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    t.typname::text AS type_name,
    'ENUM'::text AS logical_type,
    array_agg(e.enumlabel::text ORDER BY e.enumsortorder) AS labels
FROM pg_catalog.pg_type AS t
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
JOIN pg_catalog.pg_enum AS e ON e.enumtypid = t.oid
WHERE t.typtype = 'e'
  AND n.nspname = :schema_name
  AND NOT EXISTS (
      SELECT 1
      FROM pg_catalog.pg_depend AS dep
      WHERE dep.classid = 'pg_catalog.pg_type'::regclass
        AND dep.objid = t.oid
        AND dep.deptype = 'e'
  )
GROUP BY n.nspname, t.typname
ORDER BY t.typname;
