-- name: by_object
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS object_name,
    CASE c.relkind
        WHEN 'v' THEN pg_catalog.pg_get_viewdef(c.oid, true)::text
        WHEN 'm' THEN pg_catalog.pg_get_viewdef(c.oid, true)::text
        WHEN 'S' THEN 'CREATE SEQUENCE ' || pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)
        ELSE NULL::text
    END AS ddl,
    CASE c.relkind
        WHEN 'v' THEN 'native'
        WHEN 'm' THEN 'native'
        WHEN 'S' THEN 'generated'
        ELSE 'unsupported'
    END AS fidelity,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
  AND c.relname = :object_name
  AND (:object_type::text IS NULL OR c.relkind = CASE :object_type
      WHEN 'view' THEN 'v'
      WHEN 'materialized_view' THEN 'm'
      WHEN 'sequence' THEN 'S'
      WHEN 'table' THEN 'r'
      ELSE c.relkind
  END)
UNION ALL
SELECT
    n.nspname::text AS schema_name,
    p.proname::text AS object_name,
    pg_catalog.pg_get_functiondef(p.oid)::text AS ddl,
    'native'::text AS fidelity,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = p.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
  AND p.proname = :object_name
  AND (:object_type::text IS NULL OR :object_type IN ('routine', 'function', 'procedure'))
ORDER BY schema_name, object_name;
