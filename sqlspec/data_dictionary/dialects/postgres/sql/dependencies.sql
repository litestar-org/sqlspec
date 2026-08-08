-- name: by_schema
-- dialect: postgres
SELECT
    src_ns.nspname::text AS schema_name,
    src.relname::text AS object_name,
    src.relkind::text AS object_type,
    target_ns.nspname::text AS referenced_schema,
    target.relname::text AS referenced_object,
    target.relkind::text AS referenced_object_type,
    dep.deptype::text AS dependency_type,
    ext.extname::text AS extension_name,
    dep.objid::text AS object_oid,
    dep.refobjid::text AS referenced_oid
FROM pg_catalog.pg_depend dep
JOIN pg_catalog.pg_class src ON src.oid = dep.objid
JOIN pg_catalog.pg_namespace src_ns ON src_ns.oid = src.relnamespace
LEFT JOIN pg_catalog.pg_class target ON target.oid = dep.refobjid
LEFT JOIN pg_catalog.pg_namespace target_ns ON target_ns.oid = target.relnamespace
LEFT JOIN pg_catalog.pg_extension ext ON ext.oid = dep.refobjid AND dep.deptype = 'e'
WHERE src_ns.nspname = :schema_name
  AND (:object_name::text IS NULL OR src.relname = :object_name)
UNION ALL
SELECT
    src_ns.nspname::text AS schema_name,
    src.relname::text AS object_name,
    'table'::text AS object_type,
    target_ns.nspname::text AS referenced_schema,
    target.relname::text AS referenced_object,
    'table'::text AS referenced_object_type,
    'foreign_key'::text AS dependency_type,
    NULL::text AS extension_name,
    fk.oid::text AS object_oid,
    fk.confrelid::text AS referenced_oid
FROM pg_catalog.pg_constraint fk
JOIN pg_catalog.pg_class src ON src.oid = fk.conrelid
JOIN pg_catalog.pg_namespace src_ns ON src_ns.oid = src.relnamespace
JOIN pg_catalog.pg_class target ON target.oid = fk.confrelid
JOIN pg_catalog.pg_namespace target_ns ON target_ns.oid = target.relnamespace
WHERE fk.contype = 'f'
  AND src_ns.nspname = :schema_name
  AND (:object_name::text IS NULL OR src.relname = :object_name)
ORDER BY schema_name, object_name, dependency_type, referenced_object;
