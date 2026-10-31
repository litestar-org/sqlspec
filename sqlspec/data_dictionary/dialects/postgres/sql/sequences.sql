-- name: by_schema
-- dialect: postgres
WITH extension_deps AS (
    SELECT DISTINCT objid
    FROM pg_catalog.pg_depend
    WHERE classid = 'pg_catalog.pg_class'::regclass
      AND deptype = 'e'
),
identity_deps AS (
    SELECT DISTINCT objid
    FROM pg_catalog.pg_depend
    WHERE classid = 'pg_catalog.pg_class'::regclass
      AND deptype = 'i'
)
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS sequence_name,
    s.seqstart AS start_value,
    s.seqmin AS minimum_value,
    s.seqmax AS maximum_value,
    s.seqincrement AS increment,
    s.seqcycle AS cycles,
    pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
    ext_dep.objid IS NOT NULL AS is_extension_owned,
    id_dep.objid IS NOT NULL AS is_identity_owned
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
LEFT JOIN extension_deps AS ext_dep ON ext_dep.objid = c.oid
LEFT JOIN identity_deps AS id_dep ON id_dep.objid = c.oid
WHERE n.nspname = :schema_name
ORDER BY n.nspname, c.relname;
