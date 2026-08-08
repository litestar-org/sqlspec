-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS sequence_name,
    s.seqstart AS start_value,
    s.seqmin AS minimum_value,
    s.seqmax AS maximum_value,
    s.seqincrement AS increment,
    s.seqcycle AS cycles,
    pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
ORDER BY n.nspname, c.relname;
