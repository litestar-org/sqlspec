-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    tg.tgname::text AS trigger_name,
    p.proname::text AS function_name,
    pg_catalog.pg_get_triggerdef(tg.oid, true)::text AS definition,
    NOT tg.tgenabled = 'D' AS is_enabled,
    pg_catalog.obj_description(tg.oid, 'pg_trigger')::text AS comment,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_trigger tg
JOIN pg_catalog.pg_class c ON c.oid = tg.tgrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_proc p ON p.oid = tg.tgfoid
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = tg.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
  AND (:table_name::text IS NULL OR c.relname = :table_name)
  AND NOT tg.tgisinternal
ORDER BY n.nspname, c.relname, tg.tgname;
