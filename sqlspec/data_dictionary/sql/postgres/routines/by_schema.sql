-- name: by_schema
-- dialect: postgres
SELECT
    n.nspname::text AS schema_name,
    p.proname::text AS routine_name,
    p.oid::text AS routine_oid,
    CASE p.prokind
        WHEN 'f' THEN 'function'
        WHEN 'p' THEN 'procedure'
        WHEN 'a' THEN 'aggregate'
        WHEN 'w' THEN 'window'
        ELSE p.prokind::text
    END AS routine_type,
    pg_catalog.pg_get_function_arguments(p.oid)::text AS arguments,
    pg_catalog.pg_get_function_result(p.oid)::text AS result_type,
    lang.lanname::text AS language_name,
    pg_catalog.pg_get_functiondef(p.oid)::text AS definition,
    pg_catalog.obj_description(p.oid, 'pg_proc')::text AS comment,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
JOIN pg_catalog.pg_language lang ON lang.oid = p.prolang
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = p.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
ORDER BY n.nspname, p.proname, p.oid;
