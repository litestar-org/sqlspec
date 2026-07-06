-- name: by_schema
-- dialect: postgres
SELECT
    current_database()::text AS constraint_catalog,
    n.nspname::text AS constraint_schema,
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    con.conname::text AS constraint_name,
    CASE con.contype
        WHEN 'c' THEN 'CHECK'
        WHEN 'f' THEN 'FOREIGN KEY'
        WHEN 'p' THEN 'PRIMARY KEY'
        WHEN 'u' THEN 'UNIQUE'
        WHEN 'x' THEN 'EXCLUDE'
        ELSE con.contype::text
    END AS constraint_type,
    pg_catalog.pg_get_constraintdef(con.oid, true)::text AS definition,
    ARRAY(
        SELECT att.attname::text
        FROM unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ord)
        JOIN pg_catalog.pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = keys.attnum
        ORDER BY keys.ord
    )::text[] AS columns,
    ref_ns.nspname::text AS referenced_schema,
    ref.relname::text AS referenced_table,
    ARRAY(
        SELECT att.attname::text
        FROM unnest(con.confkey) WITH ORDINALITY AS keys(attnum, ord)
        JOIN pg_catalog.pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = keys.attnum
        ORDER BY keys.ord
    )::text[] AS referenced_columns,
    con.condeferrable AS is_deferrable,
    con.condeferred AS initially_deferred,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
LEFT JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
LEFT JOIN pg_catalog.pg_class ref ON ref.oid = con.confrelid
LEFT JOIN pg_catalog.pg_namespace ref_ns ON ref_ns.oid = ref.relnamespace
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = con.oid AND dep.deptype = 'e'
WHERE n.nspname = :schema_name
  AND (:table_name::text IS NULL OR c.relname = :table_name)
ORDER BY n.nspname, c.relname, con.conname;
