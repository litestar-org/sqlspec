-- name: by_schema
-- dialect: postgres
SELECT
    tn.nspname::text AS schema_name,
    tbl.relname::text AS table_name,
    idx.relname::text AS index_name,
    am.amname::text AS access_method,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary,
    ix.indisexclusion AS is_exclusion,
    ix.indisvalid AS is_valid,
    ix.indispartial AS is_partial,
    pg_catalog.pg_get_expr(ix.indpred, ix.indrelid)::text AS predicate,
    pg_catalog.pg_get_indexdef(idx.oid)::text AS definition,
    ARRAY(
        SELECT COALESCE(att.attname::text, pg_catalog.pg_get_indexdef(idx.oid, keys.ord::integer, true)::text)
        FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord)
        LEFT JOIN pg_catalog.pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = keys.attnum
        WHERE keys.ord <= ix.indnkeyatts
        ORDER BY keys.ord
    )::text[] AS columns,
    ARRAY(
        SELECT pg_catalog.pg_get_indexdef(idx.oid, keys.ord::integer, true)::text
        FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord)
        WHERE keys.ord > ix.indnkeyatts
        ORDER BY keys.ord
    )::text[] AS included_columns,
    ARRAY(
        SELECT opc.opcname::text
        FROM unnest(ix.indclass) WITH ORDINALITY AS classes(opclass_oid, ord)
        JOIN pg_catalog.pg_opclass opc ON opc.oid = classes.opclass_oid
        ORDER BY classes.ord
    )::text[] AS opclasses,
    dep.refobjid IS NOT NULL AS is_extension_owned
FROM pg_catalog.pg_index ix
JOIN pg_catalog.pg_class tbl ON tbl.oid = ix.indrelid
JOIN pg_catalog.pg_namespace tn ON tn.oid = tbl.relnamespace
JOIN pg_catalog.pg_class idx ON idx.oid = ix.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = idx.relam
LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = idx.oid AND dep.deptype = 'e'
WHERE tn.nspname = :schema_name
  AND (:table_name::text IS NULL OR tbl.relname = :table_name)
ORDER BY tn.nspname, tbl.relname, idx.relname;
