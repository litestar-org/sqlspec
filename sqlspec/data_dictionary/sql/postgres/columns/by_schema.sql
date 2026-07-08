-- name: by_schema
-- dialect: postgres
SELECT
    current_database()::text AS table_catalog,
    n.nspname::text AS table_schema,
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    a.attname::text AS column_name,
    a.attnum::integer AS ordinal_position,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    t.typname::text AS type_name,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text AS column_default,
    a.attidentity::text AS identity_generation,
    a.attgenerated::text AS generated_kind,
    col_description(c.oid, a.attnum)::text AS comment,
    EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint pk
        WHERE pk.conrelid = c.oid
          AND pk.contype = 'p'
          AND a.attnum = ANY(pk.conkey)
    ) AS is_primary,
    EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint uq
        WHERE uq.conrelid = c.oid
          AND uq.contype = 'u'
          AND a.attnum = ANY(uq.conkey)
    ) AS is_unique
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = :schema_name
  AND (:table_name::text IS NULL OR c.relname = :table_name)
  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY n.nspname, c.relname, a.attnum;
