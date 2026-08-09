-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_catalog::text AS table_catalog,
    table_schema::text AS table_schema,
    table_schema::text AS schema_name,
    table_name::text AS table_name,
    column_name::text AS column_name,
    ordinal_position,
    data_type::text AS data_type,
    is_nullable::text AS is_nullable,
    column_default::text AS column_default,
    generation_expression::text AS generation_expression,
    is_hidden::text AS is_hidden
FROM information_schema.columns
WHERE table_schema = :schema_name
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name, ordinal_position;

-- name: by_table
-- dialect: cockroachdb
SELECT
    a.attname::text AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text AS column_default
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE c.relname = :table_name
  AND n.nspname = :schema_name
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
