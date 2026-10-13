-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_schema::text AS schema_name,
    table_name::text AS table_name,
    index_name::text AS index_name,
    array_agg(column_name::text ORDER BY seq_in_index::int) AS columns,
    (non_unique = false OR non_unique::text IN ('0', 'f', 'false', 'NO')) AS is_unique,
    index_name = 'primary' AS is_primary
FROM information_schema.statistics
WHERE table_schema = :schema_name
  AND (:table_name::text IS NULL OR table_name = :table_name)
GROUP BY table_schema, table_name, index_name, non_unique
ORDER BY table_schema, table_name, index_name;

-- name: by_table
-- dialect: cockroachdb
SELECT
    i.relname::text AS index_name,
    t.relname::text AS table_name,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary,
    array_agg(a.attname::text ORDER BY array_position(ix.indkey, a.attnum)) AS columns
FROM pg_catalog.pg_class t
JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE t.relname = :table_name
  AND n.nspname = :schema_name
GROUP BY t.relname, i.relname, ix.indisunique, ix.indisprimary
ORDER BY i.relname;
