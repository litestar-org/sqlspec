-- name: indexes_by_table
-- dialect: cockroachdb
SELECT
    i.relname::text AS index_name,
    a.attname::text AS column_name,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary
FROM pg_catalog.pg_class t
JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE t.relname = :table_name
  AND n.nspname = :schema_name
ORDER BY i.relname, a.attnum;

-- name: indexes_by_schema
-- dialect: cockroachdb
SELECT
    n.nspname::text AS schema_name,
    t.relname::text AS table_name,
    i.relname::text AS index_name,
    a.attname::text AS column_name,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary
FROM pg_catalog.pg_class t
JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE n.nspname = :schema_name
ORDER BY t.relname, i.relname, a.attnum;
