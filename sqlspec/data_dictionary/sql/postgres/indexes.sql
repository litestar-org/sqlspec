-- name: indexes_by_table
-- dialect: postgres
SELECT
    i.relname as index_name,
    t.relname as table_name,
    ix.indisunique as is_unique,
    ix.indisprimary as is_primary,
    array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
FROM
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a,
    pg_namespace n
WHERE
    t.oid = ix.indrelid
    AND i.oid = ix.indexrelid
    AND a.attrelid = t.oid
    AND a.attnum = ANY(ix.indkey)
    AND t.relkind = 'r'
    AND t.relnamespace = n.oid
    AND n.nspname = :schema_name
    AND t.relname = :table_name
GROUP BY
    t.relname,
    i.relname,
    ix.indisunique,
    ix.indisprimary;

-- name: indexes_by_schema
-- dialect: postgres
SELECT
    i.relname as index_name,
    t.relname as table_name,
    ix.indisunique as is_unique,
    ix.indisprimary as is_primary,
    array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as columns
FROM
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a,
    pg_namespace n
WHERE
    t.oid = ix.indrelid
    AND i.oid = ix.indexrelid
    AND a.attrelid = t.oid
    AND a.attnum = ANY(ix.indkey)
    AND t.relkind = 'r'
    AND t.relnamespace = n.oid
    AND n.nspname = :schema_name
GROUP BY
    t.relname,
    i.relname,
    ix.indisunique,
    ix.indisprimary;
