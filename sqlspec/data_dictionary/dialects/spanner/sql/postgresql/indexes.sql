-- name: by_schema
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    index_name,
    index_type,
    is_unique
FROM information_schema.indexes
WHERE (:schema_name::text IS NULL OR table_schema = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name, index_name;

-- name: by_table
-- dialect: spanner
SELECT
    table_catalog,
    table_schema,
    table_name,
    index_name,
    index_type,
    is_unique
FROM information_schema.indexes
WHERE (:schema_name::text IS NULL OR table_schema = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name, index_name;
