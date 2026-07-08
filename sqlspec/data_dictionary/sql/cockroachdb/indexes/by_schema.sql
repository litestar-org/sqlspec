-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_schema::text AS schema_name,
    table_name::text AS table_name,
    index_name::text AS index_name,
    column_name::text AS column_name,
    non_unique = false AS is_unique,
    index_name = 'primary' AS is_primary,
    direction::text AS direction,
    storing::text AS storing
FROM information_schema.statistics
WHERE table_schema = :schema_name
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name, index_name, seq_in_index;
