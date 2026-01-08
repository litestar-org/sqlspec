-- name: tables_by_schema
-- dialect: spanner
SELECT
    table_name,
    0 AS level
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND (:schema_name IS NULL OR table_schema = :schema_name)
ORDER BY table_name;
