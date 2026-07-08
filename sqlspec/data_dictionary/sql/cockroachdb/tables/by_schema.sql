-- name: by_schema
-- dialect: cockroachdb
SELECT
    table_catalog::text AS table_catalog,
    table_schema::text AS table_schema,
    table_schema::text AS schema_name,
    table_name::text AS table_name,
    table_type::text AS table_type,
    NULL::int AS dependency_level,
    NULL::int AS level
FROM information_schema.tables
WHERE table_schema = :schema_name
  AND (:table_name::text IS NULL OR table_name = :table_name)
  AND table_type = 'BASE TABLE'
ORDER BY table_schema, table_name;
