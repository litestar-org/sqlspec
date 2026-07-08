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
