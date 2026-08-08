-- name: ranges
-- dialect: cockroachdb
SELECT
    database_name::text AS database_name,
    schema_name::text AS schema_name,
    table_name::text AS table_name,
    range_id::text AS range_id
FROM crdb_internal.ranges
WHERE (:schema_name::text IS NULL OR schema_name = :schema_name)
  AND (:table_name::text IS NULL OR table_name = :table_name)
ORDER BY database_name, schema_name, table_name, range_id;
