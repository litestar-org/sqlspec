-- name: by_object
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    name AS object_name,
    type AS object_type,
    sql AS native_sql
FROM {schema_prefix}sqlite_schema
WHERE name = :object_name
  AND (:object_type IS NULL OR type = :object_type);
