-- name: by_dataset
-- dialect: bigquery
SELECT
    object_catalog,
    object_schema,
    object_name,
    object_type,
    privilege_type,
    grantee
FROM {object_privileges_table}
WHERE (:schema_name IS NULL OR object_schema = :schema_name)
  AND (:object_name IS NULL OR object_name = :object_name)
ORDER BY object_schema, object_name, privilege_type, grantee;
