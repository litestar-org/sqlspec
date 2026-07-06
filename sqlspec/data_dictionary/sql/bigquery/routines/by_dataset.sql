-- name: by_dataset
-- dialect: bigquery
SELECT
    routine_catalog,
    routine_schema,
    routine_name,
    routine_type,
    data_type,
    routine_body,
    routine_definition,
    external_language,
    is_deterministic,
    security_type,
    created,
    last_altered
FROM {routines_table}
WHERE (:schema_name IS NULL OR routine_schema = :schema_name)
  AND (:routine_name IS NULL OR routine_name = :routine_name)
ORDER BY routine_schema, routine_name;
