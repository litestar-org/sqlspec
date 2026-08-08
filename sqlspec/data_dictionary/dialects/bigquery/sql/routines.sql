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

-- name: options_by_dataset
-- dialect: bigquery
SELECT
    specific_catalog,
    specific_schema,
    specific_name,
    option_name,
    option_type,
    option_value
FROM {routine_options_table}
WHERE (:schema_name IS NULL OR specific_schema = :schema_name)
  AND (:routine_name IS NULL OR specific_name = :routine_name)
ORDER BY specific_schema, specific_name, option_name;

-- name: parameters_by_dataset
-- dialect: bigquery
SELECT
    specific_catalog,
    specific_schema,
    specific_name,
    ordinal_position,
    parameter_mode,
    is_result,
    parameter_name,
    data_type,
    parameter_default
FROM {parameters_table}
WHERE (:schema_name IS NULL OR specific_schema = :schema_name)
  AND (:routine_name IS NULL OR specific_name = :routine_name)
ORDER BY specific_schema, specific_name, ordinal_position;
