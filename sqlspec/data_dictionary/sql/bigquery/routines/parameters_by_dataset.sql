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
