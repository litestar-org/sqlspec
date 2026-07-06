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
