-- name: by_schema
-- dialect: spanner
SELECT
    ROUTINE_CATALOG AS routine_catalog,
    ROUTINE_SCHEMA AS routine_schema,
    ROUTINE_NAME AS routine_name,
    ROUTINE_TYPE AS routine_type,
    DATA_TYPE AS data_type,
    ROUTINE_BODY AS routine_body,
    ROUTINE_DEFINITION AS routine_definition,
    EXTERNAL_LANGUAGE AS external_language,
    IS_DETERMINISTIC AS is_deterministic,
    SECURITY_TYPE AS security_type
FROM INFORMATION_SCHEMA.ROUTINES
WHERE (CAST(:schema_name AS STRING) IS NULL OR ROUTINE_SCHEMA = :schema_name)
  AND (CAST(:routine_name AS STRING) IS NULL OR ROUTINE_NAME = :routine_name)
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;

-- name: parameters_by_schema
-- dialect: spanner
SELECT
    SPECIFIC_CATALOG AS specific_catalog,
    SPECIFIC_SCHEMA AS specific_schema,
    SPECIFIC_NAME AS specific_name,
    ORDINAL_POSITION AS ordinal_position,
    PARAMETER_MODE AS parameter_mode,
    IS_RESULT AS is_result,
    PARAMETER_NAME AS parameter_name,
    DATA_TYPE AS data_type
FROM INFORMATION_SCHEMA.PARAMETERS
WHERE (CAST(:schema_name AS STRING) IS NULL OR SPECIFIC_SCHEMA = :schema_name)
  AND (CAST(:routine_name AS STRING) IS NULL OR SPECIFIC_NAME = :routine_name)
ORDER BY SPECIFIC_SCHEMA, SPECIFIC_NAME, ORDINAL_POSITION;
