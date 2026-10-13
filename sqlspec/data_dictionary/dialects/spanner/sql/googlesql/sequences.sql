-- name: by_schema
-- dialect: spanner
SELECT
    SEQUENCE_CATALOG AS sequence_catalog,
    SEQUENCE_SCHEMA AS sequence_schema,
    SEQUENCE_NAME AS sequence_name,
    DATA_TYPE AS data_type,
    START_WITH_COUNTER AS start_with_counter,
    SKIP_RANGE_MIN AS skip_range_min,
    SKIP_RANGE_MAX AS skip_range_max
FROM INFORMATION_SCHEMA.SEQUENCES
WHERE (CAST(:schema_name AS STRING) IS NULL OR SEQUENCE_SCHEMA = :schema_name)
  AND (CAST(:sequence_name AS STRING) IS NULL OR SEQUENCE_NAME = :sequence_name)
ORDER BY SEQUENCE_SCHEMA, SEQUENCE_NAME;
