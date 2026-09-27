-- name: by_schema
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    COLUMN_NAME AS column_name,
    ORDINAL_POSITION AS ordinal_position,
    COLUMN_DEFAULT AS column_default,
    IS_NULLABLE AS is_nullable,
    SPANNER_TYPE AS data_type,
    IS_GENERATED AS is_generated,
    GENERATION_EXPRESSION AS generation_expression,
    IS_STORED AS is_stored,
    IS_HIDDEN AS is_hidden,
    IS_IDENTITY AS is_identity,
    IDENTITY_GENERATION AS identity_generation,
    IDENTITY_KIND AS identity_kind,
    IDENTITY_START_WITH_COUNTER AS identity_start_with_counter
FROM INFORMATION_SCHEMA.COLUMNS
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
  AND (CAST(:table_name AS STRING) IS NULL OR TABLE_NAME = :table_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;

-- name: by_table
-- dialect: spanner
SELECT
    TABLE_CATALOG AS table_catalog,
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    COLUMN_NAME AS column_name,
    ORDINAL_POSITION AS ordinal_position,
    COLUMN_DEFAULT AS column_default,
    IS_NULLABLE AS is_nullable,
    SPANNER_TYPE AS data_type,
    IS_GENERATED AS is_generated,
    GENERATION_EXPRESSION AS generation_expression,
    IS_STORED AS is_stored,
    IS_HIDDEN AS is_hidden,
    IS_IDENTITY AS is_identity,
    IDENTITY_GENERATION AS identity_generation,
    IDENTITY_KIND AS identity_kind,
    IDENTITY_START_WITH_COUNTER AS identity_start_with_counter
FROM INFORMATION_SCHEMA.COLUMNS
WHERE (CAST(:schema_name AS STRING) IS NULL OR TABLE_SCHEMA = :schema_name)
  AND (CAST(:table_name AS STRING) IS NULL OR TABLE_NAME = :table_name)
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
