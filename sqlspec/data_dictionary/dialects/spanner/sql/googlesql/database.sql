-- name: options
-- dialect: spanner
SELECT
    CATALOG_NAME AS catalog_name,
    OPTION_NAME AS option_name,
    OPTION_TYPE AS option_type,
    OPTION_VALUE AS option_value
FROM INFORMATION_SCHEMA.DATABASE_OPTIONS
ORDER BY OPTION_NAME;
