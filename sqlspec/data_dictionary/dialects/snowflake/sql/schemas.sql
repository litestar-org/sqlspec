-- name: list
-- dialect: snowflake
SELECT
    catalog_name AS "catalog_name",
    schema_name AS "schema_name",
    schema_owner AS "owner_name",
    is_transient AS "is_transient",
    is_managed_access AS "is_managed_access",
    retention_time AS "retention_time",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.schemata
WHERE UPPER(schema_name) <> 'INFORMATION_SCHEMA'
ORDER BY schema_name;

-- name: current
-- dialect: snowflake
SELECT CURRENT_DATABASE() AS "database_name", CURRENT_SCHEMA() AS "schema_name";
