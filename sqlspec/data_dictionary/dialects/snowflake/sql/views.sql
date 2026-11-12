-- name: by_schema
-- dialect: snowflake
SELECT
    table_catalog AS "catalog_name",
    table_schema AS "schema_name",
    table_name AS "view_name",
    table_owner AS "owner_name",
    view_definition AS "view_definition",
    view_definition AS "definition",
    check_option AS "check_option",
    is_updatable AS "is_updatable",
    is_secure AS "is_secure",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.views
WHERE UPPER(table_schema) = UPPER(COALESCE(:schema_name, CURRENT_SCHEMA()))
ORDER BY table_schema, table_name;

-- name: by_name
-- dialect: snowflake
SELECT
    table_catalog AS "catalog_name",
    table_schema AS "schema_name",
    table_name AS "view_name",
    table_owner AS "owner_name",
    view_definition AS "view_definition",
    view_definition AS "definition",
    check_option AS "check_option",
    is_updatable AS "is_updatable",
    is_secure AS "is_secure",
    created AS "created",
    last_altered AS "last_altered",
    comment AS "comment"
FROM information_schema.views
WHERE UPPER(table_schema) = UPPER(COALESCE(:schema_name, CURRENT_SCHEMA()))
  AND UPPER(table_name) = UPPER(:view_name);
