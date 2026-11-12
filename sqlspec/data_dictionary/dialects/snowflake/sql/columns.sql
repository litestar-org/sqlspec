-- name: by_schema
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_name AS "table_name",
    column_name AS "column_name",
    ordinal_position AS "ordinal_position",
    data_type AS "data_type",
    is_nullable AS "is_nullable",
    column_default AS "column_default",
    character_maximum_length AS "max_length",
    character_maximum_length AS "character_maximum_length",
    numeric_precision AS "numeric_precision",
    numeric_scale AS "numeric_scale",
    datetime_precision AS "datetime_precision",
    CASE WHEN UPPER(COALESCE(is_identity, 'NO')) = 'YES' THEN TRUE ELSE FALSE END AS "is_identity",
    identity_start AS "identity_start",
    identity_increment AS "identity_increment",
    comment AS "column_comment"
FROM information_schema.columns
WHERE table_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
ORDER BY table_name, ordinal_position;

-- name: by_table
-- dialect: snowflake
SELECT
    table_catalog AS "table_catalog",
    table_schema AS "schema_name",
    table_name AS "table_name",
    column_name AS "column_name",
    ordinal_position AS "ordinal_position",
    data_type AS "data_type",
    is_nullable AS "is_nullable",
    column_default AS "column_default",
    character_maximum_length AS "max_length",
    character_maximum_length AS "character_maximum_length",
    numeric_precision AS "numeric_precision",
    numeric_scale AS "numeric_scale",
    datetime_precision AS "datetime_precision",
    CASE WHEN UPPER(COALESCE(is_identity, 'NO')) = 'YES' THEN TRUE ELSE FALSE END AS "is_identity",
    identity_start AS "identity_start",
    identity_increment AS "identity_increment",
    comment AS "column_comment"
FROM information_schema.columns
WHERE table_schema = COALESCE(:schema_name, CURRENT_SCHEMA())
  AND table_name = :table_name
ORDER BY ordinal_position;
