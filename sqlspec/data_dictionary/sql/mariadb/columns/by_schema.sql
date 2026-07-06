-- name: by_schema
-- dialect: mysql
SELECT
    table_schema AS schema_name,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    column_type,
    character_maximum_length AS max_length,
    numeric_precision,
    numeric_scale,
    character_set_name,
    collation_name,
    column_key,
    extra,
    generation_expression,
    column_comment
FROM information_schema.columns
WHERE table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY table_schema, table_name, ordinal_position;
