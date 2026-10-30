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
    column_key = 'PRI' AS is_primary,
    extra,
    extra LIKE '%auto_increment%' AS is_identity,
    generation_expression,
    CASE WHEN generation_expression IS NOT NULL AND generation_expression <> '' THEN 1 ELSE 0 END AS is_generated,
    column_comment
FROM information_schema.columns
WHERE table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY table_schema, table_name, ordinal_position;

-- name: by_table
-- dialect: mysql
SELECT
    column_name AS `column_name`,
    data_type AS `data_type`,
    column_type AS `column_type`,
    is_nullable AS `is_nullable`,
    column_default AS `column_default`,
    column_key AS `column_key`,
    column_key = 'PRI' AS `is_primary`,
    extra AS `extra`,
    extra LIKE '%auto_increment%' AS `is_identity`,
    generation_expression AS `generation_expression`,
    CASE WHEN generation_expression IS NOT NULL AND generation_expression <> '' THEN 1 ELSE 0 END AS `is_generated`
FROM information_schema.columns
WHERE table_name = :table_name
  AND table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY ordinal_position;
