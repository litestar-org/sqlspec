-- name: list
-- dialect: mysql
SELECT
    schema_name,
    default_character_set_name,
    default_collation_name,
    sql_path
FROM information_schema.schemata
WHERE :schema_name IS NULL OR schema_name = :schema_name
ORDER BY schema_name;
