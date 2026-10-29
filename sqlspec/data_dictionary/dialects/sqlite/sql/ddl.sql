-- name: by_object
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    name AS object_name,
    type AS object_type,
    tbl_name AS table_name,
    sql AS native_sql,
    sql AS ddl,
    'native' AS fidelity
FROM {schema_prefix}sqlite_schema
WHERE name = :object_name
  AND (:object_type IS NULL OR LOWER(type) = LOWER(:object_type))
  AND sql IS NOT NULL;

-- name: by_schema
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    name AS object_name,
    type AS object_type,
    tbl_name AS table_name,
    sql AS native_sql,
    sql AS ddl,
    'native' AS fidelity
FROM {schema_prefix}sqlite_schema
WHERE name NOT LIKE 'sqlite\_%' ESCAPE '\'
  AND sql IS NOT NULL
ORDER BY
    CASE type
        WHEN 'table' THEN 1
        WHEN 'view' THEN 2
        WHEN 'index' THEN 3
        WHEN 'trigger' THEN 4
        ELSE 5
    END,
    name;
