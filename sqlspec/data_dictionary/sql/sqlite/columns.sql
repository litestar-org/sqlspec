-- name: columns_by_table
-- dialect: sqlite
SELECT
    name AS column_name,
    type AS data_type,
    CASE WHEN "notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
    dflt_value AS column_default
FROM pragma_table_info({table_name})
ORDER BY cid;

-- name: columns_by_schema
-- dialect: sqlite
SELECT
    m.name AS table_name,
    ti.name AS column_name,
    ti.type AS data_type,
    CASE WHEN ti."notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
    ti.dflt_value AS column_default
FROM {schema_prefix}sqlite_schema m
JOIN pragma_table_info(m.name) AS ti
WHERE m.type = 'table'
  AND m.name NOT LIKE 'sqlite_%'
ORDER BY m.name, ti.cid;
