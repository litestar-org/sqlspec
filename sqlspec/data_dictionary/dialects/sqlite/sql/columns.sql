-- name: by_schema
-- dialect: sqlite
SELECT
    tl.schema AS schema_name,
    tl.name AS table_name,
    ti.name AS column_name,
    ti.type AS data_type,
    CASE WHEN ti."notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
    ti.dflt_value AS column_default,
    ti.cid + 1 AS ordinal_position,
    ti.pk AS is_primary,
    CASE
        WHEN ti.hidden = 1 THEN 'hidden'
        WHEN ti.hidden = 2 THEN 'virtual generated'
        WHEN ti.hidden = 3 THEN 'stored generated'
        WHEN ti.pk = 1 AND UPPER(ti.type) = 'INTEGER' AND UPPER(sm.sql) LIKE '%AUTOINCREMENT%' THEN 'autoincrement'
        ELSE NULL
    END AS extra
FROM pragma_table_list AS tl
JOIN pragma_table_xinfo(tl.name, COALESCE(:schema_name, 'main')) AS ti
LEFT JOIN {schema_prefix}sqlite_schema AS sm ON sm.type = 'table' AND sm.name = tl.name
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite\_%' ESCAPE '\'
ORDER BY tl.name, ti.cid;

-- name: by_table
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    :table_name AS table_name,
    ti.name AS column_name,
    ti.type AS data_type,
    CASE WHEN ti."notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
    ti.dflt_value AS column_default,
    ti.cid + 1 AS ordinal_position,
    ti.pk AS is_primary,
    CASE
        WHEN ti.hidden = 1 THEN 'hidden'
        WHEN ti.hidden = 2 THEN 'virtual generated'
        WHEN ti.hidden = 3 THEN 'stored generated'
        WHEN ti.pk = 1 AND UPPER(ti.type) = 'INTEGER' AND UPPER(sm.sql) LIKE '%AUTOINCREMENT%' THEN 'autoincrement'
        ELSE NULL
    END AS extra
FROM pragma_table_xinfo(:table_name, :schema_name) AS ti
LEFT JOIN {schema_prefix}sqlite_schema AS sm ON sm.type = 'table' AND sm.name = :table_name
ORDER BY ti.cid;
