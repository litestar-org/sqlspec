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
        WHEN ti.hidden IN (2, 3) THEN 'generated'
        ELSE NULL
    END AS extra
FROM pragma_table_list AS tl
JOIN pragma_table_xinfo(tl.name, COALESCE(:schema_name, 'main')) AS ti
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
ORDER BY tl.name, ti.cid;
