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
        WHEN ti.hidden IN (2, 3) THEN 'generated'
        ELSE NULL
    END AS extra
FROM pragma_table_xinfo(:table_name, :schema_name) AS ti
ORDER BY ti.cid;
