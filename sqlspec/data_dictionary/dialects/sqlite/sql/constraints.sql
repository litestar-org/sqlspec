-- name: by_schema
-- dialect: sqlite
SELECT
    COALESCE(:schema_name, 'main') AS schema_name,
    tl.name AS table_name,
    il.name AS constraint_name,
    CASE WHEN il.origin = 'pk' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type
FROM pragma_table_list AS tl
JOIN pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND il.origin IN ('pk', 'u')
  AND (:table_name IS NULL OR tl.name = :table_name)
ORDER BY tl.name, il.name;

-- name: by_table
-- dialect: sqlite
SELECT
    COALESCE(:schema_name, 'main') AS schema_name,
    :table_name AS table_name,
    il.name AS constraint_name,
    CASE WHEN il.origin = 'pk' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type
FROM pragma_index_list(:table_name, COALESCE(:schema_name, 'main')) AS il
WHERE il.origin IN ('pk', 'u')
ORDER BY il.name;
