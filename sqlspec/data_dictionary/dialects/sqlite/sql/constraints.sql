-- name: by_schema
-- dialect: sqlite
SELECT
    COALESCE(:schema_name, 'main') AS schema_name,
    tl.name AS table_name,
    il.name AS index_name,
    CASE WHEN il.origin = 'pk' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
    ii.name AS column_name,
    ii.seqno AS ordinal_position
FROM pragma_table_list AS tl
JOIN pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il
JOIN pragma_index_info(il.name, COALESCE(:schema_name, 'main')) AS ii
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND il.origin IN ('pk', 'u')
  AND (:table_name IS NULL OR tl.name = :table_name)
UNION ALL
SELECT
    COALESCE(:schema_name, 'main') AS schema_name,
    tl.name AS table_name,
    NULL AS index_name,
    'PRIMARY KEY' AS constraint_type,
    ti.name AS column_name,
    ti.pk - 1 AS ordinal_position
FROM pragma_table_list AS tl
JOIN pragma_table_info(tl.name, COALESCE(:schema_name, 'main')) AS ti
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND ti.pk > 0
  AND (:table_name IS NULL OR tl.name = :table_name)
  AND NOT EXISTS (
      SELECT 1
      FROM pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il_pk
      WHERE il_pk.origin = 'pk'
  )
ORDER BY table_name, index_name, ordinal_position;

-- name: by_table
-- dialect: sqlite
SELECT
    COALESCE(:schema_name, 'main') AS schema_name,
    :table_name AS table_name,
    il.name AS index_name,
    CASE WHEN il.origin = 'pk' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
    ii.name AS column_name,
    ii.seqno AS ordinal_position
FROM pragma_index_list(:table_name, COALESCE(:schema_name, 'main')) AS il
JOIN pragma_index_info(il.name, COALESCE(:schema_name, 'main')) AS ii
WHERE il.origin IN ('pk', 'u')
UNION ALL
SELECT
    COALESCE(:schema_name, 'main') AS schema_name,
    :table_name AS table_name,
    NULL AS index_name,
    'PRIMARY KEY' AS constraint_type,
    ti.name AS column_name,
    ti.pk - 1 AS ordinal_position
FROM pragma_table_info(:table_name, COALESCE(:schema_name, 'main')) AS ti
WHERE ti.pk > 0
  AND NOT EXISTS (
      SELECT 1
      FROM pragma_index_list(:table_name, COALESCE(:schema_name, 'main')) AS il_pk
      WHERE il_pk.origin = 'pk'
  )
ORDER BY index_name, ordinal_position;
