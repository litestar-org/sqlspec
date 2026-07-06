-- name: by_schema
-- dialect: sqlite
SELECT
    tl.schema AS schema_name,
    tl.name AS table_name,
    il.name AS index_name,
    il."unique" AS is_unique,
    CASE WHEN il.origin = 'pk' THEN 1 ELSE 0 END AS is_primary,
    il.origin,
    il.partial AS is_partial,
    ix.seqno,
    ix.cid AS column_id,
    ix.name AS column_name,
    ix.desc AS is_descending,
    ix.coll AS collation,
    ix.key AS is_key_column,
    sm.sql AS index_sql
FROM pragma_table_list AS tl
JOIN pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il
JOIN pragma_index_xinfo(il.name, COALESCE(:schema_name, 'main')) AS ix
LEFT JOIN {schema_prefix}sqlite_schema AS sm
  ON sm.type = 'index'
 AND sm.name = il.name
WHERE tl.schema = COALESCE(:schema_name, 'main')
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND il.name NOT LIKE 'sqlite_%'
ORDER BY tl.name, il.seq, ix.seqno;
