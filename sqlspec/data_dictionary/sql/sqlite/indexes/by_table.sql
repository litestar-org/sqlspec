-- name: by_table
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    :table_name AS table_name,
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
FROM pragma_index_list(:table_name, :schema_name) AS il
JOIN pragma_index_xinfo(il.name, :schema_name) AS ix
LEFT JOIN {schema_prefix}sqlite_schema AS sm
  ON sm.type = 'index'
 AND sm.name = il.name
WHERE il.name NOT LIKE 'sqlite_%'
ORDER BY il.seq, ix.seqno;
