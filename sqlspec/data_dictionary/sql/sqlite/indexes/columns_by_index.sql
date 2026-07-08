-- name: columns_by_index
-- dialect: sqlite
SELECT
    :schema_name AS schema_name,
    :index_name AS index_name,
    ix.seqno,
    ix.cid AS column_id,
    ix.name AS column_name,
    ix.desc AS is_descending,
    ix.coll AS collation,
    ix.key AS is_key_column
FROM pragma_index_xinfo(:index_name, :schema_name) AS ix
ORDER BY ix.seqno;
