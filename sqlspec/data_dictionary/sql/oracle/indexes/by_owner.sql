-- name: by_owner
-- dialect: oracle
SELECT
    i.owner AS schema_name,
    i.table_name,
    i.index_name,
    i.index_type,
    i.uniqueness,
    i.compression,
    i.prefix_length,
    i.tablespace_name,
    i.status,
    i.partitioned,
    i.temporary,
    i.generated,
    i.visibility,
    i.blevel,
    i.leaf_blocks,
    i.distinct_keys,
    i.avg_leaf_blocks_per_key,
    i.avg_data_blocks_per_key,
    i.clustering_factor,
    LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns,
    LISTAGG(ie.column_expression, ',') WITHIN GROUP (ORDER BY ie.column_position) AS expressions
FROM all_indexes i
LEFT JOIN all_ind_columns ic
  ON ic.index_owner = i.owner
  AND ic.index_name = i.index_name
LEFT JOIN all_ind_expressions ie
  ON ie.index_owner = i.owner
  AND ie.index_name = i.index_name
  AND ie.column_position = ic.column_position
WHERE i.owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR i.table_name = :table_name)
GROUP BY
    i.owner,
    i.table_name,
    i.index_name,
    i.index_type,
    i.uniqueness,
    i.compression,
    i.prefix_length,
    i.tablespace_name,
    i.status,
    i.partitioned,
    i.temporary,
    i.generated,
    i.visibility,
    i.blevel,
    i.leaf_blocks,
    i.distinct_keys,
    i.avg_leaf_blocks_per_key,
    i.avg_data_blocks_per_key,
    i.clustering_factor
ORDER BY i.owner, i.table_name, i.index_name;
