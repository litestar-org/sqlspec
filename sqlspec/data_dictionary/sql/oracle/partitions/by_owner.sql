-- name: by_owner
-- dialect: oracle
SELECT
    table_owner AS schema_name,
    table_name,
    partition_name,
    composite,
    subpartition_count,
    high_value,
    high_value_length,
    partition_position,
    tablespace_name,
    pct_free,
    pct_used,
    ini_trans,
    max_trans,
    initial_extent,
    next_extent,
    min_extent,
    max_extent,
    max_size,
    pct_increase,
    freelists,
    freelist_groups,
    logging,
    compression,
    compress_for,
    num_rows,
    blocks,
    empty_blocks,
    avg_space,
    chain_cnt,
    avg_row_len,
    sample_size,
    last_analyzed,
    interval,
    segment_created,
    indexing,
    read_only,
    inmemory
FROM all_tab_partitions
WHERE table_owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR table_name = :table_name)
ORDER BY table_owner, table_name, partition_position;
