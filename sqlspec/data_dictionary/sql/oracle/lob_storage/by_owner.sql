-- name: by_owner
-- dialect: oracle
SELECT
    owner AS schema_name,
    table_name,
    column_name,
    segment_name,
    tablespace_name,
    index_name,
    chunk,
    pctversion,
    retention,
    freepools,
    cache,
    logging,
    encrypt,
    compression,
    deduplication,
    in_row,
    format,
    partitioned,
    securefile,
    retention_type,
    retention_value
FROM all_lobs
WHERE owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR table_name = :table_name)
ORDER BY owner, table_name, column_name;
