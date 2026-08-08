-- name: by_owner
-- dialect: oracle
SELECT
    t.owner AS schema_name,
    t.table_name,
    t.tablespace_name,
    t.cluster_name,
    t.iot_name,
    t.status,
    t.pct_free,
    t.pct_used,
    t.ini_trans,
    t.max_trans,
    t.logging,
    t.num_rows,
    t.blocks,
    t.empty_blocks,
    t.avg_space,
    t.chain_cnt,
    t.avg_row_len,
    t.degree,
    t.instances,
    t.cache,
    t.table_lock,
    t.partitioned,
    t.temporary,
    t.secondary,
    t.nested,
    t.dropped,
    t.read_only,
    t.segment_created,
    t.result_cache,
    c.comments AS table_comment
FROM all_tables t
LEFT JOIN all_tab_comments c
  ON c.owner = t.owner
  AND c.table_name = t.table_name
WHERE t.owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY t.owner, t.table_name;

-- name: tables_by_schema
-- dialect: oracle
SELECT
    table_name,
    MAX(LEVEL) AS dependency_level
FROM all_constraints
WHERE owner = COALESCE(:schema_name, USER)
START WITH table_name NOT IN (
    SELECT table_name
    FROM all_constraints
    WHERE constraint_type = 'R'
      AND owner = COALESCE(:schema_name, USER)
)
CONNECT BY NOCYCLE PRIOR constraint_name = r_constraint_name
    AND PRIOR owner = owner
GROUP BY table_name
ORDER BY dependency_level, table_name;

-- name: all_tables_by_schema
-- dialect: oracle
SELECT
    table_name
FROM all_tables
WHERE owner = COALESCE(:schema_name, USER)
ORDER BY table_name;
