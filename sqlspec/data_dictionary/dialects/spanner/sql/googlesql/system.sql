-- name: query_stats_top
-- dialect: spanner
SELECT
    INTERVAL_END AS interval_end,
    TEXT AS text,
    TEXT AS query_text,
    TEXT_FINGERPRINT AS text_fingerprint,
    EXECUTION_COUNT AS execution_count,
    AVG_LATENCY_SECONDS AS avg_latency_seconds,
    AVG_ROWS AS avg_rows,
    AVG_BYTES AS avg_bytes,
    AVG_CPU_SECONDS AS avg_cpu_seconds
FROM SPANNER_SYS.QUERY_STATS_TOP_MINUTE
WHERE (CAST(:interval_end_after AS TIMESTAMP) IS NULL OR INTERVAL_END >= :interval_end_after)
ORDER BY INTERVAL_END DESC, EXECUTION_COUNT DESC
LIMIT :limit;

-- name: table_sizes
-- dialect: spanner
SELECT
    INTERVAL_END AS interval_end,
    TABLE_NAME AS table_name,
    USED_BYTES AS used_bytes
FROM SPANNER_SYS.TABLE_SIZES_STATS_1HOUR
WHERE (CAST(:table_name AS STRING) IS NULL OR TABLE_NAME = :table_name)
  AND (CAST(:interval_end_after AS TIMESTAMP) IS NULL OR INTERVAL_END >= :interval_end_after)
ORDER BY INTERVAL_END DESC, TABLE_NAME
LIMIT :limit;
