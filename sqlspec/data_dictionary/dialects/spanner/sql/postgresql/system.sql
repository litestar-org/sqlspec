-- name: query_stats_top
-- dialect: spanner
SELECT
    interval_end,
    text,
    text_fingerprint,
    execution_count,
    avg_latency_seconds,
    avg_rows,
    avg_bytes,
    avg_cpu_seconds
FROM spanner_sys.query_stats_top_minute
WHERE (:interval_end_after::timestamptz IS NULL OR interval_end >= :interval_end_after)
ORDER BY interval_end DESC, execution_count DESC
LIMIT :limit;

-- name: table_sizes
-- dialect: spanner
SELECT
    interval_end,
    table_name,
    used_bytes
FROM spanner_sys.table_sizes_stats_1hour
WHERE (:table_name::text IS NULL OR table_name = :table_name)
  AND (:interval_end_after::timestamptz IS NULL OR interval_end >= :interval_end_after)
ORDER BY interval_end DESC, table_name
LIMIT :limit;
