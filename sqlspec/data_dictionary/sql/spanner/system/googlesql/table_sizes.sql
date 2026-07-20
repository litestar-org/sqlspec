-- name: table_sizes
-- dialect: spanner
SELECT
    INTERVAL_END,
    TABLE_NAME,
    USED_BYTES
FROM SPANNER_SYS.TABLE_SIZES_STATS_1HOUR
WHERE (CAST(:table_name AS STRING) IS NULL OR TABLE_NAME = :table_name)
  AND (CAST(:interval_end_after AS TIMESTAMP) IS NULL OR INTERVAL_END >= :interval_end_after)
ORDER BY INTERVAL_END DESC, TABLE_NAME
LIMIT :limit;
