-- name: table_sizes
-- dialect: spanner
SELECT
    INTERVAL_END,
    TABLE_NAME,
    USED_BYTES
FROM SPANNER_SYS.TABLE_SIZES_STATS_1HOUR
WHERE (:table_name IS NULL OR TABLE_NAME = :table_name)
  AND (:interval_end_after IS NULL OR INTERVAL_END >= :interval_end_after)
ORDER BY INTERVAL_END DESC, TABLE_NAME
LIMIT :limit;
