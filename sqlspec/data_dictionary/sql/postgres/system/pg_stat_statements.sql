-- name: pg_stat_statements
-- dialect: postgres
SELECT
    userid::text AS user_oid,
    dbid::text AS database_oid,
    queryid::text AS query_id,
    query::text AS query_text,
    calls,
    total_exec_time,
    mean_exec_time,
    rows
FROM pg_catalog.pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT :limit;
