-- name: query_store_runtime
-- dialect: mssql
/* sqlspec:mssql:query_store_runtime */
SELECT
    qry.query_id,
    plan.plan_id,
    runtime_stats.count_executions,
    runtime_stats.avg_duration,
    runtime_stats.avg_cpu_time,
    runtime_stats.avg_logical_io_reads,
    runtime_stats.last_execution_time,
    query_text.query_sql_text AS sql_text
FROM sys.query_store_query AS qry
INNER JOIN sys.query_store_query_text AS query_text ON qry.query_text_id = query_text.query_text_id
INNER JOIN sys.query_store_plan AS plan ON qry.query_id = plan.query_id
INNER JOIN sys.query_store_runtime_stats AS runtime_stats ON plan.plan_id = runtime_stats.plan_id
ORDER BY runtime_stats.last_execution_time DESC;
