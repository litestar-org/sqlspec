-- name: dmv_exec_requests
-- dialect: mssql
/* sqlspec:mssql:dmv_exec_requests */
SELECT
    req.session_id,
    sess.login_name,
    req.status,
    req.command,
    req.cpu_time,
    req.total_elapsed_time,
    req.logical_reads,
    req.reads,
    req.writes,
    DB_NAME(req.database_id) AS database_name,
    txt.text AS sql_text
FROM sys.dm_exec_requests AS req
INNER JOIN sys.dm_exec_sessions AS sess ON req.session_id = sess.session_id
OUTER APPLY sys.dm_exec_sql_text(req.sql_handle) AS txt
WHERE req.session_id <> @@SPID
ORDER BY req.total_elapsed_time DESC;

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
