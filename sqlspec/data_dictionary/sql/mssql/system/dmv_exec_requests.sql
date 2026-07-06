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
