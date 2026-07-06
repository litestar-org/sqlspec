-- name: list
-- dialect: mssql
SELECT
    s.name AS schema_name,
    s.schema_id,
    USER_NAME(s.principal_id) AS owner_name
FROM sys.schemas AS s
WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
ORDER BY s.name;
