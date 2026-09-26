-- name: by_schema
-- dialect: db2
SELECT
    RTRIM(s.SCHEMANAME) AS schema_name,
    RTRIM(s.OWNER) AS owner_name,
    s.CREATE_TIME AS create_time,
    s.REMARKS AS comment
FROM SYSCAT.SCHEMATA s
WHERE s.SCHEMANAME NOT LIKE 'SYS%'
  AND s.SCHEMANAME NOT LIKE 'NULLID%'
  AND s.SCHEMANAME NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR s.SCHEMANAME = :schema_name)
ORDER BY s.SCHEMANAME;

-- name: current
-- dialect: db2
VALUES CURRENT SCHEMA;
