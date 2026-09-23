-- name: by_schema
-- dialect: db2
SELECT
    RTRIM(tc.TABSCHEMA) AS schema_name,
    RTRIM(tc.TABNAME) AS table_name,
    RTRIM(tc.CONSTNAME) AS constraint_name,
    CASE tc.TYPE
        WHEN 'P' THEN 'PRIMARY KEY'
        WHEN 'U' THEN 'UNIQUE'
        WHEN 'F' THEN 'FOREIGN KEY'
        WHEN 'K' THEN 'CHECK'
        ELSE tc.TYPE
    END AS constraint_type,
    RTRIM(ck.TEXT) AS check_clause,
    tc.ENFORCED AS is_enforced
FROM SYSCAT.TABCONST tc
LEFT JOIN SYSCAT.CHECKS ck
  ON tc.TABSCHEMA = ck.TABSCHEMA AND tc.TABNAME = ck.TABNAME AND tc.CONSTNAME = ck.CONSTNAME
WHERE tc.TABSCHEMA NOT LIKE 'SYS%'
  AND tc.TABSCHEMA NOT LIKE 'NULLID%'
  AND tc.TABSCHEMA NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR tc.TABSCHEMA = :schema_name)
  AND (:table_name IS NULL OR tc.TABNAME = :table_name)
ORDER BY tc.TABSCHEMA, tc.TABNAME, tc.CONSTNAME;

-- name: by_table
-- dialect: db2
SELECT
    RTRIM(tc.TABSCHEMA) AS schema_name,
    RTRIM(tc.TABNAME) AS table_name,
    RTRIM(tc.CONSTNAME) AS constraint_name,
    CASE tc.TYPE
        WHEN 'P' THEN 'PRIMARY KEY'
        WHEN 'U' THEN 'UNIQUE'
        WHEN 'F' THEN 'FOREIGN KEY'
        WHEN 'K' THEN 'CHECK'
        ELSE tc.TYPE
    END AS constraint_type,
    RTRIM(ck.TEXT) AS check_clause,
    tc.ENFORCED AS is_enforced
FROM SYSCAT.TABCONST tc
LEFT JOIN SYSCAT.CHECKS ck
  ON tc.TABSCHEMA = ck.TABSCHEMA AND tc.TABNAME = ck.TABNAME AND tc.CONSTNAME = ck.CONSTNAME
WHERE tc.TABSCHEMA NOT LIKE 'SYS%'
  AND tc.TABSCHEMA NOT LIKE 'NULLID%'
  AND tc.TABSCHEMA NOT LIKE 'SQLJ%'
  AND tc.TABNAME = :table_name
  AND (:schema_name IS NULL OR tc.TABSCHEMA = :schema_name)
ORDER BY tc.CONSTNAME;
