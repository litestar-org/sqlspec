-- name: by_schema
-- dialect: db2
WITH dependency_tree (schema_name, table_name, level, path) AS (
    SELECT
        RTRIM(t.TABSCHEMA) AS schema_name,
        RTRIM(t.TABNAME) AS table_name,
        0 AS level,
        VARCHAR('/' || RTRIM(t.TABSCHEMA) || '.' || RTRIM(t.TABNAME) || '/', 2000) AS path
    FROM SYSCAT.TABLES t
    WHERE t.TYPE = 'T'
      AND t.TABSCHEMA NOT LIKE 'SYS%'
      AND t.TABSCHEMA NOT LIKE 'NULLID%'
      AND t.TABSCHEMA NOT LIKE 'SQLJ%'
      AND (:schema_name IS NULL OR t.TABSCHEMA = :schema_name)
      AND (:table_name IS NULL OR t.TABNAME = :table_name)
      AND NOT EXISTS (
          SELECT 1
          FROM SYSCAT.REFERENCES r
          WHERE r.TABSCHEMA = t.TABSCHEMA
            AND r.TABNAME = t.TABNAME
      )

    UNION ALL

    SELECT
        RTRIM(child.TABSCHEMA) AS schema_name,
        RTRIM(child.TABNAME) AS table_name,
        dt.level + 1 AS level,
        VARCHAR(dt.path || RTRIM(child.TABSCHEMA) || '.' || RTRIM(child.TABNAME) || '/', 2000) AS path
    FROM SYSCAT.REFERENCES r
    JOIN SYSCAT.TABLES child ON r.TABSCHEMA = child.TABSCHEMA AND r.TABNAME = child.TABNAME
    JOIN dependency_tree dt ON r.REFTABSCHEMA = dt.schema_name AND r.REFTABNAME = dt.table_name
    WHERE child.TYPE = 'T'
      AND child.TABSCHEMA NOT LIKE 'SYS%'
      AND child.TABSCHEMA NOT LIKE 'NULLID%'
      AND child.TABSCHEMA NOT LIKE 'SQLJ%'
      AND (:schema_name IS NULL OR child.TABSCHEMA = :schema_name)
      AND (:table_name IS NULL OR child.TABNAME = :table_name)
      AND LOCATE('/' || RTRIM(child.TABSCHEMA) || '.' || RTRIM(child.TABNAME) || '/', dt.path) = 0
)
SELECT
    dt.schema_name,
    dt.table_name,
    'BASE TABLE' AS table_type,
    MAX(dt.level) AS dependency_level,
    MAX(dt.level) AS level
FROM dependency_tree dt
GROUP BY dt.schema_name, dt.table_name
ORDER BY dependency_level, dt.schema_name, dt.table_name;

-- name: all_by_schema
-- dialect: db2
SELECT
    RTRIM(t.TABSCHEMA) AS schema_name,
    RTRIM(t.TABNAME) AS table_name,
    CASE t.TYPE
        WHEN 'T' THEN 'BASE TABLE'
        WHEN 'V' THEN 'VIEW'
        WHEN 'A' THEN 'ALIAS'
        WHEN 'N' THEN 'NICKNAME'
        ELSE 'TABLE'
    END AS table_type,
    0 AS dependency_level,
    0 AS level,
    t.CARD AS estimated_row_count,
    t.REMARKS AS table_comment
FROM SYSCAT.TABLES t
WHERE t.TABSCHEMA NOT LIKE 'SYS%'
  AND t.TABSCHEMA NOT LIKE 'NULLID%'
  AND t.TABSCHEMA NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR t.TABSCHEMA = :schema_name)
  AND (:table_name IS NULL OR t.TABNAME = :table_name)
ORDER BY t.TABSCHEMA, t.TABNAME;

-- name: names_by_schema
-- dialect: db2
SELECT
    RTRIM(t.TABNAME) AS table_name
FROM SYSCAT.TABLES t
WHERE t.TYPE = 'T'
  AND t.TABSCHEMA NOT LIKE 'SYS%'
  AND t.TABSCHEMA NOT LIKE 'NULLID%'
  AND t.TABSCHEMA NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR t.TABSCHEMA = :schema_name)
ORDER BY t.TABNAME;
