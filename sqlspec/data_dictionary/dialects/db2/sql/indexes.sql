-- name: by_schema
-- dialect: db2
SELECT
    RTRIM(i.TABSCHEMA) AS schema_name,
    RTRIM(i.TABNAME) AS table_name,
    RTRIM(i.INDNAME) AS index_name,
    RTRIM(c.COLNAME) AS column_name,
    c.COLSEQ AS column_position,
    CASE i.UNIQUERULE WHEN 'D' THEN 0 ELSE 1 END AS is_unique,
    CASE i.UNIQUERULE WHEN 'P' THEN 1 ELSE 0 END AS is_primary
FROM SYSCAT.INDEXES i
JOIN SYSCAT.INDEXCOLUSE c
  ON i.INDSCHEMA = c.INDSCHEMA AND i.INDNAME = c.INDNAME
WHERE i.TABSCHEMA NOT LIKE 'SYS%'
  AND i.TABSCHEMA NOT LIKE 'NULLID%'
  AND i.TABSCHEMA NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR i.TABSCHEMA = :schema_name)
  AND (:table_name IS NULL OR i.TABNAME = :table_name)
ORDER BY i.TABSCHEMA, i.TABNAME, i.INDNAME, c.COLSEQ;

-- name: by_table
-- dialect: db2
SELECT
    RTRIM(i.TABSCHEMA) AS schema_name,
    RTRIM(i.TABNAME) AS table_name,
    RTRIM(i.INDNAME) AS index_name,
    RTRIM(c.COLNAME) AS column_name,
    c.COLSEQ AS column_position,
    CASE i.UNIQUERULE WHEN 'D' THEN 0 ELSE 1 END AS is_unique,
    CASE i.UNIQUERULE WHEN 'P' THEN 1 ELSE 0 END AS is_primary
FROM SYSCAT.INDEXES i
JOIN SYSCAT.INDEXCOLUSE c
  ON i.INDSCHEMA = c.INDSCHEMA AND i.INDNAME = c.INDNAME
WHERE i.TABSCHEMA NOT LIKE 'SYS%'
  AND i.TABSCHEMA NOT LIKE 'NULLID%'
  AND i.TABSCHEMA NOT LIKE 'SQLJ%'
  AND i.TABNAME = :table_name
  AND (:schema_name IS NULL OR i.TABSCHEMA = :schema_name)
ORDER BY i.INDNAME, c.COLSEQ;
