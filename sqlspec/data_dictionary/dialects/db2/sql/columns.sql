-- name: by_schema
-- dialect: db2
SELECT
    RTRIM(c.TABSCHEMA) AS schema_name,
    RTRIM(c.TABNAME) AS table_name,
    RTRIM(c.COLNAME) AS column_name,
    RTRIM(c.TYPENAME) AS data_type,
    CASE c.NULLS WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable,
    c.DEFAULT AS column_default,
    c.COLNO + 1 AS ordinal_position,
    c.LENGTH AS max_length,
    c.SCALE AS numeric_scale,
    CASE WHEN c.KEYSEQ IS NOT NULL AND c.KEYSEQ > 0 THEN 1 ELSE 0 END AS is_primary,
    CASE WHEN c.KEYSEQ IS NOT NULL AND c.KEYSEQ > 0 THEN 1 ELSE 0 END AS is_unique,
    c.IDENTITY AS identity_generation,
    CASE WHEN c.GENERATED IN ('A', 'D') THEN 1 ELSE 0 END AS is_generated,
    c.REMARKS AS column_comment
FROM SYSCAT.COLUMNS c
JOIN SYSCAT.TABLES t ON c.TABSCHEMA = t.TABSCHEMA AND c.TABNAME = t.TABNAME
WHERE c.TABSCHEMA NOT LIKE 'SYS%'
  AND c.TABSCHEMA NOT LIKE 'NULLID%'
  AND c.TABSCHEMA NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR c.TABSCHEMA = :schema_name)
  AND (:table_name IS NULL OR c.TABNAME = :table_name)
ORDER BY c.TABSCHEMA, c.TABNAME, c.COLNO;

-- name: by_table
-- dialect: db2
SELECT
    RTRIM(c.TABSCHEMA) AS schema_name,
    RTRIM(c.TABNAME) AS table_name,
    RTRIM(c.COLNAME) AS column_name,
    RTRIM(c.TYPENAME) AS data_type,
    CASE c.NULLS WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable,
    c.DEFAULT AS column_default,
    c.COLNO + 1 AS ordinal_position,
    c.LENGTH AS max_length,
    c.SCALE AS numeric_scale,
    CASE WHEN c.KEYSEQ IS NOT NULL AND c.KEYSEQ > 0 THEN 1 ELSE 0 END AS is_primary,
    CASE WHEN c.KEYSEQ IS NOT NULL AND c.KEYSEQ > 0 THEN 1 ELSE 0 END AS is_unique,
    c.IDENTITY AS identity_generation,
    CASE WHEN c.GENERATED IN ('A', 'D') THEN 1 ELSE 0 END AS is_generated,
    c.REMARKS AS column_comment
FROM SYSCAT.COLUMNS c
WHERE c.TABSCHEMA NOT LIKE 'SYS%'
  AND c.TABSCHEMA NOT LIKE 'NULLID%'
  AND c.TABSCHEMA NOT LIKE 'SQLJ%'
  AND c.TABNAME = :table_name
  AND (:schema_name IS NULL OR c.TABSCHEMA = :schema_name)
ORDER BY c.COLNO;
