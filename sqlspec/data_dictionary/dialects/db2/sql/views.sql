-- name: by_schema
-- dialect: db2
SELECT
    RTRIM(v.VIEWSCHEMA) AS schema_name,
    RTRIM(v.VIEWNAME) AS view_name,
    v.TEXT AS definition,
    CASE v.READONLY WHEN 'Y' THEN 1 ELSE 0 END AS is_read_only,
    CASE v.VALID WHEN 'Y' THEN 1 ELSE 0 END AS is_valid
FROM SYSCAT.VIEWS v
WHERE v.VIEWSCHEMA NOT LIKE 'SYS%'
  AND v.VIEWSCHEMA NOT LIKE 'NULLID%'
  AND v.VIEWSCHEMA NOT LIKE 'SQLJ%'
  AND (:schema_name IS NULL OR v.VIEWSCHEMA = :schema_name)
  AND (:view_name IS NULL OR v.VIEWNAME = :view_name)
ORDER BY v.VIEWSCHEMA, v.VIEWNAME;
