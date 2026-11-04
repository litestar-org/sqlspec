-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS view_name,
    CASE WHEN (COALESCE(cm.status, 0) & 1) = 0 THEN cm.text ELSE NULL END AS definition,
    cm.id AS _definition_id,
    cm.number AS _definition_number,
    cm.colid2 AS _definition_colid2,
    cm.colid AS _definition_colid,
    0 AS is_read_only,
    1 AS is_valid
FROM dbo.sysobjects o
JOIN dbo.sysusers u ON o.uid = u.uid
LEFT JOIN dbo.syscomments cm ON o.id = cm.id AND cm.texttype = 0
WHERE o.type = 'V'
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:view_name IS NULL OR o.name = :view_name)
ORDER BY u.name, o.name, cm.number, cm.colid2, cm.colid;
