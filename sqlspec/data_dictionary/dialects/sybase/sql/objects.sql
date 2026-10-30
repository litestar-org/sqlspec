-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS object_name,
    CASE o.type
        WHEN 'U' THEN 'TABLE'
        WHEN 'V' THEN 'VIEW'
        WHEN 'P' THEN 'PROCEDURE'
        WHEN 'SF' THEN 'FUNCTION'
        WHEN 'TR' THEN 'TRIGGER'
        ELSE o.type
    END AS object_type,
    o.crdate AS created,
    NULL AS remarks
FROM dbo.sysobjects o
JOIN dbo.sysusers u ON o.uid = u.uid
WHERE o.type IN ('U', 'V', 'P', 'SF', 'TR')
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:object_name IS NULL OR o.name = :object_name)
ORDER BY u.name, object_type, o.name;
