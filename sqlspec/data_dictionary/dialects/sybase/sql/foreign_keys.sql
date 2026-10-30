-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    co.name AS constraint_name,
    fc.name AS column_name,
    ru.name AS referenced_schema,
    ro.name AS referenced_table,
    rc.name AS referenced_column
FROM dbo.sysreferences r
JOIN dbo.sysobjects o ON r.tableid = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.sysobjects co ON r.constrid = co.id
JOIN dbo.sysobjects ro ON r.reftabid = ro.id
JOIN dbo.sysusers ru ON ro.uid = ru.uid
JOIN dbo.syscolumns fc ON fc.id = o.id AND fc.colid = r.fokey1
JOIN dbo.syscolumns rc ON rc.id = ro.id AND rc.colid = r.refkey1
WHERE u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name, co.name;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    co.name AS constraint_name,
    fc.name AS column_name,
    ru.name AS referenced_schema,
    ro.name AS referenced_table,
    rc.name AS referenced_column
FROM dbo.sysreferences r
JOIN dbo.sysobjects o ON r.tableid = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.sysobjects co ON r.constrid = co.id
JOIN dbo.sysobjects ro ON r.reftabid = ro.id
JOIN dbo.sysusers ru ON ro.uid = ru.uid
JOIN dbo.syscolumns fc ON fc.id = o.id AND fc.colid = r.fokey1
JOIN dbo.syscolumns rc ON rc.id = ro.id AND rc.colid = r.refkey1
WHERE o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY co.name;
