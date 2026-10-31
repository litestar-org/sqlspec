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
CROSS JOIN (
    SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
) k
JOIN dbo.syscolumns fc ON fc.id = o.id AND fc.colid = CASE k.pos
    WHEN 1 THEN r.fokey1 WHEN 2 THEN r.fokey2 WHEN 3 THEN r.fokey3 WHEN 4 THEN r.fokey4
    WHEN 5 THEN r.fokey5 WHEN 6 THEN r.fokey6 WHEN 7 THEN r.fokey7 WHEN 8 THEN r.fokey8
    WHEN 9 THEN r.fokey9 WHEN 10 THEN r.fokey10 WHEN 11 THEN r.fokey11 WHEN 12 THEN r.fokey12
    WHEN 13 THEN r.fokey13 WHEN 14 THEN r.fokey14 WHEN 15 THEN r.fokey15 WHEN 16 THEN r.fokey16
END
JOIN dbo.syscolumns rc ON rc.id = ro.id AND rc.colid = CASE k.pos
    WHEN 1 THEN r.refkey1 WHEN 2 THEN r.refkey2 WHEN 3 THEN r.refkey3 WHEN 4 THEN r.refkey4
    WHEN 5 THEN r.refkey5 WHEN 6 THEN r.refkey6 WHEN 7 THEN r.refkey7 WHEN 8 THEN r.refkey8
    WHEN 9 THEN r.refkey9 WHEN 10 THEN r.refkey10 WHEN 11 THEN r.refkey11 WHEN 12 THEN r.refkey12
    WHEN 13 THEN r.refkey13 WHEN 14 THEN r.refkey14 WHEN 15 THEN r.refkey15 WHEN 16 THEN r.refkey16
END
WHERE k.pos <= r.frgcnt
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name, co.name, k.pos;

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
CROSS JOIN (
    SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
    UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
    UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
) k
JOIN dbo.syscolumns fc ON fc.id = o.id AND fc.colid = CASE k.pos
    WHEN 1 THEN r.fokey1 WHEN 2 THEN r.fokey2 WHEN 3 THEN r.fokey3 WHEN 4 THEN r.fokey4
    WHEN 5 THEN r.fokey5 WHEN 6 THEN r.fokey6 WHEN 7 THEN r.fokey7 WHEN 8 THEN r.fokey8
    WHEN 9 THEN r.fokey9 WHEN 10 THEN r.fokey10 WHEN 11 THEN r.fokey11 WHEN 12 THEN r.fokey12
    WHEN 13 THEN r.fokey13 WHEN 14 THEN r.fokey14 WHEN 15 THEN r.fokey15 WHEN 16 THEN r.fokey16
END
JOIN dbo.syscolumns rc ON rc.id = ro.id AND rc.colid = CASE k.pos
    WHEN 1 THEN r.refkey1 WHEN 2 THEN r.refkey2 WHEN 3 THEN r.refkey3 WHEN 4 THEN r.refkey4
    WHEN 5 THEN r.refkey5 WHEN 6 THEN r.refkey6 WHEN 7 THEN r.refkey7 WHEN 8 THEN r.refkey8
    WHEN 9 THEN r.refkey9 WHEN 10 THEN r.refkey10 WHEN 11 THEN r.refkey11 WHEN 12 THEN r.refkey12
    WHEN 13 THEN r.refkey13 WHEN 14 THEN r.refkey14 WHEN 15 THEN r.refkey15 WHEN 16 THEN r.refkey16
END
WHERE k.pos <= r.frgcnt
  AND o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY co.name, k.pos;
