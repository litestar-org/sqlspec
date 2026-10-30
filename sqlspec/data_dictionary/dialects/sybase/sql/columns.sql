-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    c.name AS column_name,
    t.name AS data_type,
    CASE WHEN (c.status & 8) = 8 THEN 'YES' ELSE 'NO' END AS is_nullable,
    cm.text AS column_default,
    c.colid AS ordinal_position,
    c.length AS char_length,
    c.length AS max_length,
    c.prec AS numeric_precision,
    c.scale AS numeric_scale,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dbo.sysindexes i
            WHERE i.id = o.id
              AND (i.status & 2048) = 2048
              AND (
                  index_col(u.name + '.' + o.name, i.indid, 1) = c.name
                  OR index_col(u.name + '.' + o.name, i.indid, 2) = c.name
                  OR index_col(u.name + '.' + o.name, i.indid, 3) = c.name
                  OR index_col(u.name + '.' + o.name, i.indid, 4) = c.name
              )
        ) THEN 1
        ELSE 0
    END AS is_primary,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dbo.sysindexes i
            WHERE i.id = o.id
              AND (i.status & 2) = 2
              AND i.keycnt = 1
              AND index_col(u.name + '.' + o.name, i.indid, 1) = c.name
        ) THEN 1
        ELSE 0
    END AS is_unique,
    CASE WHEN (c.status & 128) = 128 THEN 'YES' ELSE NULL END AS identity_generation,
    0 AS is_generated,
    NULL AS column_comment
FROM dbo.syscolumns c
JOIN dbo.sysobjects o ON c.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.systypes t ON c.usertype = t.usertype
LEFT JOIN dbo.syscomments cm ON c.cdefault = cm.id AND cm.colid = 1
WHERE o.type IN ('U', 'V')
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name, c.colid;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    c.name AS column_name,
    t.name AS data_type,
    CASE WHEN (c.status & 8) = 8 THEN 'YES' ELSE 'NO' END AS is_nullable,
    cm.text AS column_default,
    c.colid AS ordinal_position,
    c.length AS char_length,
    c.length AS max_length,
    c.prec AS numeric_precision,
    c.scale AS numeric_scale,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dbo.sysindexes i
            WHERE i.id = o.id
              AND (i.status & 2048) = 2048
              AND (
                  index_col(u.name + '.' + o.name, i.indid, 1) = c.name
                  OR index_col(u.name + '.' + o.name, i.indid, 2) = c.name
                  OR index_col(u.name + '.' + o.name, i.indid, 3) = c.name
                  OR index_col(u.name + '.' + o.name, i.indid, 4) = c.name
              )
        ) THEN 1
        ELSE 0
    END AS is_primary,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM dbo.sysindexes i
            WHERE i.id = o.id
              AND (i.status & 2) = 2
              AND i.keycnt = 1
              AND index_col(u.name + '.' + o.name, i.indid, 1) = c.name
        ) THEN 1
        ELSE 0
    END AS is_unique,
    CASE WHEN (c.status & 128) = 128 THEN 'YES' ELSE NULL END AS identity_generation,
    0 AS is_generated,
    NULL AS column_comment
FROM dbo.syscolumns c
JOIN dbo.sysobjects o ON c.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.systypes t ON c.usertype = t.usertype
LEFT JOIN dbo.syscomments cm ON c.cdefault = cm.id AND cm.colid = 1
WHERE o.type IN ('U', 'V')
  AND o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY c.colid;
