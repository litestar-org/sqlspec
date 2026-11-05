-- name: by_schema
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    c.name AS column_name,
    t.name AS data_type,
    CASE WHEN (c.status & 8) = 8 THEN 'YES' ELSE 'NO' END AS is_nullable,
    CASE WHEN cm.id = c.cdefault AND (COALESCE(cm.status, 0) & 1) = 0
        THEN cm.text ELSE NULL END AS column_default,
    c.colid AS ordinal_position,
    c.length AS char_length,
    c.length AS max_length,
    c.prec AS numeric_precision,
    c.scale AS numeric_scale,
    CASE WHEN pk.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    CASE WHEN uq.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN (c.status & 128) = 128 THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN (c.status & 128) = 128 THEN 'ALWAYS' ELSE NULL END AS identity_generation,
    CASE WHEN (COALESCE(c.status2, 0) & 112) <> 0 THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN cm.id = c.computedcol AND (COALESCE(cm.status, 0) & 1) = 0
        THEN cm.text ELSE NULL END AS generation_expression,
    CASE WHEN (COALESCE(c.status3, 0) & 1) = 1 THEN 1 ELSE 0 END AS is_hidden,
    cm.id AS _definition_id,
    cm.number AS _definition_number,
    cm.colid2 AS _definition_colid2,
    cm.colid AS _definition_colid,
    NULL AS column_comment
FROM dbo.syscolumns c
JOIN dbo.sysobjects o ON c.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.systypes t ON c.usertype = t.usertype
LEFT JOIN dbo.syscomments cm
    ON (cm.id = c.cdefault OR cm.id = c.computedcol) AND cm.texttype = 0
LEFT JOIN (
    SELECT DISTINCT
        i.id,
        index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    WHERE (i.status & 2048) = 2048
      AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
) pk ON pk.id = o.id AND pk.col_name = c.name
LEFT JOIN (
    SELECT DISTINCT
        i.id,
        index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    WHERE (i.status & 2) = 2
      AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
) uq ON uq.id = o.id AND uq.col_name = c.name
WHERE o.type IN ('U', 'V')
  AND u.name = COALESCE(:schema_name, USER_NAME())
  AND (:table_name IS NULL OR o.name = :table_name)
ORDER BY u.name, o.name, c.colid, cm.id, cm.number, cm.colid2, cm.colid;

-- name: by_table
-- dialect: sybase
SELECT
    u.name AS schema_name,
    o.name AS table_name,
    c.name AS column_name,
    t.name AS data_type,
    CASE WHEN (c.status & 8) = 8 THEN 'YES' ELSE 'NO' END AS is_nullable,
    CASE WHEN cm.id = c.cdefault AND (COALESCE(cm.status, 0) & 1) = 0
        THEN cm.text ELSE NULL END AS column_default,
    c.colid AS ordinal_position,
    c.length AS char_length,
    c.length AS max_length,
    c.prec AS numeric_precision,
    c.scale AS numeric_scale,
    CASE WHEN pk.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    CASE WHEN uq.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN (c.status & 128) = 128 THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN (c.status & 128) = 128 THEN 'ALWAYS' ELSE NULL END AS identity_generation,
    CASE WHEN (COALESCE(c.status2, 0) & 112) <> 0 THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN cm.id = c.computedcol AND (COALESCE(cm.status, 0) & 1) = 0
        THEN cm.text ELSE NULL END AS generation_expression,
    CASE WHEN (COALESCE(c.status3, 0) & 1) = 1 THEN 1 ELSE 0 END AS is_hidden,
    cm.id AS _definition_id,
    cm.number AS _definition_number,
    cm.colid2 AS _definition_colid2,
    cm.colid AS _definition_colid,
    NULL AS column_comment
FROM dbo.syscolumns c
JOIN dbo.sysobjects o ON c.id = o.id
JOIN dbo.sysusers u ON o.uid = u.uid
JOIN dbo.systypes t ON c.usertype = t.usertype
LEFT JOIN dbo.syscomments cm
    ON (cm.id = c.cdefault OR cm.id = c.computedcol) AND cm.texttype = 0
LEFT JOIN (
    SELECT DISTINCT
        i.id,
        index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    WHERE (i.status & 2048) = 2048
      AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
) pk ON pk.id = o.id AND pk.col_name = c.name
LEFT JOIN (
    SELECT DISTINCT
        i.id,
        index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    WHERE (i.status & 2) = 2
      AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
) uq ON uq.id = o.id AND uq.col_name = c.name
WHERE o.type IN ('U', 'V')
  AND o.name = :table_name
  AND u.name = COALESCE(:schema_name, USER_NAME())
ORDER BY c.colid, cm.id, cm.number, cm.colid2, cm.colid;
