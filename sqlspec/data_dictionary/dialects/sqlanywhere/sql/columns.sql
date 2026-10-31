-- name: by_schema
-- dialect: sqlanywhere
WITH pk_cols AS (
    SELECT DISTINCT
        i.table_id,
        ic.column_id
    FROM SYS.SYSIDX i
    JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
    WHERE i.index_category = 1
),
unique_cols AS (
    SELECT DISTINCT
        i.table_id,
        ic.column_id
    FROM SYS.SYSIDX i
    JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
    WHERE i."unique" IN (1, 2)
)
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    c.column_name AS column_name,
    d.domain_name AS data_type,
    CASE c.nulls WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
    c."default" AS column_default,
    c.column_id AS ordinal_position,
    c.width AS char_length,
    c.width AS max_length,
    c.width AS numeric_precision,
    c.scale AS numeric_scale,
    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    CASE WHEN uq.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN c."default" LIKE '%autoincrement%' THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN c."default" LIKE '%autoincrement%' THEN 'ALWAYS' ELSE NULL END AS identity_generation,
    CASE WHEN c.column_type = 'C' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN c.column_type = 'C' THEN c."default" ELSE NULL END AS generation_expression,
    0 AS is_hidden,
    NULL AS column_comment
FROM SYS.SYSTABCOL c
JOIN SYS.SYSTAB t ON c.table_id = t.table_id
JOIN SYS.SYSUSER u ON t.creator = u.user_id
JOIN SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
LEFT JOIN pk_cols pk ON pk.table_id = t.table_id AND pk.column_id = c.column_id
LEFT JOIN unique_cols uq ON uq.table_id = t.table_id AND uq.column_id = c.column_id
WHERE t.table_type IN (1, 21)
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
  AND (:table_name IS NULL OR t.table_name = :table_name)
ORDER BY u.user_name, t.table_name, c.column_id;

-- name: by_table
-- dialect: sqlanywhere
WITH pk_cols AS (
    SELECT DISTINCT
        i.table_id,
        ic.column_id
    FROM SYS.SYSIDX i
    JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
    WHERE i.index_category = 1
),
unique_cols AS (
    SELECT DISTINCT
        i.table_id,
        ic.column_id
    FROM SYS.SYSIDX i
    JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
    WHERE i."unique" IN (1, 2)
)
SELECT
    u.user_name AS schema_name,
    t.table_name AS table_name,
    c.column_name AS column_name,
    d.domain_name AS data_type,
    CASE c.nulls WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
    c."default" AS column_default,
    c.column_id AS ordinal_position,
    c.width AS char_length,
    c.width AS max_length,
    c.width AS numeric_precision,
    c.scale AS numeric_scale,
    CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    CASE WHEN uq.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN c."default" LIKE '%autoincrement%' THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN c."default" LIKE '%autoincrement%' THEN 'ALWAYS' ELSE NULL END AS identity_generation,
    CASE WHEN c.column_type = 'C' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN c.column_type = 'C' THEN c."default" ELSE NULL END AS generation_expression,
    0 AS is_hidden,
    NULL AS column_comment
FROM SYS.SYSTABCOL c
JOIN SYS.SYSTAB t ON c.table_id = t.table_id
JOIN SYS.SYSUSER u ON t.creator = u.user_id
JOIN SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
LEFT JOIN pk_cols pk ON pk.table_id = t.table_id AND pk.column_id = c.column_id
LEFT JOIN unique_cols uq ON uq.table_id = t.table_id AND uq.column_id = c.column_id
WHERE t.table_type IN (1, 21)
  AND t.table_name = :table_name
  AND u.user_name = COALESCE(:schema_name, CURRENT USER)
ORDER BY c.column_id;
