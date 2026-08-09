-- name: by_schema
-- dialect: sqlite
WITH table_inventory AS (
    SELECT
        tl.schema AS schema_name,
        tl.name AS table_name,
        tl.type AS table_type,
        tl.ncol,
        tl.wr,
        tl.strict
    FROM pragma_table_list AS tl
    WHERE tl.schema = COALESCE(:schema_name, 'main')
      AND tl.type IN ('table', 'virtual')
      AND tl.name NOT LIKE 'sqlite_%'
),
dependency_tree AS (
    SELECT
        ti.table_name,
        0 AS level,
        '/' || ti.table_name || '/' AS path
    FROM table_inventory AS ti
    WHERE NOT EXISTS (
        SELECT 1
        FROM pragma_foreign_key_list(ti.table_name, COALESCE(:schema_name, 'main'))
    )

    UNION ALL

    SELECT
        ti.table_name,
        dt.level + 1,
        dt.path || ti.table_name || '/'
    FROM table_inventory AS ti
    JOIN pragma_foreign_key_list(ti.table_name, COALESCE(:schema_name, 'main')) AS fk
    JOIN dependency_tree AS dt ON fk."table" = dt.table_name
    WHERE instr(dt.path, '/' || ti.table_name || '/') = 0
)
SELECT DISTINCT
    ti.schema_name,
    ti.table_name,
    ti.table_type,
    COALESCE(dt.level, 0) AS dependency_level,
    COALESCE(dt.level, 0) AS level,
    ti.ncol,
    ti.wr,
    ti.strict
FROM table_inventory AS ti
LEFT JOIN dependency_tree AS dt ON dt.table_name = ti.table_name
ORDER BY COALESCE(dt.level, 0), ti.table_name;
