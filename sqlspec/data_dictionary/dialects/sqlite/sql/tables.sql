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
fk_edges AS MATERIALIZED (
    SELECT
        ti.table_name,
        fk."table" AS referenced_table
    FROM table_inventory AS ti
    JOIN pragma_foreign_key_list(ti.table_name, COALESCE(:schema_name, 'main')) AS fk
    WHERE fk."table" <> ti.table_name
),
dependency_tree AS (
    SELECT
        ti.table_name,
        0 AS level,
        '/' || ti.table_name || '/' AS path
    FROM table_inventory AS ti
    WHERE NOT EXISTS (
        SELECT 1
        FROM fk_edges AS e
        WHERE e.table_name = ti.table_name
    )

    UNION ALL

    SELECT
        e.table_name,
        dt.level + 1,
        dt.path || e.table_name || '/'
    FROM fk_edges AS e
    JOIN dependency_tree AS dt ON e.referenced_table = dt.table_name
    WHERE instr(dt.path, '/' || e.table_name || '/') = 0
),
dependency_levels AS (
    SELECT
        table_name,
        MAX(level) AS dependency_level
    FROM dependency_tree
    GROUP BY table_name
)
SELECT
    ti.schema_name,
    ti.table_name,
    ti.table_type,
    COALESCE(dl.dependency_level, 0) AS dependency_level,
    COALESCE(dl.dependency_level, 0) AS level,
    ti.ncol,
    ti.wr,
    ti.strict
FROM table_inventory AS ti
LEFT JOIN dependency_levels AS dl ON dl.table_name = ti.table_name
ORDER BY COALESCE(dl.dependency_level, 0), ti.table_name;
