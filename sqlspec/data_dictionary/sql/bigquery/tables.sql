-- name: tables_by_schema
-- dialect: bigquery
WITH RECURSIVE dependency_tree AS (
    SELECT
        t.table_name,
        0 AS level,
        [t.table_name] AS path
    FROM {tables_table} t
    WHERE t.table_type = 'BASE TABLE'
      AND NOT EXISTS (
          SELECT 1
          FROM {kcu_table} kcu
          JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
          WHERE kcu.table_name = t.table_name
      )

    UNION ALL

    SELECT
        kcu.table_name,
        dt.level + 1,
        ARRAY_CONCAT(dt.path, [kcu.table_name])
    FROM {kcu_table} kcu
    JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
    JOIN {kcu_table} pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND kcu.ordinal_position = pk_kcu.ordinal_position
    JOIN dependency_tree dt ON pk_kcu.table_name = dt.table_name
    WHERE kcu.table_name NOT IN UNNEST(dt.path)
)
SELECT DISTINCT table_name
FROM dependency_tree
ORDER BY level, table_name;
