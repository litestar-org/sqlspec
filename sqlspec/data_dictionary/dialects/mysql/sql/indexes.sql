-- name: by_schema
-- dialect: mysql
SELECT
    table_schema AS schema_name,
    table_name,
    index_name,
    non_unique = 0 AS is_unique,
    index_name = 'PRIMARY' AS is_primary,
    index_type,
    is_visible,
    GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns,
    GROUP_CONCAT(sub_part ORDER BY seq_in_index) AS prefix_lengths,
    GROUP_CONCAT(collation ORDER BY seq_in_index) AS column_orders,
    index_comment
FROM information_schema.statistics
WHERE table_schema = COALESCE(:schema_name, DATABASE())
  AND (:table_name IS NULL OR table_name = :table_name)
GROUP BY table_schema, table_name, index_name, non_unique, index_type, is_visible, index_comment
ORDER BY table_schema, table_name, index_name;

-- name: indexes_by_table
-- dialect: mysql
SELECT
    s.index_name AS index_name,
    s.table_name AS table_name,
    CASE WHEN s.non_unique = 0 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN s.index_name = 'PRIMARY' THEN 1 ELSE 0 END AS is_primary,
    GROUP_CONCAT(s.column_name ORDER BY s.seq_in_index) AS columns
FROM information_schema.statistics s
WHERE s.table_schema = COALESCE(:schema_name, DATABASE())
  AND s.table_name = :table_name
GROUP BY s.index_name, s.table_name, s.non_unique
ORDER BY s.index_name;

-- name: indexes_by_schema
-- dialect: mysql
SELECT
    s.index_name AS index_name,
    s.table_name AS table_name,
    CASE WHEN s.non_unique = 0 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN s.index_name = 'PRIMARY' THEN 1 ELSE 0 END AS is_primary,
    GROUP_CONCAT(s.column_name ORDER BY s.seq_in_index) AS columns
FROM information_schema.statistics s
WHERE s.table_schema = COALESCE(:schema_name, DATABASE())
GROUP BY s.index_name, s.table_name, s.non_unique
ORDER BY s.table_name, s.index_name;
