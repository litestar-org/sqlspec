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
