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
