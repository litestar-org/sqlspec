-- name: indexes_by_table
-- dialect: sqlite
PRAGMA index_list({table_name});

-- name: index_columns_by_index
-- dialect: sqlite
PRAGMA index_info({index_name});
