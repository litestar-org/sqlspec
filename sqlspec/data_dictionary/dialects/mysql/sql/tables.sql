-- name: by_schema
-- dialect: mysql
SELECT
    table_schema AS schema_name,
    table_name,
    table_type,
    engine,
    version,
    row_format,
    table_rows,
    avg_row_length,
    data_length,
    max_data_length,
    index_length,
    data_free,
    auto_increment,
    create_time,
    update_time,
    check_time,
    table_collation,
    checksum,
    create_options,
    table_comment
FROM information_schema.tables
WHERE table_schema = COALESCE(:schema_name, DATABASE())
  AND (:table_name IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name;

-- name: tables_by_schema
-- dialect: mysql
WITH RECURSIVE dependency_tree AS (
    SELECT
        table_name,
        0 AS level,
        CAST(table_name AS CHAR(4000)) AS path
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema = COALESCE(:schema_name, DATABASE())
      AND NOT EXISTS (
          SELECT 1
          FROM information_schema.key_column_usage kcu
          WHERE kcu.table_name = t.table_name
            AND kcu.table_schema = t.table_schema
            AND kcu.referenced_table_name IS NOT NULL
      )

    UNION ALL

    SELECT
        kcu.table_name,
        dt.level + 1,
        CONCAT(dt.path, ',', kcu.table_name)
    FROM information_schema.key_column_usage kcu
    JOIN dependency_tree dt ON kcu.referenced_table_name = dt.table_name
    WHERE kcu.table_schema = COALESCE(:schema_name, DATABASE())
      AND kcu.referenced_table_name IS NOT NULL
      AND NOT FIND_IN_SET(kcu.table_name, dt.path)
)
SELECT
    table_name,
    MIN(level) AS level
FROM dependency_tree
GROUP BY table_name
ORDER BY level, table_name;
