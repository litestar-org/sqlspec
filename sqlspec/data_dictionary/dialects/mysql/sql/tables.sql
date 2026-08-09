-- name: by_schema
-- dialect: mysql
WITH RECURSIVE dependency_tree AS (
    SELECT
        table_name,
        0 AS level,
        CAST(table_name AS CHAR(4000)) AS path
    FROM information_schema.tables AS root_table
    WHERE root_table.table_type = 'BASE TABLE'
      AND root_table.table_schema = COALESCE(:schema_name, DATABASE())
      AND NOT EXISTS (
          SELECT 1
          FROM information_schema.key_column_usage AS root_key
          WHERE root_key.table_name = root_table.table_name
            AND root_key.table_schema = root_table.table_schema
            AND root_key.referenced_table_name IS NOT NULL
      )

    UNION ALL

    SELECT
        child_key.table_name,
        dependency_tree.level + 1,
        CONCAT(dependency_tree.path, ',', child_key.table_name)
    FROM information_schema.key_column_usage AS child_key
    JOIN dependency_tree ON child_key.referenced_table_name = dependency_tree.table_name
    WHERE child_key.table_schema = COALESCE(:schema_name, DATABASE())
      AND child_key.referenced_table_name IS NOT NULL
      AND NOT FIND_IN_SET(child_key.table_name, dependency_tree.path)
), dependency_levels AS (
    SELECT table_name, MIN(level) AS dependency_level
    FROM dependency_tree
    GROUP BY table_name
)
SELECT
    catalog_table.table_schema AS schema_name,
    CAST(catalog_table.table_name AS CHAR) AS table_name,
    catalog_table.table_type AS table_type,
    catalog_table.engine AS engine,
    catalog_table.version AS version,
    catalog_table.row_format AS row_format,
    catalog_table.table_rows AS table_rows,
    catalog_table.avg_row_length AS avg_row_length,
    catalog_table.data_length AS data_length,
    catalog_table.max_data_length AS max_data_length,
    catalog_table.index_length AS index_length,
    catalog_table.data_free AS data_free,
    catalog_table.auto_increment AS auto_increment,
    catalog_table.create_time AS create_time,
    catalog_table.update_time AS update_time,
    catalog_table.check_time AS check_time,
    catalog_table.table_collation AS table_collation,
    catalog_table.checksum AS checksum,
    catalog_table.create_options AS create_options,
    catalog_table.table_comment AS table_comment,
    COALESCE(dependency_levels.dependency_level, 0) AS dependency_level,
    COALESCE(dependency_levels.dependency_level, 0) AS level
FROM information_schema.tables AS catalog_table
LEFT JOIN dependency_levels ON dependency_levels.table_name = catalog_table.table_name
WHERE catalog_table.table_schema = COALESCE(:schema_name, DATABASE())
  AND (:table_name IS NULL OR catalog_table.table_name = :table_name)
ORDER BY dependency_level, catalog_table.table_name;
