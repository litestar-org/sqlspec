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
