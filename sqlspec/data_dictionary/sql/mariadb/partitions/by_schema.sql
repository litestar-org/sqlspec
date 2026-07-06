-- name: by_schema
-- dialect: mysql
SELECT
    table_schema AS schema_name,
    table_name,
    partition_name,
    subpartition_name,
    partition_ordinal_position,
    subpartition_ordinal_position,
    partition_method,
    subpartition_method,
    partition_expression,
    subpartition_expression,
    partition_description,
    table_rows,
    avg_row_length,
    data_length,
    index_length,
    create_time,
    update_time,
    check_time,
    check_sum,
    partition_comment
FROM information_schema.partitions
WHERE table_schema = COALESCE(:schema_name, DATABASE())
  AND partition_name IS NOT NULL
ORDER BY table_schema, table_name, partition_ordinal_position, subpartition_ordinal_position;
