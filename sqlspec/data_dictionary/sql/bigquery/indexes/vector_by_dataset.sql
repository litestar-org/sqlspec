-- name: vector_by_dataset
-- dialect: bigquery
SELECT
    vi.index_catalog AS schema_name,
    vi.table_name,
    vi.index_name,
    ARRAY_AGG(vic.index_field_path ORDER BY vic.index_field_path) AS columns,
    FALSE AS is_unique,
    FALSE AS is_primary
FROM {vector_indexes_table} AS vi
LEFT JOIN {vector_index_columns_table} AS vic
  ON vi.index_catalog = vic.index_catalog
  AND vi.index_schema = vic.index_schema
  AND vi.table_name = vic.table_name
  AND vi.index_name = vic.index_name
WHERE (:schema_name IS NULL OR vi.index_schema = :schema_name)
  AND (:table_name IS NULL OR vi.table_name = :table_name)
GROUP BY vi.index_catalog, vi.table_name, vi.index_name
ORDER BY vi.table_name, vi.index_name;
