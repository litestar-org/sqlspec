-- name: search_by_dataset
-- dialect: bigquery
SELECT
    si.index_catalog AS schema_name,
    si.table_name,
    si.index_name,
    ARRAY_AGG(sic.index_field_path ORDER BY sic.index_field_path) AS columns,
    FALSE AS is_unique,
    FALSE AS is_primary
FROM {search_indexes_table} AS si
LEFT JOIN {search_index_columns_table} AS sic
  ON si.index_catalog = sic.index_catalog
  AND si.index_schema = sic.index_schema
  AND si.table_name = sic.table_name
  AND si.index_name = sic.index_name
WHERE (:schema_name IS NULL OR si.index_schema = :schema_name)
  AND (:table_name IS NULL OR si.table_name = :table_name)
GROUP BY si.index_catalog, si.table_name, si.index_name
ORDER BY si.table_name, si.index_name;

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

-- name: indexes_by_table
-- dialect: bigquery
SELECT
    NULL AS index_name,
    NULL AS table_name,
    NULL AS is_unique,
    NULL AS is_primary,
    NULL AS columns
WHERE FALSE;

-- name: indexes_by_schema
-- dialect: bigquery
SELECT
    NULL AS index_name,
    NULL AS table_name,
    NULL AS is_unique,
    NULL AS is_primary,
    NULL AS columns
WHERE FALSE;
