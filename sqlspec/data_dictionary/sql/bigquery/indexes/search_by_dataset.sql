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
