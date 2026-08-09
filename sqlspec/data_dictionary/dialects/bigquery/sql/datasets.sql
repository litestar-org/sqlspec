-- name: list
-- dialect: bigquery
SELECT
    s.catalog_name AS project_id,
    s.schema_name AS dataset_name,
    s.location,
    ARRAY_AGG(STRUCT(so.option_name, so.option_type, so.option_value) ORDER BY so.option_name) AS options
FROM {schemata_table} AS s
LEFT JOIN {schemata_options_table} AS so
  ON s.catalog_name = so.catalog_name
  AND s.schema_name = so.schema_name
WHERE (:dataset_name IS NULL OR s.schema_name = :dataset_name)
GROUP BY s.catalog_name, s.schema_name, s.location
ORDER BY s.catalog_name, s.schema_name;
