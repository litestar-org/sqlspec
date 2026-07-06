-- name: by_dataset
-- dialect: bigquery
SELECT
    v.table_catalog,
    v.table_schema,
    v.table_name,
    v.view_definition,
    v.check_option,
    v.use_standard_sql
FROM {views_table} AS v
WHERE (:schema_name IS NULL OR v.table_schema = :schema_name)
  AND (:view_name IS NULL OR v.table_name = :view_name)
ORDER BY v.table_schema, v.table_name;
