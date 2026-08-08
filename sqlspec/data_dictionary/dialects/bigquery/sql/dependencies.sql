-- name: hints_by_dataset
-- dialect: bigquery
SELECT
    table_catalog,
    table_schema,
    table_name AS object_name,
    table_type AS object_type,
    ddl AS definition_text
FROM {tables_table}
WHERE (:schema_name IS NULL OR table_schema = :schema_name)
  AND ddl IS NOT NULL
UNION ALL
SELECT
    routine_catalog AS table_catalog,
    routine_schema AS table_schema,
    routine_name AS object_name,
    routine_type AS object_type,
    routine_definition AS definition_text
FROM {routines_table}
WHERE (:schema_name IS NULL OR routine_schema = :schema_name)
  AND routine_definition IS NOT NULL
ORDER BY table_schema, object_name;
