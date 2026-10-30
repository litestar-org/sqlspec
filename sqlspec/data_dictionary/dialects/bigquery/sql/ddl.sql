-- name: by_object
-- dialect: bigquery
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type,
    table_name AS object_name,
    CASE table_type
        WHEN 'VIEW' THEN 'view'
        WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
        ELSE 'table'
    END AS object_type,
    ddl,
    'native' AS fidelity
FROM {tables_table}
WHERE (:schema_name IS NULL OR table_schema = :schema_name)
  AND (:table_name IS NULL OR table_name = :table_name)
ORDER BY table_schema, table_name;

-- name: by_schema
-- dialect: bigquery
SELECT
    table_catalog AS catalog_name,
    table_schema AS schema_name,
    table_name AS object_name,
    CASE table_type
        WHEN 'VIEW' THEN 'view'
        WHEN 'MATERIALIZED VIEW' THEN 'materialized_view'
        ELSE 'table'
    END AS object_type,
    ddl,
    'native' AS fidelity
FROM {schema_prefix}INFORMATION_SCHEMA.TABLES
WHERE (:schema_name IS NULL OR table_schema = :schema_name)
  AND ddl IS NOT NULL
UNION ALL
SELECT
    routine_catalog AS catalog_name,
    routine_schema AS schema_name,
    routine_name AS object_name,
    CASE UPPER(COALESCE(routine_type, 'FUNCTION'))
        WHEN 'PROCEDURE' THEN 'procedure'
        ELSE 'function'
    END AS object_type,
    ddl,
    'native' AS fidelity
FROM {schema_prefix}INFORMATION_SCHEMA.ROUTINES
WHERE (:schema_name IS NULL OR routine_schema = :schema_name)
  AND ddl IS NOT NULL
ORDER BY schema_name, object_type, object_name;
