-- name: by_table
-- dialect: spanner
WITH pk_cols AS (
    SELECT
        ic.table_schema,
        ic.table_name,
        ic.column_name
    FROM information_schema.index_columns AS ic
    WHERE ic.index_name = 'PRIMARY_KEY'
      AND ic.table_name = :table_name
      AND (CAST(:schema_name AS STRING) IS NULL OR ic.table_schema = :schema_name)
)
SELECT
    c.column_name,
    c.spanner_type AS data_type,
    c.is_nullable,
    c.column_default,
    CASE WHEN pk_cols.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary,
    CASE WHEN c.is_identity = 'YES' THEN TRUE ELSE FALSE END AS is_identity,
    CASE WHEN c.is_generated = 'ALWAYS' OR c.generation_expression IS NOT NULL THEN TRUE ELSE FALSE END AS is_generated,
    c.generation_expression
FROM information_schema.columns AS c
LEFT JOIN pk_cols
    ON c.table_schema = pk_cols.table_schema
    AND c.table_name = pk_cols.table_name
    AND c.column_name = pk_cols.column_name
WHERE c.table_name = :table_name
  AND (CAST(:schema_name AS STRING) IS NULL OR c.table_schema = :schema_name)
ORDER BY c.ordinal_position;

-- name: by_schema
-- dialect: spanner
WITH pk_cols AS (
    SELECT
        ic.table_schema,
        ic.table_name,
        ic.column_name
    FROM information_schema.index_columns AS ic
    WHERE ic.index_name = 'PRIMARY_KEY'
      AND (CAST(:schema_name AS STRING) IS NULL OR ic.table_schema = :schema_name)
)
SELECT
    c.table_name,
    c.column_name,
    c.spanner_type AS data_type,
    c.is_nullable,
    c.column_default,
    CASE WHEN pk_cols.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary,
    CASE WHEN c.is_identity = 'YES' THEN TRUE ELSE FALSE END AS is_identity,
    CASE WHEN c.is_generated = 'ALWAYS' OR c.generation_expression IS NOT NULL THEN TRUE ELSE FALSE END AS is_generated,
    c.generation_expression
FROM information_schema.columns AS c
LEFT JOIN pk_cols
    ON c.table_schema = pk_cols.table_schema
    AND c.table_name = pk_cols.table_name
    AND c.column_name = pk_cols.column_name
WHERE (CAST(:schema_name AS STRING) IS NULL OR c.table_schema = :schema_name)
ORDER BY c.table_name, c.ordinal_position;
