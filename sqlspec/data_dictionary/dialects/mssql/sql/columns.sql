-- name: by_schema
-- dialect: mssql
WITH pk_cols AS (
    SELECT DISTINCT
        idx_col.object_id,
        idx_col.column_id
    FROM sys.indexes AS idx
    INNER JOIN sys.index_columns AS idx_col
      ON idx.object_id = idx_col.object_id
     AND idx.index_id = idx_col.index_id
    WHERE idx.is_primary_key = 1
),
unique_cols AS (
    SELECT DISTINCT
        idx_col.object_id,
        idx_col.column_id
    FROM sys.indexes AS idx
    INNER JOIN sys.index_columns AS idx_col
      ON idx.object_id = idx_col.object_id
     AND idx.index_id = idx_col.index_id
    WHERE idx.is_unique = 1
)
SELECT
    s.name AS schema_name,
    tab.name AS table_name,
    c.column_id AS ordinal_position,
    c.name AS column_name,
    typ.name AS data_type,
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
    OBJECT_DEFINITION(c.default_object_id) AS column_default,
    c.max_length,
    c.precision AS numeric_precision,
    c.scale AS numeric_scale,
    CAST(CASE WHEN pk_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_primary,
    CAST(CASE WHEN unique_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_unique,
    c.collation_name,
    CAST(c.is_sparse AS BIT) AS is_sparse,
    CAST(c.is_column_set AS BIT) AS is_column_set,
    CAST(c.is_hidden AS BIT) AS is_hidden,
    CAST(c.is_masked AS BIT) AS is_masked,
    c.encryption_type_desc,
    c.encryption_algorithm_name,
    c.generated_always_type_desc,
    CAST(CASE WHEN cc.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_generated,
    cc.definition AS generation_expression,
    CAST(cc.is_persisted AS BIT) AS is_persisted_computed,
    CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
    CASE WHEN ic.object_id IS NOT NULL THEN 'ALWAYS' ELSE NULL END AS identity_generation,
    CONVERT(BIGINT, ic.seed_value) AS identity_seed,
    CONVERT(BIGINT, ic.increment_value) AS identity_increment
FROM sys.columns AS c
INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN pk_cols ON pk_cols.object_id = tab.object_id AND pk_cols.column_id = c.column_id
LEFT JOIN unique_cols ON unique_cols.object_id = tab.object_id AND unique_cols.column_id = c.column_id
WHERE (:schema_name IS NULL OR s.name = :schema_name)
ORDER BY s.name, tab.name, c.column_id;

-- name: by_table
-- dialect: mssql
WITH pk_cols AS (
    SELECT DISTINCT
        idx_col.object_id,
        idx_col.column_id
    FROM sys.indexes AS idx
    INNER JOIN sys.index_columns AS idx_col
      ON idx.object_id = idx_col.object_id
     AND idx.index_id = idx_col.index_id
    WHERE idx.is_primary_key = 1
),
unique_cols AS (
    SELECT DISTINCT
        idx_col.object_id,
        idx_col.column_id
    FROM sys.indexes AS idx
    INNER JOIN sys.index_columns AS idx_col
      ON idx.object_id = idx_col.object_id
     AND idx.index_id = idx_col.index_id
    WHERE idx.is_unique = 1
)
SELECT
    s.name AS schema_name,
    tab.name AS table_name,
    c.column_id AS ordinal_position,
    c.name AS column_name,
    typ.name AS data_type,
    CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
    OBJECT_DEFINITION(c.default_object_id) AS column_default,
    c.max_length,
    c.precision AS numeric_precision,
    c.scale AS numeric_scale,
    CAST(CASE WHEN pk_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_primary,
    CAST(CASE WHEN unique_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_unique,
    c.collation_name,
    CAST(c.is_sparse AS BIT) AS is_sparse,
    CAST(c.is_column_set AS BIT) AS is_column_set,
    CAST(c.is_hidden AS BIT) AS is_hidden,
    CAST(c.is_masked AS BIT) AS is_masked,
    c.encryption_type_desc,
    c.encryption_algorithm_name,
    c.generated_always_type_desc,
    CAST(CASE WHEN cc.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_generated,
    cc.definition AS generation_expression,
    CAST(cc.is_persisted AS BIT) AS is_persisted_computed,
    CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
    CASE WHEN ic.object_id IS NOT NULL THEN 'ALWAYS' ELSE NULL END AS identity_generation,
    CONVERT(BIGINT, ic.seed_value) AS identity_seed,
    CONVERT(BIGINT, ic.increment_value) AS identity_increment
FROM sys.columns AS c
INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN pk_cols ON pk_cols.object_id = tab.object_id AND pk_cols.column_id = c.column_id
LEFT JOIN unique_cols ON unique_cols.object_id = tab.object_id AND unique_cols.column_id = c.column_id
WHERE s.name = COALESCE(:schema_name, SCHEMA_NAME())
  AND tab.name = :table_name
ORDER BY c.column_id;
