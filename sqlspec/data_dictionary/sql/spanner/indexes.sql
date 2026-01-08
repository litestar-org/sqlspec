-- name: indexes_by_table
-- dialect: spanner
SELECT
    i.index_name,
    i.table_name,
    i.is_unique AS is_unique,
    i.is_primary_key AS is_primary,
    ARRAY_AGG(ic.column_name ORDER BY ic.ordinal_position) AS columns
FROM information_schema.indexes i
JOIN information_schema.index_columns ic
  ON i.index_name = ic.index_name
  AND i.table_name = ic.table_name
WHERE i.table_name = :table_name
  AND (:schema_name IS NULL OR i.table_schema = :schema_name)
GROUP BY i.index_name, i.table_name, i.is_unique, i.is_primary_key;

-- name: indexes_by_schema
-- dialect: spanner
SELECT
    i.index_name,
    i.table_name,
    i.is_unique AS is_unique,
    i.is_primary_key AS is_primary,
    ARRAY_AGG(ic.column_name ORDER BY ic.ordinal_position) AS columns
FROM information_schema.indexes i
JOIN information_schema.index_columns ic
  ON i.index_name = ic.index_name
  AND i.table_name = ic.table_name
WHERE (:schema_name IS NULL OR i.table_schema = :schema_name)
GROUP BY i.index_name, i.table_name, i.is_unique, i.is_primary_key;
