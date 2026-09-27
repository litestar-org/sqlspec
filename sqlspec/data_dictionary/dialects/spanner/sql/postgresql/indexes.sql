-- name: by_schema
-- dialect: spanner
SELECT
    i.table_catalog AS table_catalog,
    i.table_schema AS schema_name,
    i.table_name AS table_name,
    i.index_name AS index_name,
    i.index_type AS index_type,
    i.is_unique AS is_unique,
    ARRAY_AGG(ic.column_name ORDER BY ic.ordinal_position) AS columns
FROM information_schema.indexes AS i
LEFT JOIN information_schema.index_columns AS ic
  ON i.table_catalog = ic.table_catalog
  AND i.table_schema = ic.table_schema
  AND i.table_name = ic.table_name
  AND i.index_name = ic.index_name
WHERE (:schema_name::text IS NULL OR i.table_schema = :schema_name)
  AND (:table_name::text IS NULL OR i.table_name = :table_name)
GROUP BY
    i.table_catalog,
    i.table_schema,
    i.table_name,
    i.index_name,
    i.index_type,
    i.is_unique
ORDER BY i.table_schema, i.table_name, i.index_name;

-- name: by_table
-- dialect: spanner
SELECT
    i.table_catalog AS table_catalog,
    i.table_schema AS schema_name,
    i.table_name AS table_name,
    i.index_name AS index_name,
    i.index_type AS index_type,
    i.is_unique AS is_unique,
    ARRAY_AGG(ic.column_name ORDER BY ic.ordinal_position) AS columns
FROM information_schema.indexes AS i
LEFT JOIN information_schema.index_columns AS ic
  ON i.table_catalog = ic.table_catalog
  AND i.table_schema = ic.table_schema
  AND i.table_name = ic.table_name
  AND i.index_name = ic.index_name
WHERE (:schema_name::text IS NULL OR i.table_schema = :schema_name)
  AND (:table_name::text IS NULL OR i.table_name = :table_name)
GROUP BY
    i.table_catalog,
    i.table_schema,
    i.table_name,
    i.index_name,
    i.index_type,
    i.is_unique
ORDER BY i.table_schema, i.table_name, i.index_name;
