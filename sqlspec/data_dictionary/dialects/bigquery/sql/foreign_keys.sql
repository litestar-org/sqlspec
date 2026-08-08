-- name: foreign_keys_by_table
-- dialect: bigquery
SELECT
    kcu.table_name,
    kcu.column_name,
    pk_kcu.table_name AS referenced_table_name,
    pk_kcu.column_name AS referenced_column_name,
    kcu.constraint_name,
    kcu.table_schema,
    pk_kcu.table_schema AS referenced_table_schema
FROM {kcu_table} kcu
JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
JOIN {kcu_table} pk_kcu
  ON rc.unique_constraint_name = pk_kcu.constraint_name
  AND kcu.ordinal_position = pk_kcu.ordinal_position
WHERE kcu.table_name = :table_name
  AND (:schema_name IS NULL OR kcu.table_schema = :schema_name);

-- name: foreign_keys_by_schema
-- dialect: bigquery
SELECT
    kcu.table_name,
    kcu.column_name,
    pk_kcu.table_name AS referenced_table_name,
    pk_kcu.column_name AS referenced_column_name,
    kcu.constraint_name,
    kcu.table_schema,
    pk_kcu.table_schema AS referenced_table_schema
FROM {kcu_table} kcu
JOIN {rc_table} rc ON kcu.constraint_name = rc.constraint_name
JOIN {kcu_table} pk_kcu
  ON rc.unique_constraint_name = pk_kcu.constraint_name
  AND kcu.ordinal_position = pk_kcu.ordinal_position
WHERE (:schema_name IS NULL OR kcu.table_schema = :schema_name);
