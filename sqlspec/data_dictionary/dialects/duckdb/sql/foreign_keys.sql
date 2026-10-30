-- name: by_table
-- dialect: duckdb
SELECT
    kcu.table_name,
    kcu.column_name,
    pk_kcu.table_name AS referenced_table_name,
    pk_kcu.column_name AS referenced_column_name,
    kcu.constraint_name,
    kcu.table_schema,
    pk_kcu.table_schema AS referenced_table_schema,
    rc.delete_rule,
    rc.update_rule
FROM information_schema.key_column_usage kcu
JOIN information_schema.referential_constraints rc
  ON kcu.constraint_name = rc.constraint_name
  AND kcu.constraint_schema = rc.constraint_schema
JOIN information_schema.key_column_usage pk_kcu
  ON rc.unique_constraint_name = pk_kcu.constraint_name
  AND rc.unique_constraint_schema = pk_kcu.constraint_schema
  AND kcu.ordinal_position = pk_kcu.ordinal_position
WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
  AND kcu.table_name = :table_name
ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position;

-- name: by_schema
-- dialect: duckdb
SELECT
    kcu.table_name,
    kcu.column_name,
    pk_kcu.table_name AS referenced_table_name,
    pk_kcu.column_name AS referenced_column_name,
    kcu.constraint_name,
    kcu.table_schema,
    pk_kcu.table_schema AS referenced_table_schema,
    rc.delete_rule,
    rc.update_rule
FROM information_schema.key_column_usage kcu
JOIN information_schema.referential_constraints rc
  ON kcu.constraint_name = rc.constraint_name
  AND kcu.constraint_schema = rc.constraint_schema
JOIN information_schema.key_column_usage pk_kcu
  ON rc.unique_constraint_name = pk_kcu.constraint_name
  AND rc.unique_constraint_schema = pk_kcu.constraint_schema
  AND kcu.ordinal_position = pk_kcu.ordinal_position
WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
