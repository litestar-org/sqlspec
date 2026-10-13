-- name: by_dataset
-- dialect: bigquery
SELECT
    tc.constraint_catalog,
    tc.constraint_schema,
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name,
    kcu.ordinal_position,
    COALESCE(pk_kcu.table_name, ccu.table_name) AS referenced_table_name,
    COALESCE(pk_kcu.column_name, ccu.column_name) AS referenced_column_name,
    rc.match_option,
    rc.update_rule,
    rc.delete_rule
FROM {table_constraints_table} AS tc
LEFT JOIN {key_column_usage_table} AS kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
  AND tc.constraint_schema = kcu.constraint_schema
  AND tc.constraint_name = kcu.constraint_name
LEFT JOIN {referential_constraints_table} AS rc
  ON tc.constraint_catalog = rc.constraint_catalog
  AND tc.constraint_schema = rc.constraint_schema
  AND tc.constraint_name = rc.constraint_name
LEFT JOIN {key_column_usage_table} AS pk_kcu
  ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog
  AND rc.unique_constraint_schema = pk_kcu.constraint_schema
  AND rc.unique_constraint_name = pk_kcu.constraint_name
  AND COALESCE(kcu.position_in_unique_constraint, kcu.ordinal_position) = pk_kcu.ordinal_position
LEFT JOIN {constraint_column_usage_table} AS ccu
  ON tc.constraint_catalog = ccu.constraint_catalog
  AND tc.constraint_schema = ccu.constraint_schema
  AND tc.constraint_name = ccu.constraint_name
  AND ccu.column_name = kcu.column_name
WHERE (:schema_name IS NULL OR tc.constraint_schema = :schema_name)
  AND (:table_name IS NULL OR tc.table_name = :table_name)
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;
