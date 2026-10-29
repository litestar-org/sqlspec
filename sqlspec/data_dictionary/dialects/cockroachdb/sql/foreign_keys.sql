-- name: by_table
-- dialect: cockroachdb
SELECT
    tc.constraint_name::text AS constraint_name,
    kcu.column_name::text AS column_name,
    pk_kcu.table_name::text AS referenced_table,
    pk_kcu.column_name::text AS referenced_column,
    tc.table_name::text AS table_name,
    tc.table_schema::text AS table_schema,
    pk_kcu.table_schema::text AS referenced_schema,
    pk_kcu.table_schema::text AS referenced_table_schema,
    rc.delete_rule::text AS on_delete,
    rc.update_rule::text AS on_update
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
  AND tc.constraint_schema = kcu.constraint_schema
  AND tc.constraint_name = kcu.constraint_name
JOIN information_schema.referential_constraints rc
  ON tc.constraint_catalog = rc.constraint_catalog
  AND tc.constraint_schema = rc.constraint_schema
  AND tc.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage pk_kcu
  ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog
  AND rc.unique_constraint_schema = pk_kcu.constraint_schema
  AND rc.unique_constraint_name = pk_kcu.constraint_name
  AND COALESCE(kcu.position_in_unique_constraint, kcu.ordinal_position) = pk_kcu.ordinal_position
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = :schema_name
  AND tc.table_name = :table_name
ORDER BY tc.constraint_name, kcu.ordinal_position;

-- name: by_schema
-- dialect: cockroachdb
SELECT
    tc.constraint_name::text AS constraint_name,
    kcu.column_name::text AS column_name,
    pk_kcu.table_name::text AS referenced_table,
    pk_kcu.column_name::text AS referenced_column,
    tc.table_name::text AS table_name,
    tc.table_schema::text AS table_schema,
    pk_kcu.table_schema::text AS referenced_schema,
    pk_kcu.table_schema::text AS referenced_table_schema,
    rc.delete_rule::text AS on_delete,
    rc.update_rule::text AS on_update
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
  AND tc.constraint_schema = kcu.constraint_schema
  AND tc.constraint_name = kcu.constraint_name
JOIN information_schema.referential_constraints rc
  ON tc.constraint_catalog = rc.constraint_catalog
  AND tc.constraint_schema = rc.constraint_schema
  AND tc.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage pk_kcu
  ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog
  AND rc.unique_constraint_schema = pk_kcu.constraint_schema
  AND rc.unique_constraint_name = pk_kcu.constraint_name
  AND COALESCE(kcu.position_in_unique_constraint, kcu.ordinal_position) = pk_kcu.ordinal_position
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = :schema_name
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;
