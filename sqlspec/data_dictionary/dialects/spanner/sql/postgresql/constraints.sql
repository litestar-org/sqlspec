-- name: by_schema
-- dialect: spanner
SELECT
    tc.constraint_catalog,
    tc.constraint_schema,
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name,
    kcu.ordinal_position,
    rc.unique_constraint_name,
    rc.match_option,
    rc.update_rule,
    rc.delete_rule,
    cc.check_clause
FROM information_schema.table_constraints AS tc
LEFT JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
  AND tc.constraint_schema = kcu.constraint_schema
  AND tc.constraint_name = kcu.constraint_name
LEFT JOIN information_schema.referential_constraints AS rc
  ON tc.constraint_catalog = rc.constraint_catalog
  AND tc.constraint_schema = rc.constraint_schema
  AND tc.constraint_name = rc.constraint_name
LEFT JOIN information_schema.check_constraints AS cc
  ON tc.constraint_catalog = cc.constraint_catalog
  AND tc.constraint_schema = cc.constraint_schema
  AND tc.constraint_name = cc.constraint_name
WHERE (:schema_name::text IS NULL OR tc.constraint_schema = :schema_name)
  AND (:table_name::text IS NULL OR tc.table_name = :table_name)
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;

-- name: by_table
-- dialect: spanner
SELECT
    tc.constraint_catalog,
    tc.constraint_schema,
    tc.constraint_name,
    tc.table_name,
    tc.constraint_type,
    kcu.column_name,
    kcu.ordinal_position,
    rc.unique_constraint_name,
    rc.match_option,
    rc.update_rule,
    rc.delete_rule,
    cc.check_clause
FROM information_schema.table_constraints AS tc
LEFT JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_catalog = kcu.constraint_catalog
  AND tc.constraint_schema = kcu.constraint_schema
  AND tc.constraint_name = kcu.constraint_name
LEFT JOIN information_schema.referential_constraints AS rc
  ON tc.constraint_catalog = rc.constraint_catalog
  AND tc.constraint_schema = rc.constraint_schema
  AND tc.constraint_name = rc.constraint_name
LEFT JOIN information_schema.check_constraints AS cc
  ON tc.constraint_catalog = cc.constraint_catalog
  AND tc.constraint_schema = cc.constraint_schema
  AND tc.constraint_name = cc.constraint_name
WHERE (:schema_name::text IS NULL OR tc.constraint_schema = :schema_name)
  AND (:table_name::text IS NULL OR tc.table_name = :table_name)
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;
