-- name: by_schema
-- dialect: mysql
SELECT
    tc.constraint_schema AS schema_name,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    kcu.ordinal_position,
    kcu.referenced_table_schema,
    kcu.referenced_table_name,
    kcu.referenced_column_name,
    rc.update_rule,
    rc.delete_rule,
    cc.check_clause
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_schema = tc.constraint_schema
 AND kcu.constraint_name = tc.constraint_name
 AND kcu.table_schema = tc.table_schema
 AND kcu.table_name = tc.table_name
LEFT JOIN information_schema.referential_constraints rc
  ON rc.constraint_schema = tc.constraint_schema
 AND rc.constraint_name = tc.constraint_name
LEFT JOIN information_schema.check_constraints cc
  ON cc.constraint_schema = tc.constraint_schema
 AND cc.constraint_name = tc.constraint_name
WHERE tc.table_schema = COALESCE(:schema_name, DATABASE())
  AND (:table_name IS NULL OR tc.table_name = :table_name)
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;
