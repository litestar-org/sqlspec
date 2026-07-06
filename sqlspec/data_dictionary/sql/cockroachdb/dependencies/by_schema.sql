-- name: by_schema
-- dialect: cockroachdb
SELECT
    tc.table_schema::text AS schema_name,
    tc.table_name::text AS object_name,
    'table'::text AS object_type,
    ccu.table_schema::text AS referenced_schema,
    ccu.table_name::text AS referenced_object,
    'table'::text AS referenced_object_type,
    'foreign_key'::text AS dependency_type,
    tc.constraint_name::text AS constraint_name
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_catalog = tc.constraint_catalog
 AND ccu.constraint_schema = tc.constraint_schema
 AND ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = :schema_name
  AND (:object_name::text IS NULL OR tc.table_name = :object_name)
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
