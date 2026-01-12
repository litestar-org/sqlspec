-- name: foreign_keys_by_table
-- dialect: cockroachdb
SELECT
    tc.constraint_name::text AS constraint_name,
    kcu.column_name::text AS column_name,
    ccu.table_name::text AS referenced_table,
    ccu.column_name::text AS referenced_column,
    tc.table_name::text AS table_name,
    tc.table_schema::text AS table_schema
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name
  AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = :schema_name
  AND tc.table_name = :table_name
ORDER BY tc.constraint_name, kcu.column_name;

-- name: foreign_keys_by_schema
-- dialect: cockroachdb
SELECT
    tc.constraint_name::text AS constraint_name,
    kcu.column_name::text AS column_name,
    ccu.table_name::text AS referenced_table,
    ccu.column_name::text AS referenced_column,
    tc.table_name::text AS table_name,
    tc.table_schema::text AS table_schema
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_name = tc.constraint_name
  AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_schema = :schema_name
ORDER BY tc.table_name, tc.constraint_name, kcu.column_name;
