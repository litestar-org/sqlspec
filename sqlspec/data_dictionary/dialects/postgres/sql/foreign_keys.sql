-- name: foreign_keys_by_table
-- dialect: postgres
SELECT
    kcu.table_name,
    kcu.column_name,
    ccu.table_name AS referenced_table_name,
    ccu.column_name AS referenced_column_name,
    tc.constraint_name,
    tc.table_schema,
    ccu.table_schema AS referenced_table_schema
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
  AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND (:schema_name::text IS NULL OR tc.table_schema = :schema_name)
  AND (:table_name::text IS NULL OR tc.table_name = :table_name);

-- name: foreign_keys_by_schema
-- dialect: postgres
SELECT
    kcu.table_name,
    kcu.column_name,
    ccu.table_name AS referenced_table_name,
    ccu.column_name AS referenced_column_name,
    tc.constraint_name,
    tc.table_schema,
    ccu.table_schema AS referenced_table_schema
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
  AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND (:schema_name::text IS NULL OR tc.table_schema = :schema_name);
