-- name: by_schema
-- dialect: cockroachdb
SELECT
    tc.constraint_catalog::text AS constraint_catalog,
    tc.constraint_schema::text AS constraint_schema,
    tc.table_schema::text AS schema_name,
    tc.table_name::text AS table_name,
    tc.constraint_name::text AS constraint_name,
    tc.constraint_type::text AS constraint_type,
    tc.is_deferrable::text AS is_deferrable,
    tc.initially_deferred::text AS initially_deferred,
    ARRAY(
        SELECT kcu.column_name::text
        FROM information_schema.key_column_usage kcu
        WHERE kcu.constraint_catalog = tc.constraint_catalog
          AND kcu.constraint_schema = tc.constraint_schema
          AND kcu.constraint_name = tc.constraint_name
          AND kcu.table_schema = tc.table_schema
          AND kcu.table_name = tc.table_name
        ORDER BY kcu.ordinal_position
    )::text[] AS columns,
    rc.unique_constraint_schema::text AS referenced_schema,
    ccu.table_name::text AS referenced_table,
    ARRAY(
        SELECT ccu_inner.column_name::text
        FROM information_schema.constraint_column_usage ccu_inner
        WHERE ccu_inner.constraint_catalog = tc.constraint_catalog
          AND ccu_inner.constraint_schema = tc.constraint_schema
          AND ccu_inner.constraint_name = tc.constraint_name
        ORDER BY ccu_inner.column_name
    )::text[] AS referenced_columns
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.referential_constraints rc
  ON rc.constraint_catalog = tc.constraint_catalog
 AND rc.constraint_schema = tc.constraint_schema
 AND rc.constraint_name = tc.constraint_name
LEFT JOIN information_schema.constraint_column_usage ccu
  ON ccu.constraint_catalog = tc.constraint_catalog
 AND ccu.constraint_schema = tc.constraint_schema
 AND ccu.constraint_name = tc.constraint_name
WHERE tc.table_schema = :schema_name
  AND (:table_name::text IS NULL OR tc.table_name = :table_name)
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
