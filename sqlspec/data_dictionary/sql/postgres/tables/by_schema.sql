-- name: by_schema
-- dialect: postgres
WITH RECURSIVE dependency_tree AS (
    SELECT
        c.oid,
        n.nspname::text AS schema_name,
        c.relname::text AS table_name,
        0 AS dependency_level,
        ARRAY[c.oid] AS path
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :schema_name
      AND (:table_name::text IS NULL OR c.relname = :table_name)
      AND c.relkind IN ('r', 'p')
      AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_constraint fk
          WHERE fk.conrelid = c.oid
            AND fk.contype = 'f'
      )

    UNION ALL

    SELECT
        child.oid,
        child_ns.nspname::text AS schema_name,
        child.relname::text AS table_name,
        parent.dependency_level + 1,
        parent.path || child.oid
    FROM pg_catalog.pg_constraint fk
    JOIN pg_catalog.pg_class child ON child.oid = fk.conrelid
    JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace
    JOIN dependency_tree parent ON parent.oid = fk.confrelid
    WHERE fk.contype = 'f'
      AND child_ns.nspname = :schema_name
      AND (:table_name::text IS NULL OR child.relname = :table_name)
      AND NOT child.oid = ANY(parent.path)
)
SELECT DISTINCT ON (t.oid)
    current_database()::text AS table_catalog,
    n.nspname::text AS table_schema,
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    CASE c.relkind WHEN 'p' THEN 'PARTITIONED TABLE' ELSE 'BASE TABLE' END AS table_type,
    COALESCE(t.dependency_level, 0) AS dependency_level,
    COALESCE(t.dependency_level, 0) AS level,
    c.reltuples::bigint AS estimated_row_count,
    c.relispartition AS is_partition,
    r.rolname::text AS owner_name,
    pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
LEFT JOIN dependency_tree t ON t.oid = c.oid
WHERE n.nspname = :schema_name
  AND (:table_name::text IS NULL OR c.relname = :table_name)
  AND c.relkind IN ('r', 'p')
ORDER BY t.oid, dependency_level, c.relname;
