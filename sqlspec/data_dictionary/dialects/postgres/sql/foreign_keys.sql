-- name: by_table
-- dialect: postgres
SELECT
    c.relname::text AS table_name,
    att.attname::text AS column_name,
    ref.relname::text AS referenced_table_name,
    ref_att.attname::text AS referenced_column_name,
    con.conname::text AS constraint_name,
    n.nspname::text AS table_schema,
    ref_ns.nspname::text AS referenced_table_schema,
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_delete,
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_update
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class AS ref ON ref.oid = con.confrelid
JOIN pg_catalog.pg_namespace AS ref_ns ON ref_ns.oid = ref.relnamespace
CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS keys(attnum, ref_attnum, ord)
JOIN pg_catalog.pg_attribute AS att ON att.attrelid = c.oid AND att.attnum = keys.attnum
JOIN pg_catalog.pg_attribute AS ref_att ON ref_att.attrelid = ref.oid AND ref_att.attnum = keys.ref_attnum
WHERE con.contype = 'f'
  AND (:schema_name::text IS NULL OR n.nspname = :schema_name)
  AND (:table_name::text IS NULL OR c.relname = :table_name)
ORDER BY c.relname, con.conname, keys.ord;

-- name: by_schema
-- dialect: postgres
SELECT
    c.relname::text AS table_name,
    att.attname::text AS column_name,
    ref.relname::text AS referenced_table_name,
    ref_att.attname::text AS referenced_column_name,
    con.conname::text AS constraint_name,
    n.nspname::text AS table_schema,
    ref_ns.nspname::text AS referenced_table_schema,
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_delete,
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
    END AS on_update
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_class AS ref ON ref.oid = con.confrelid
JOIN pg_catalog.pg_namespace AS ref_ns ON ref_ns.oid = ref.relnamespace
CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS keys(attnum, ref_attnum, ord)
JOIN pg_catalog.pg_attribute AS att ON att.attrelid = c.oid AND att.attnum = keys.attnum
JOIN pg_catalog.pg_attribute AS ref_att ON ref_att.attrelid = ref.oid AND ref_att.attnum = keys.ref_attnum
WHERE con.contype = 'f'
  AND (:schema_name::text IS NULL OR n.nspname = :schema_name)
ORDER BY c.relname, con.conname, keys.ord;
