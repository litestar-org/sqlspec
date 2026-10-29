-- name: by_object
-- dialect: cockroachdb
WITH ddl_objects AS (
    SELECT
        ct.schema_name::text AS schema_name,
        ct.name::text AS object_name,
        COALESCE(
            CASE c.relkind
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized_view'
                WHEN 'S' THEN 'sequence'
                ELSE 'table'
            END,
            CASE
                WHEN ct.create_statement ILIKE 'CREATE VIEW%' THEN 'view'
                WHEN ct.create_statement ILIKE 'CREATE MATERIALIZED VIEW%' THEN 'materialized_view'
                WHEN ct.create_statement ILIKE 'CREATE SEQUENCE%' THEN 'sequence'
                ELSE 'table'
            END
        )::text AS object_type,
        ct.name::text AS table_name,
        ct.create_statement::text AS ddl,
        'native'::text AS fidelity
    FROM crdb_internal.tables ct
    LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = ct.schema_name
    LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = ct.name
    WHERE ct.create_statement IS NOT NULL
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        i.relname::text AS object_name,
        'index'::text AS object_type,
        t.relname::text AS table_name,
        pg_catalog.pg_get_indexdef(i.oid)::text AS ddl,
        'native'::text AS fidelity
    FROM pg_catalog.pg_index ix
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
    WHERE NOT ix.indisprimary
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        p.proname::text AS object_name,
        'function'::text AS object_type,
        NULL::text AS table_name,
        pg_catalog.pg_get_functiondef(p.oid)::text AS ddl,
        'native'::text AS fidelity
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        tg.tgname::text AS object_name,
        'trigger'::text AS object_type,
        t.relname::text AS table_name,
        pg_catalog.pg_get_triggerdef(tg.oid, true)::text AS ddl,
        'native'::text AS fidelity
    FROM pg_catalog.pg_trigger tg
    JOIN pg_catalog.pg_class t ON t.oid = tg.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    WHERE NOT tg.tgisinternal
)
SELECT
    schema_name,
    object_name,
    object_type,
    table_name,
    ddl,
    fidelity
FROM ddl_objects
WHERE schema_name = :schema_name
  AND object_name = :object_name
  AND (
      :object_type::text IS NULL
      OR object_type = LOWER(:object_type::text)
      OR (:object_type::text IN ('routine', 'function', 'procedure') AND object_type = 'function')
  )
ORDER BY schema_name, object_name;

-- name: by_schema
-- dialect: cockroachdb
WITH ddl_objects AS (
    SELECT
        ct.schema_name::text AS schema_name,
        ct.name::text AS object_name,
        COALESCE(
            CASE c.relkind
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'materialized_view'
                WHEN 'S' THEN 'sequence'
                ELSE 'table'
            END,
            CASE
                WHEN ct.create_statement ILIKE 'CREATE VIEW%' THEN 'view'
                WHEN ct.create_statement ILIKE 'CREATE MATERIALIZED VIEW%' THEN 'materialized_view'
                WHEN ct.create_statement ILIKE 'CREATE SEQUENCE%' THEN 'sequence'
                ELSE 'table'
            END
        )::text AS object_type,
        ct.name::text AS table_name,
        ct.create_statement::text AS ddl,
        'native'::text AS fidelity
    FROM crdb_internal.tables ct
    LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = ct.schema_name
    LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = ct.name
    WHERE ct.create_statement IS NOT NULL
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        i.relname::text AS object_name,
        'index'::text AS object_type,
        t.relname::text AS table_name,
        pg_catalog.pg_get_indexdef(i.oid)::text AS ddl,
        'native'::text AS fidelity
    FROM pg_catalog.pg_index ix
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
    WHERE NOT ix.indisprimary
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        p.proname::text AS object_name,
        'function'::text AS object_type,
        NULL::text AS table_name,
        pg_catalog.pg_get_functiondef(p.oid)::text AS ddl,
        'native'::text AS fidelity
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        tg.tgname::text AS object_name,
        'trigger'::text AS object_type,
        t.relname::text AS table_name,
        pg_catalog.pg_get_triggerdef(tg.oid, true)::text AS ddl,
        'native'::text AS fidelity
    FROM pg_catalog.pg_trigger tg
    JOIN pg_catalog.pg_class t ON t.oid = tg.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
    WHERE NOT tg.tgisinternal
)
SELECT
    schema_name,
    object_name,
    object_type,
    table_name,
    ddl,
    fidelity
FROM ddl_objects
WHERE schema_name = :schema_name
  AND ddl IS NOT NULL
ORDER BY
    CASE object_type
        WHEN 'sequence' THEN 1
        WHEN 'table' THEN 2
        WHEN 'view' THEN 3
        WHEN 'materialized_view' THEN 4
        WHEN 'index' THEN 5
        WHEN 'function' THEN 6
        WHEN 'trigger' THEN 7
        ELSE 8
    END,
    object_name;
