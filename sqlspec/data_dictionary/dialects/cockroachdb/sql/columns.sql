-- name: by_schema
-- dialect: cockroachdb
SELECT
    c.table_catalog::text AS table_catalog,
    c.table_schema::text AS table_schema,
    c.table_schema::text AS schema_name,
    c.table_name::text AS table_name,
    c.column_name::text AS column_name,
    c.ordinal_position,
    c.data_type::text AS data_type,
    c.is_nullable::text AS is_nullable,
    c.column_default::text AS column_default,
    c.generation_expression::text AS generation_expression,
    c.is_hidden::text AS is_hidden,
    EXISTS (
        SELECT 1
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
         AND kcu.table_name = tc.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = c.table_schema
          AND tc.table_name = c.table_name
          AND kcu.column_name = c.column_name
    ) AS is_primary,
    COALESCE(
        (
            SELECT a.attidentity::text
            FROM pg_catalog.pg_attribute a
            WHERE a.attrelid = pg_catalog.to_regclass(
                pg_catalog.quote_ident(c.table_schema) || '.' || pg_catalog.quote_ident(c.table_name)
            )
              AND a.attname = c.column_name
        ),
        ''
    ) AS identity_generation,
    pg_catalog.pg_get_serial_sequence(
        pg_catalog.quote_ident(c.table_schema) || '.' || pg_catalog.quote_ident(c.table_name), c.column_name
    )::text AS sequence_name
FROM information_schema.columns AS c
WHERE c.table_schema = COALESCE(
    :schema_name::text,
    (
        SELECT rn.nspname::text
        FROM pg_catalog.pg_class rc
        JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
        WHERE rc.oid = pg_catalog.to_regclass(pg_catalog.quote_ident(:table_name::text))
    )
)
  AND (:table_name::text IS NULL OR c.table_name = :table_name)
ORDER BY c.table_schema, c.table_name, c.ordinal_position;

-- name: by_table
-- dialect: cockroachdb
SELECT
    a.attname::text AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text AS column_default
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE c.relname = :table_name
  AND n.nspname = :schema_name
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
