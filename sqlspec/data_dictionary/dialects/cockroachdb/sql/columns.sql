-- name: by_schema
-- dialect: cockroachdb
WITH target_schema AS (
    SELECT COALESCE(
        :schema_name::text,
        (
            SELECT rn.nspname::text
            FROM pg_catalog.pg_class rc
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
            WHERE rc.oid = pg_catalog.to_regclass(pg_catalog.quote_ident(:table_name::text))
        ),
        current_schema()
    ) AS schema_name
),
pk_columns AS (
    SELECT DISTINCT
        tc.table_schema,
        tc.table_name,
        kcu.column_name
    FROM target_schema ts
    JOIN information_schema.table_constraints tc
      ON tc.table_schema = ts.schema_name
    JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_schema = tc.constraint_schema
     AND kcu.constraint_name = tc.constraint_name
     AND kcu.table_name = tc.table_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND (:table_name::text IS NULL OR tc.table_name = :table_name)
),
identity_attrs AS (
    SELECT
        n.nspname AS table_schema,
        c.relname AS table_name,
        a.attname AS column_name,
        CASE
            WHEN a.attidentity = 'a' THEN 'ALWAYS'
            WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
            ELSE NULL
        END AS identity_generation,
        CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity
    FROM target_schema ts
    JOIN pg_catalog.pg_namespace n ON n.nspname = ts.schema_name
    JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND (:table_name::text IS NULL OR c.relname = :table_name)
)
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
    CASE WHEN c.generation_expression IS NOT NULL AND c.generation_expression <> '' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN c.is_hidden = 'YES' THEN 1 ELSE 0 END AS is_hidden,
    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    ia.identity_generation,
    COALESCE(ia.is_identity, 0) AS is_identity,
    pg_catalog.pg_get_serial_sequence(
        pg_catalog.quote_ident(c.table_schema) || '.' || pg_catalog.quote_ident(c.table_name), c.column_name
    )::text AS sequence_name
FROM target_schema ts
JOIN information_schema.columns AS c
  ON c.table_schema = ts.schema_name
LEFT JOIN pk_columns pk
  ON pk.table_schema = c.table_schema
 AND pk.table_name = c.table_name
 AND pk.column_name = c.column_name
LEFT JOIN identity_attrs ia
  ON ia.table_schema = c.table_schema
 AND ia.table_name = c.table_name
 AND ia.column_name = c.column_name
WHERE (:table_name::text IS NULL OR c.table_name = :table_name)
ORDER BY c.table_schema, c.table_name, c.ordinal_position;

-- name: by_table
-- dialect: cockroachdb
WITH target_schema AS (
    SELECT COALESCE(
        :schema_name::text,
        (
            SELECT rn.nspname::text
            FROM pg_catalog.pg_class rc
            JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
            WHERE rc.oid = pg_catalog.to_regclass(pg_catalog.quote_ident(:table_name::text))
        ),
        current_schema()
    ) AS schema_name
),
pk_columns AS (
    SELECT DISTINCT
        tc.table_schema,
        tc.table_name,
        kcu.column_name
    FROM target_schema ts
    JOIN information_schema.table_constraints tc
      ON tc.table_schema = ts.schema_name
    JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_schema = tc.constraint_schema
     AND kcu.constraint_name = tc.constraint_name
     AND kcu.table_name = tc.table_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_name = :table_name
),
identity_attrs AS (
    SELECT
        n.nspname AS table_schema,
        c.relname AS table_name,
        a.attname AS column_name,
        CASE
            WHEN a.attidentity = 'a' THEN 'ALWAYS'
            WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
            ELSE NULL
        END AS identity_generation,
        CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity
    FROM target_schema ts
    JOIN pg_catalog.pg_namespace n ON n.nspname = ts.schema_name
    JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
    WHERE a.attnum > 0
      AND NOT a.attisdropped
      AND c.relname = :table_name
)
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
    CASE WHEN c.generation_expression IS NOT NULL AND c.generation_expression <> '' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN c.is_hidden = 'YES' THEN 1 ELSE 0 END AS is_hidden,
    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    ia.identity_generation,
    COALESCE(ia.is_identity, 0) AS is_identity,
    pg_catalog.pg_get_serial_sequence(
        pg_catalog.quote_ident(c.table_schema) || '.' || pg_catalog.quote_ident(c.table_name), c.column_name
    )::text AS sequence_name
FROM target_schema ts
JOIN information_schema.columns AS c
  ON c.table_schema = ts.schema_name
LEFT JOIN pk_columns pk
  ON pk.table_schema = c.table_schema
 AND pk.table_name = c.table_name
 AND pk.column_name = c.column_name
LEFT JOIN identity_attrs ia
  ON ia.table_schema = c.table_schema
 AND ia.table_name = c.table_name
 AND ia.column_name = c.column_name
WHERE c.table_name = :table_name
ORDER BY c.ordinal_position;
