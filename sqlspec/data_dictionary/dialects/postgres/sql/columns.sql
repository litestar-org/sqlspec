-- name: by_schema
-- dialect: postgres
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
table_constraints AS (
    SELECT
        con.conrelid,
        unnest(con.conkey) AS attnum,
        bool_or(con.contype = 'p') AS is_primary,
        bool_or(con.contype = 'u') AS is_unique
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN target_schema ts ON ts.schema_name = n.nspname
    WHERE con.contype IN ('p', 'u')
      AND (:table_name::text IS NULL OR c.relname = :table_name)
    GROUP BY con.conrelid, unnest(con.conkey)
)
SELECT
    current_database()::text AS table_catalog,
    n.nspname::text AS table_schema,
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    a.attname::text AS column_name,
    a.attnum::integer AS ordinal_position,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    t.typname::text AS type_name,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    CASE WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS column_default,
    CASE
        WHEN a.attidentity = 'a' THEN 'ALWAYS'
        WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
        ELSE NULL
    END AS identity_generation,
    CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN a.attgenerated <> '' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN a.attgenerated <> '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS generation_expression,
    col_description(c.oid, a.attnum)::text AS comment,
    COALESCE(tc.is_primary, false) AS is_primary,
    COALESCE(tc.is_unique, false) AS is_unique,
    pg_catalog.pg_get_serial_sequence(
        pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname), a.attname
    )::text AS sequence_name
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN target_schema ts ON ts.schema_name = n.nspname
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
LEFT JOIN table_constraints tc ON tc.conrelid = c.oid AND tc.attnum = a.attnum
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND (:table_name::text IS NULL OR c.relname = :table_name)
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY n.nspname, c.relname, a.attnum;

-- name: by_table
-- dialect: postgres
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
table_constraints AS (
    SELECT
        con.conrelid,
        unnest(con.conkey) AS attnum,
        bool_or(con.contype = 'p') AS is_primary,
        bool_or(con.contype = 'u') AS is_unique
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN target_schema ts ON ts.schema_name = n.nspname
    WHERE con.contype IN ('p', 'u')
      AND c.relname = :table_name::text
    GROUP BY con.conrelid, unnest(con.conkey)
)
SELECT
    current_database()::text AS table_catalog,
    n.nspname::text AS table_schema,
    n.nspname::text AS schema_name,
    c.relname::text AS table_name,
    a.attname::text AS column_name,
    a.attnum::integer AS ordinal_position,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    t.typname::text AS type_name,
    CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
    CASE WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS column_default,
    CASE
        WHEN a.attidentity = 'a' THEN 'ALWAYS'
        WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
        ELSE NULL
    END AS identity_generation,
    CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity,
    CASE WHEN a.attgenerated <> '' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN a.attgenerated <> '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS generation_expression,
    col_description(c.oid, a.attnum)::text AS comment,
    COALESCE(tc.is_primary, false) AS is_primary,
    COALESCE(tc.is_unique, false) AS is_unique,
    pg_catalog.pg_get_serial_sequence(
        pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname), a.attname
    )::text AS sequence_name
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
JOIN target_schema ts ON ts.schema_name = n.nspname
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
LEFT JOIN table_constraints tc ON tc.conrelid = c.oid AND tc.attnum = a.attnum
WHERE c.relname = :table_name::text
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
