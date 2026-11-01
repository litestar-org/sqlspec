-- name: by_schema
-- dialect: cockroachdb
WITH con_cols AS (
    SELECT
        kcu.constraint_catalog,
        kcu.constraint_schema,
        kcu.constraint_name,
        kcu.table_schema,
        kcu.table_name,
        array_agg(kcu.column_name::text ORDER BY kcu.ordinal_position) AS columns
    FROM information_schema.key_column_usage kcu
    WHERE kcu.table_schema = :schema_name
      AND (:table_name::text IS NULL OR kcu.table_name = :table_name)
    GROUP BY
        kcu.constraint_catalog,
        kcu.constraint_schema,
        kcu.constraint_name,
        kcu.table_schema,
        kcu.table_name
),
ref_cols AS (
    SELECT
        pk_kcu.constraint_catalog,
        pk_kcu.constraint_schema,
        pk_kcu.constraint_name,
        array_agg(pk_kcu.column_name::text ORDER BY pk_kcu.ordinal_position) AS referenced_columns
    FROM information_schema.key_column_usage pk_kcu
    GROUP BY
        pk_kcu.constraint_catalog,
        pk_kcu.constraint_schema,
        pk_kcu.constraint_name
)
SELECT
    tc.constraint_catalog::text AS constraint_catalog,
    tc.constraint_schema::text AS constraint_schema,
    tc.table_schema::text AS schema_name,
    tc.table_name::text AS table_name,
    tc.constraint_name::text AS constraint_name,
    tc.constraint_type::text AS constraint_type,
    tc.is_deferrable::text AS is_deferrable,
    tc.initially_deferred::text AS initially_deferred,
    COALESCE(cc.columns, ARRAY[]::text[]) AS columns,
    rc.unique_constraint_schema::text AS referenced_schema,
    pk_tc.table_name::text AS referenced_table,
    COALESCE(rc_cols.referenced_columns, ARRAY[]::text[]) AS referenced_columns
FROM information_schema.table_constraints tc
LEFT JOIN con_cols cc
  ON cc.constraint_catalog = tc.constraint_catalog
 AND cc.constraint_schema = tc.constraint_schema
 AND cc.constraint_name = tc.constraint_name
 AND cc.table_schema = tc.table_schema
 AND cc.table_name = tc.table_name
LEFT JOIN information_schema.referential_constraints rc
  ON rc.constraint_catalog = tc.constraint_catalog
 AND rc.constraint_schema = tc.constraint_schema
 AND rc.constraint_name = tc.constraint_name
LEFT JOIN information_schema.table_constraints pk_tc
  ON pk_tc.constraint_catalog = rc.unique_constraint_catalog
 AND pk_tc.constraint_schema = rc.unique_constraint_schema
 AND pk_tc.constraint_name = rc.unique_constraint_name
LEFT JOIN ref_cols rc_cols
  ON rc_cols.constraint_catalog = rc.unique_constraint_catalog
 AND rc_cols.constraint_schema = rc.unique_constraint_schema
 AND rc_cols.constraint_name = rc.unique_constraint_name
WHERE tc.table_schema = :schema_name
  AND (:table_name::text IS NULL OR tc.table_name = :table_name)
ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
