-- name: by_schema
-- dialect: oracle
WITH pk_columns AS (
    SELECT DISTINCT
        acc.owner,
        acc.table_name,
        acc.column_name
    FROM all_constraints ac
    JOIN all_cons_columns acc
      ON acc.owner = ac.owner
     AND acc.constraint_name = ac.constraint_name
     AND acc.table_name = ac.table_name
    WHERE ac.owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR ac.table_name = :table_name)
      AND ac.constraint_type = 'P'
)
SELECT
    c.owner AS schema_name,
    c.table_name,
    c.column_name,
    c.data_type,
    c.data_type_owner,
    c.data_length AS max_length,
    c.data_precision AS numeric_precision,
    c.data_scale AS numeric_scale,
    c.nullable AS is_nullable,
    c.column_id AS ordinal_position,
    c.data_default AS column_default,
    CASE WHEN c.hidden_column = 'YES' THEN 1 ELSE 0 END AS is_hidden,
    CASE WHEN c.virtual_column = 'YES' THEN 1 ELSE 0 END AS is_generated,
    c.segment_column_id,
    c.internal_column_id,
    CASE WHEN c.identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity,
    CASE
        WHEN ic.generation_type = 'BY DEFAULT' AND c.default_on_null = 'YES' THEN 'BY DEFAULT ON NULL'
        ELSE ic.generation_type
    END AS identity_generation,
    ic.sequence_name AS sequence_name,
    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
    c.collation,
    cc.comments AS column_comment
FROM all_tab_cols c
LEFT JOIN pk_columns pk
  ON pk.owner = c.owner
 AND pk.table_name = c.table_name
 AND pk.column_name = c.column_name
LEFT JOIN all_tab_identity_cols ic
  ON ic.owner = c.owner
 AND ic.table_name = c.table_name
 AND ic.column_name = c.column_name
LEFT JOIN all_col_comments cc
  ON cc.owner = c.owner
 AND cc.table_name = c.table_name
 AND cc.column_name = c.column_name
WHERE c.owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR c.table_name = :table_name)
ORDER BY c.owner, c.table_name, c.internal_column_id;

-- name: by_table
-- dialect: oracle
WITH pk_columns AS (
    SELECT DISTINCT
        acc.owner,
        acc.table_name,
        acc.column_name
    FROM all_constraints ac
    JOIN all_cons_columns acc
      ON acc.owner = ac.owner
     AND acc.constraint_name = ac.constraint_name
     AND acc.table_name = ac.table_name
    WHERE ac.owner = COALESCE(:schema_name, USER)
      AND ac.table_name = :table_name
      AND ac.constraint_type = 'P'
)
SELECT
    c.owner AS schema_name,
    c.table_name AS table_name,
    c.column_name AS column_name,
    c.data_type AS data_type,
    c.nullable AS is_nullable,
    c.data_default AS column_default,
    c.column_id AS ordinal_position,
    CASE WHEN c.hidden_column = 'YES' THEN 1 ELSE 0 END AS is_hidden,
    CASE WHEN c.virtual_column = 'YES' THEN 1 ELSE 0 END AS is_generated,
    CASE WHEN c.identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity,
    CASE
        WHEN ic.generation_type = 'BY DEFAULT' AND c.default_on_null = 'YES' THEN 'BY DEFAULT ON NULL'
        ELSE ic.generation_type
    END AS identity_generation,
    ic.sequence_name AS sequence_name,
    CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary
FROM all_tab_cols c
LEFT JOIN pk_columns pk
  ON pk.owner = c.owner
 AND pk.table_name = c.table_name
 AND pk.column_name = c.column_name
LEFT JOIN all_tab_identity_cols ic
  ON ic.owner = c.owner
 AND ic.table_name = c.table_name
 AND ic.column_name = c.column_name
WHERE c.owner = COALESCE(:schema_name, USER)
  AND c.table_name = :table_name
ORDER BY c.column_id;
