-- name: by_owner
-- dialect: oracle
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
    c.hidden_column,
    c.virtual_column,
    c.segment_column_id,
    c.internal_column_id,
    c.identity_column,
    c.default_on_null,
    c.collation,
    cc.comments AS column_comment
FROM all_tab_cols c
LEFT JOIN all_col_comments cc
  ON cc.owner = c.owner
  AND cc.table_name = c.table_name
  AND cc.column_name = c.column_name
WHERE c.owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR c.table_name = :table_name)
ORDER BY c.owner, c.table_name, c.internal_column_id;

-- name: columns_by_table
-- dialect: oracle
SELECT
    column_name AS column_name,
    data_type AS data_type,
    nullable AS is_nullable,
    data_default AS column_default,
    column_id AS ordinal_position,
    hidden_column,
    virtual_column,
    identity_column
FROM all_tab_cols
WHERE owner = COALESCE(:schema_name, USER)
  AND table_name = :table_name
ORDER BY column_id;

-- name: columns_by_schema
-- dialect: oracle
SELECT
    table_name,
    column_name AS column_name,
    data_type AS data_type,
    nullable AS is_nullable,
    data_default AS column_default,
    column_id AS ordinal_position,
    hidden_column,
    virtual_column,
    identity_column
FROM all_tab_cols
WHERE owner = COALESCE(:schema_name, USER)
ORDER BY table_name, column_id;
