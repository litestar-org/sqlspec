-- name: columns_by_table
-- dialect: oracle
SELECT
    column_name AS column_name,
    data_type AS data_type,
    nullable AS is_nullable,
    data_default AS column_default
FROM all_tab_columns
WHERE owner = COALESCE(:schema_name, USER)
  AND table_name = UPPER(:table_name)
ORDER BY column_id;

-- name: columns_by_schema
-- dialect: oracle
SELECT
    table_name,
    column_name AS column_name,
    data_type AS data_type,
    nullable AS is_nullable,
    data_default AS column_default
FROM all_tab_columns
WHERE owner = COALESCE(:schema_name, USER)
ORDER BY table_name, column_id;
