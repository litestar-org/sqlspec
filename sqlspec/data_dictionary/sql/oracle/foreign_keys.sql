-- name: foreign_keys_by_table
-- dialect: oracle
SELECT
    c.table_name,
    cc.column_name,
    r.table_name AS referenced_table_name,
    rcc.column_name AS referenced_column_name,
    c.constraint_name,
    c.owner AS table_schema,
    r.owner AS referenced_table_schema
FROM all_constraints c
JOIN all_cons_columns cc
  ON c.owner = cc.owner
  AND c.constraint_name = cc.constraint_name
JOIN all_constraints r
  ON c.r_owner = r.owner
  AND c.r_constraint_name = r.constraint_name
JOIN all_cons_columns rcc
  ON r.owner = rcc.owner
  AND r.constraint_name = rcc.constraint_name
  AND cc.position = rcc.position
WHERE c.constraint_type = 'R'
  AND c.owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR c.table_name = UPPER(:table_name));

-- name: foreign_keys_by_schema
-- dialect: oracle
SELECT
    c.table_name,
    cc.column_name,
    r.table_name AS referenced_table_name,
    rcc.column_name AS referenced_column_name,
    c.constraint_name,
    c.owner AS table_schema,
    r.owner AS referenced_table_schema
FROM all_constraints c
JOIN all_cons_columns cc
  ON c.owner = cc.owner
  AND c.constraint_name = cc.constraint_name
JOIN all_constraints r
  ON c.r_owner = r.owner
  AND c.r_constraint_name = r.constraint_name
JOIN all_cons_columns rcc
  ON r.owner = rcc.owner
  AND r.constraint_name = rcc.constraint_name
  AND cc.position = rcc.position
WHERE c.constraint_type = 'R'
  AND c.owner = COALESCE(:schema_name, USER);
