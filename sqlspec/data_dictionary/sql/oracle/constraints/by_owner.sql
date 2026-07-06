-- name: by_owner
-- dialect: oracle
SELECT
    c.owner AS schema_name,
    c.table_name,
    c.constraint_name,
    c.constraint_type,
    c.search_condition_vc AS search_condition,
    c.r_owner AS referenced_schema,
    r.table_name AS referenced_table,
    c.r_constraint_name AS referenced_constraint,
    c.delete_rule,
    c.status,
    c.deferrable,
    c.deferred,
    c.validated,
    c.generated,
    c.bad,
    c.rely,
    c.last_change,
    LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS columns
FROM all_constraints c
LEFT JOIN all_cons_columns cc
  ON cc.owner = c.owner
  AND cc.constraint_name = c.constraint_name
LEFT JOIN all_constraints r
  ON r.owner = c.r_owner
  AND r.constraint_name = c.r_constraint_name
WHERE c.owner = COALESCE(:schema_name, USER)
  AND (:table_name IS NULL OR c.table_name = :table_name)
GROUP BY
    c.owner,
    c.table_name,
    c.constraint_name,
    c.constraint_type,
    c.search_condition_vc,
    c.r_owner,
    r.table_name,
    c.r_constraint_name,
    c.delete_rule,
    c.status,
    c.deferrable,
    c.deferred,
    c.validated,
    c.generated,
    c.bad,
    c.rely,
    c.last_change
ORDER BY c.owner, c.table_name, c.constraint_name;
