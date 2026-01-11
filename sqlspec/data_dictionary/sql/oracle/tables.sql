-- name: tables_by_schema
-- dialect: oracle
SELECT
    table_name,
    MAX(level) AS level
FROM all_constraints
WHERE owner = COALESCE(:schema_name, USER)
START WITH table_name NOT IN (
    SELECT table_name
    FROM all_constraints
    WHERE constraint_type = 'R'
      AND owner = COALESCE(:schema_name, USER)
)
CONNECT BY NOCYCLE PRIOR constraint_name = r_constraint_name
    AND PRIOR owner = owner
GROUP BY table_name
ORDER BY level, table_name;

-- name: all_tables_by_schema
-- dialect: oracle
SELECT
    table_name
FROM all_tables
WHERE owner = COALESCE(:schema_name, USER)
ORDER BY table_name;
