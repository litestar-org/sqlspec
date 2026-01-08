-- name: indexes_by_table
-- dialect: oracle
SELECT
    i.index_name,
    i.table_name AS table_name,
    CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique,
    CASE
        WHEN MAX(CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END) = 1 THEN 1
        ELSE 0
    END AS is_primary,
    LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns
FROM all_indexes i
JOIN all_ind_columns ic
  ON i.owner = ic.index_owner
  AND i.index_name = ic.index_name
LEFT JOIN all_constraints c
  ON c.owner = i.owner
  AND c.index_name = i.index_name
  AND c.constraint_type = 'P'
WHERE i.table_name = UPPER(:table_name)
  AND i.owner = COALESCE(:schema_name, USER)
GROUP BY i.index_name, i.table_name, i.uniqueness;

-- name: indexes_by_schema
-- dialect: oracle
SELECT
    i.index_name,
    i.table_name AS table_name,
    CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique,
    CASE
        WHEN MAX(CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END) = 1 THEN 1
        ELSE 0
    END AS is_primary,
    LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns
FROM all_indexes i
JOIN all_ind_columns ic
  ON i.owner = ic.index_owner
  AND i.index_name = ic.index_name
LEFT JOIN all_constraints c
  ON c.owner = i.owner
  AND c.index_name = i.index_name
  AND c.constraint_type = 'P'
WHERE i.owner = COALESCE(:schema_name, USER)
GROUP BY i.index_name, i.table_name, i.uniqueness;
