-- name: by_table
-- dialect: mysql
SELECT
    kcu.table_name AS `table_name`,
    kcu.column_name AS `column_name`,
    kcu.referenced_table_name AS `referenced_table`,
    kcu.referenced_column_name AS `referenced_column`,
    kcu.constraint_name AS `constraint_name`,
    kcu.table_schema AS `schema`,
    kcu.referenced_table_schema AS `referenced_schema`,
    rc.delete_rule AS `on_delete`,
    rc.update_rule AS `on_update`
FROM information_schema.key_column_usage AS kcu
JOIN information_schema.referential_constraints AS rc
  ON rc.constraint_schema = kcu.constraint_schema
  AND rc.constraint_name = kcu.constraint_name
  AND rc.table_name = kcu.table_name
WHERE kcu.referenced_table_name IS NOT NULL
  AND kcu.table_name = :table_name
  AND kcu.table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY kcu.constraint_name, kcu.ordinal_position;

-- name: by_schema
-- dialect: mysql
SELECT
    kcu.table_name AS `table_name`,
    kcu.column_name AS `column_name`,
    kcu.referenced_table_name AS `referenced_table`,
    kcu.referenced_column_name AS `referenced_column`,
    kcu.constraint_name AS `constraint_name`,
    kcu.table_schema AS `schema`,
    kcu.referenced_table_schema AS `referenced_schema`,
    rc.delete_rule AS `on_delete`,
    rc.update_rule AS `on_update`
FROM information_schema.key_column_usage AS kcu
JOIN information_schema.referential_constraints AS rc
  ON rc.constraint_schema = kcu.constraint_schema
  AND rc.constraint_name = kcu.constraint_name
  AND rc.table_name = kcu.table_name
WHERE kcu.referenced_table_name IS NOT NULL
  AND kcu.table_schema = COALESCE(:schema_name, DATABASE())
ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
