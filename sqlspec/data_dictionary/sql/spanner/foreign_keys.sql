-- name: foreign_keys_by_table
-- dialect: spanner
WITH fk_constraints AS (
    SELECT
        constraint_name,
        unique_constraint_name
    FROM information_schema.referential_constraints
    WHERE (:schema_name IS NULL OR constraint_schema = :schema_name)
),
fk_columns AS (
    SELECT
        constraint_name,
        table_name,
        column_name,
        ordinal_position,
        table_schema
    FROM information_schema.key_column_usage
    WHERE (:schema_name IS NULL OR constraint_schema = :schema_name)
)
SELECT
    fk.table_name,
    fk.column_name,
    pk.table_name AS referenced_table_name,
    pk.column_name AS referenced_column_name,
    fk.constraint_name,
    fk.table_schema AS table_schema,
    pk.table_schema AS referenced_table_schema
FROM fk_columns fk
JOIN fk_constraints rc
  ON fk.constraint_name = rc.constraint_name
JOIN fk_columns pk
  ON pk.constraint_name = rc.unique_constraint_name
  AND pk.ordinal_position = fk.ordinal_position
WHERE (:table_name IS NULL OR fk.table_name = :table_name)
ORDER BY fk.table_name, fk.ordinal_position;

-- name: foreign_keys_by_schema
-- dialect: spanner
WITH fk_constraints AS (
    SELECT
        constraint_name,
        unique_constraint_name
    FROM information_schema.referential_constraints
    WHERE (:schema_name IS NULL OR constraint_schema = :schema_name)
),
fk_columns AS (
    SELECT
        constraint_name,
        table_name,
        column_name,
        ordinal_position,
        table_schema
    FROM information_schema.key_column_usage
    WHERE (:schema_name IS NULL OR constraint_schema = :schema_name)
)
SELECT
    fk.table_name,
    fk.column_name,
    pk.table_name AS referenced_table_name,
    pk.column_name AS referenced_column_name,
    fk.constraint_name,
    fk.table_schema AS table_schema,
    pk.table_schema AS referenced_table_schema
FROM fk_columns fk
JOIN fk_constraints rc
  ON fk.constraint_name = rc.constraint_name
JOIN fk_columns pk
  ON pk.constraint_name = rc.unique_constraint_name
  AND pk.ordinal_position = fk.ordinal_position
ORDER BY fk.table_name, fk.ordinal_position;
