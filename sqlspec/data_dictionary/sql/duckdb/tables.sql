-- name: tables_by_schema
-- dialect: duckdb
WITH RECURSIVE dependency_tree AS (
    SELECT
        table_name,
        0 AS level,
        [table_name] AS path
    FROM information_schema.tables t
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema = COALESCE(:schema_name, current_schema())
      AND NOT EXISTS (
          SELECT 1
          FROM information_schema.key_column_usage kcu
          WHERE kcu.table_name = t.table_name
            AND kcu.table_schema = t.table_schema
            AND kcu.constraint_name IN (
                SELECT constraint_name FROM information_schema.referential_constraints
            )
      )

    UNION ALL

    SELECT
        kcu.table_name,
        dt.level + 1,
        list_append(dt.path, kcu.table_name)
    FROM information_schema.key_column_usage kcu
    JOIN information_schema.referential_constraints rc ON kcu.constraint_name = rc.constraint_name
    JOIN information_schema.key_column_usage pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
    JOIN dependency_tree dt ON dt.table_name = pk_kcu.table_name
    WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
      AND NOT list_contains(dt.path, kcu.table_name)
)
SELECT DISTINCT table_name, level
FROM dependency_tree
ORDER BY level, table_name;
