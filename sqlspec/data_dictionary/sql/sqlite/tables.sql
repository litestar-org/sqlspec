-- name: tables_by_schema
-- dialect: sqlite
WITH RECURSIVE dependency_tree AS (
    SELECT
        m.name as table_name,
        0 as level,
        '/' || m.name || '/' as path
    FROM {schema_prefix}sqlite_schema m
    WHERE m.type = 'table'
      AND m.name NOT LIKE 'sqlite_%'
      AND NOT EXISTS (
          SELECT 1 FROM pragma_foreign_key_list(m.name)
      )

    UNION ALL

    SELECT
        m.name as table_name,
        dt.level + 1,
        dt.path || m.name || '/'
    FROM {schema_prefix}sqlite_schema m
    JOIN pragma_foreign_key_list(m.name) fk
    JOIN dependency_tree dt ON fk."table" = dt.table_name
    WHERE m.type = 'table'
      AND m.name NOT LIKE 'sqlite_%'
      AND instr(dt.path, '/' || m.name || '/') = 0
)
SELECT DISTINCT table_name FROM dependency_tree ORDER BY level, table_name;
