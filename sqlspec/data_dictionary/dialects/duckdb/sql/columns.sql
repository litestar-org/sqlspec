-- name: by_schema
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    table_name,
    column_name,
    column_index AS ordinal_position,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length AS max_length,
    numeric_precision,
    numeric_scale,
    comment,
    internal,
    EXISTS (
        SELECT 1
        FROM duckdb_constraints() AS k
        WHERE k.database_name = c.database_name
          AND k.schema_name = c.schema_name
          AND k.table_name = c.table_name
          AND k.constraint_type = 'PRIMARY KEY'
          AND list_contains(k.constraint_column_names, c.column_name)
    ) AS is_primary,
    EXISTS (
        SELECT 1
        FROM (
            SELECT
                database_name,
                schema_name,
                table_name,
                regexp_replace(sql, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') AS ddl_quoted,
                regexp_replace(sql, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g') AS ddl_unquoted
            FROM duckdb_tables()
        ) AS t
        WHERE t.database_name = c.database_name
          AND t.schema_name = c.schema_name
          AND t.table_name = c.table_name
          AND (
              contains(t.ddl_unquoted, '(' || c.column_name || ' ' || c.data_type || ' GENERATED ALWAYS AS(')
              OR contains(t.ddl_unquoted, ', ' || c.column_name || ' ' || c.data_type || ' GENERATED ALWAYS AS(')
              OR contains(
                  t.ddl_quoted,
                  '("' || replace(c.column_name, '"', '""') || '" ' || c.data_type || ' GENERATED ALWAYS AS('
              )
              OR contains(
                  t.ddl_quoted,
                  ', "' || replace(c.column_name, '"', '""') || '" ' || c.data_type || ' GENERATED ALWAYS AS('
              )
          )
    ) AS is_generated
FROM duckdb_columns() AS c
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND NOT internal
ORDER BY table_name, column_index;

-- name: by_table
-- dialect: duckdb
SELECT
    database_name,
    schema_name,
    table_name,
    column_name,
    column_index AS ordinal_position,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length AS max_length,
    numeric_precision,
    numeric_scale,
    comment,
    internal,
    EXISTS (
        SELECT 1
        FROM duckdb_constraints() AS k
        WHERE k.database_name = c.database_name
          AND k.schema_name = c.schema_name
          AND k.table_name = c.table_name
          AND k.constraint_type = 'PRIMARY KEY'
          AND list_contains(k.constraint_column_names, c.column_name)
    ) AS is_primary,
    EXISTS (
        SELECT 1
        FROM (
            SELECT
                database_name,
                schema_name,
                table_name,
                regexp_replace(sql, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') AS ddl_quoted,
                regexp_replace(sql, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g') AS ddl_unquoted
            FROM duckdb_tables()
        ) AS t
        WHERE t.database_name = c.database_name
          AND t.schema_name = c.schema_name
          AND t.table_name = c.table_name
          AND (
              contains(t.ddl_unquoted, '(' || c.column_name || ' ' || c.data_type || ' GENERATED ALWAYS AS(')
              OR contains(t.ddl_unquoted, ', ' || c.column_name || ' ' || c.data_type || ' GENERATED ALWAYS AS(')
              OR contains(
                  t.ddl_quoted,
                  '("' || replace(c.column_name, '"', '""') || '" ' || c.data_type || ' GENERATED ALWAYS AS('
              )
              OR contains(
                  t.ddl_quoted,
                  ', "' || replace(c.column_name, '"', '""') || '" ' || c.data_type || ' GENERATED ALWAYS AS('
              )
          )
    ) AS is_generated
FROM duckdb_columns() AS c
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND table_name = :table_name
  AND NOT internal
ORDER BY column_index;
