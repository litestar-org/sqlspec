-- name: by_schema
-- dialect: duckdb
WITH table_ddl AS (
    SELECT
        database_name,
        schema_name,
        table_name,
        regexp_replace(sql, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') AS ddl_quoted,
        regexp_replace(sql, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g') AS ddl_unquoted
    FROM duckdb_tables()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
),
pk_constraints AS (
    SELECT
        database_name,
        schema_name,
        table_name,
        unnest(constraint_column_names) AS column_name
    FROM duckdb_constraints()
    WHERE constraint_type = 'PRIMARY KEY'
      AND schema_name = COALESCE(:schema_name, current_schema())
)
SELECT
    c.database_name,
    c.schema_name,
    c.table_name,
    c.column_name,
    c.column_index AS ordinal_position,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.character_maximum_length AS max_length,
    c.numeric_precision,
    c.numeric_scale,
    c.comment,
    c.internal,
    pk.column_name IS NOT NULL AS is_primary,
    COALESCE(
        contains(
            t.ddl_unquoted,
            '(' || c.column_name || ' ' || regexp_replace(c.data_type, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g')
            || ' GENERATED ALWAYS AS('
        )
        OR contains(
            t.ddl_unquoted,
            ', ' || c.column_name || ' ' || regexp_replace(c.data_type, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g')
            || ' GENERATED ALWAYS AS('
        )
        OR contains(
            t.ddl_quoted,
            '("' || replace(c.column_name, '"', '""') || '" '
            || regexp_replace(c.data_type, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') || ' GENERATED ALWAYS AS('
        )
        OR contains(
            t.ddl_quoted,
            ', "' || replace(c.column_name, '"', '""') || '" '
            || regexp_replace(c.data_type, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') || ' GENERATED ALWAYS AS('
        ),
        FALSE
    ) AS is_generated
FROM duckdb_columns() AS c
LEFT JOIN table_ddl AS t
    ON t.database_name = c.database_name
    AND t.schema_name = c.schema_name
    AND t.table_name = c.table_name
LEFT JOIN pk_constraints AS pk
    ON pk.database_name = c.database_name
    AND pk.schema_name = c.schema_name
    AND pk.table_name = c.table_name
    AND pk.column_name = c.column_name
WHERE c.schema_name = COALESCE(:schema_name, current_schema())
  AND NOT c.internal
ORDER BY c.table_name, c.column_index;

-- name: by_table
-- dialect: duckdb
WITH table_ddl AS (
    SELECT
        database_name,
        schema_name,
        table_name,
        regexp_replace(sql, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') AS ddl_quoted,
        regexp_replace(sql, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g') AS ddl_unquoted
    FROM duckdb_tables()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
      AND table_name = :table_name
),
pk_constraints AS (
    SELECT
        database_name,
        schema_name,
        table_name,
        unnest(constraint_column_names) AS column_name
    FROM duckdb_constraints()
    WHERE constraint_type = 'PRIMARY KEY'
      AND schema_name = COALESCE(:schema_name, current_schema())
      AND table_name = :table_name
)
SELECT
    c.database_name,
    c.schema_name,
    c.table_name,
    c.column_name,
    c.column_index AS ordinal_position,
    c.data_type,
    c.is_nullable,
    c.column_default,
    c.character_maximum_length AS max_length,
    c.numeric_precision,
    c.numeric_scale,
    c.comment,
    c.internal,
    pk.column_name IS NOT NULL AS is_primary,
    COALESCE(
        contains(
            t.ddl_unquoted,
            '(' || c.column_name || ' ' || regexp_replace(c.data_type, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g')
            || ' GENERATED ALWAYS AS('
        )
        OR contains(
            t.ddl_unquoted,
            ', ' || c.column_name || ' ' || regexp_replace(c.data_type, '"([^"]|"")*"|''([^'']|'''')*''', '', 'g')
            || ' GENERATED ALWAYS AS('
        )
        OR contains(
            t.ddl_quoted,
            '("' || replace(c.column_name, '"', '""') || '" '
            || regexp_replace(c.data_type, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') || ' GENERATED ALWAYS AS('
        )
        OR contains(
            t.ddl_quoted,
            ', "' || replace(c.column_name, '"', '""') || '" '
            || regexp_replace(c.data_type, '("([^"]|"")*")|''([^'']|'''')*''', '\1', 'g') || ' GENERATED ALWAYS AS('
        ),
        FALSE
    ) AS is_generated
FROM duckdb_columns() AS c
LEFT JOIN table_ddl AS t
    ON t.database_name = c.database_name
    AND t.schema_name = c.schema_name
    AND t.table_name = c.table_name
LEFT JOIN pk_constraints AS pk
    ON pk.database_name = c.database_name
    AND pk.schema_name = c.schema_name
    AND pk.table_name = c.table_name
    AND pk.column_name = c.column_name
WHERE c.schema_name = COALESCE(:schema_name, current_schema())
  AND c.table_name = :table_name
  AND NOT c.internal
ORDER BY c.column_index;
