-- name: by_object
-- dialect: duckdb
WITH ddl_objects AS (
    SELECT
        schema_name,
        table_name AS object_name,
        CASE WHEN temporary THEN 'temporary_table' ELSE 'table' END AS object_type,
        table_name AS table_name,
        sql
    FROM duckdb_tables()
    WHERE NOT internal
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        view_name AS object_name,
        'view' AS object_type,
        NULL AS table_name,
        sql
    FROM duckdb_views()
    WHERE NOT internal
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        index_name AS object_name,
        'index' AS object_type,
        table_name AS table_name,
        sql
    FROM duckdb_indexes()
    WHERE sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        sequence_name AS object_name,
        'sequence' AS object_type,
        NULL AS table_name,
        sql
    FROM duckdb_sequences()
    WHERE sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        type_name AS object_name,
        'type' AS object_type,
        NULL AS table_name,
        CASE
            WHEN logical_type = 'ENUM' AND labels IS NOT NULL THEN
                'CREATE TYPE ' || type_name || ' AS ENUM (' || array_to_string(
                    list_transform(labels, lambda x: '''' || replace(x, '''', '''''') || ''''),
                    ', '
                ) || ');'
            WHEN logical_type IS NOT NULL THEN
                'CREATE TYPE ' || type_name || ' AS ' || logical_type || ';'
            ELSE NULL
        END AS sql
    FROM duckdb_types()
    WHERE NOT internal
)
SELECT
    schema_name,
    object_name,
    object_type,
    table_name,
    sql AS ddl,
    'native' AS fidelity
FROM ddl_objects
WHERE sql IS NOT NULL
  AND schema_name = COALESCE(:schema_name, current_schema())
  AND object_name = :object_name
  AND (:object_type IS NULL OR object_type = LOWER(:object_type))
ORDER BY schema_name, object_type, object_name;

-- name: by_schema
-- dialect: duckdb
WITH ddl_objects AS (
    SELECT
        schema_name,
        table_name AS object_name,
        CASE WHEN temporary THEN 'temporary_table' ELSE 'table' END AS object_type,
        table_name AS table_name,
        sql
    FROM duckdb_tables()
    WHERE NOT internal
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        view_name AS object_name,
        'view' AS object_type,
        NULL AS table_name,
        sql
    FROM duckdb_views()
    WHERE NOT internal
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        index_name AS object_name,
        'index' AS object_type,
        table_name AS table_name,
        sql
    FROM duckdb_indexes()
    WHERE sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        sequence_name AS object_name,
        'sequence' AS object_type,
        NULL AS table_name,
        sql
    FROM duckdb_sequences()
    WHERE sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        type_name AS object_name,
        'type' AS object_type,
        NULL AS table_name,
        CASE
            WHEN logical_type = 'ENUM' AND labels IS NOT NULL THEN
                'CREATE TYPE ' || type_name || ' AS ENUM (' || array_to_string(
                    list_transform(labels, lambda x: '''' || replace(x, '''', '''''') || ''''),
                    ', '
                ) || ');'
            WHEN logical_type IS NOT NULL THEN
                'CREATE TYPE ' || type_name || ' AS ' || logical_type || ';'
            ELSE NULL
        END AS sql
    FROM duckdb_types()
    WHERE NOT internal
)
SELECT
    schema_name,
    object_name,
    object_type,
    table_name,
    sql AS ddl,
    'native' AS fidelity
FROM ddl_objects
WHERE sql IS NOT NULL
  AND schema_name = COALESCE(:schema_name, current_schema())
ORDER BY
    CASE object_type
        WHEN 'type' THEN 1
        WHEN 'sequence' THEN 2
        WHEN 'table' THEN 3
        WHEN 'temporary_table' THEN 4
        WHEN 'view' THEN 5
        WHEN 'index' THEN 6
        ELSE 7
    END,
    object_name;
