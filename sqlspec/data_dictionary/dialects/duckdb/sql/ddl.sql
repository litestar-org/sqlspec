-- name: by_object
-- dialect: duckdb
WITH fk_tables AS (
    SELECT DISTINCT
        database_name,
        schema_name,
        table_name
    FROM duckdb_constraints()
    WHERE database_name = current_database()
      AND constraint_type = 'FOREIGN KEY'
),
ddl_objects AS (
    SELECT
        t.schema_name,
        t.table_name AS object_name,
        CASE WHEN t.temporary THEN 'temporary_table' ELSE 'table' END AS object_type,
        t.table_name AS table_name,
        t.sql,
        fk.table_name IS NOT NULL AS has_foreign_keys
    FROM duckdb_tables() AS t
    LEFT JOIN fk_tables AS fk
        ON fk.database_name = t.database_name
       AND fk.schema_name = t.schema_name
       AND fk.table_name = t.table_name
    WHERE NOT t.internal
      AND t.database_name = current_database()
      AND t.sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        view_name AS object_name,
        'view' AS object_type,
        NULL AS table_name,
        sql,
        FALSE AS has_foreign_keys
    FROM duckdb_views()
    WHERE NOT internal
      AND database_name = current_database()
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        index_name AS object_name,
        'index' AS object_type,
        table_name AS table_name,
        sql,
        FALSE AS has_foreign_keys
    FROM duckdb_indexes()
    WHERE database_name = current_database()
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        sequence_name AS object_name,
        'sequence' AS object_type,
        NULL AS table_name,
        'CREATE SEQUENCE '
            || CASE WHEN schema_name = 'main' THEN '' ELSE '"' || replace(schema_name, '"', '""') || '".' END
            || '"' || replace(sequence_name, '"', '""') || '"'
            || ' INCREMENT BY ' || increment_by
            || ' MINVALUE ' || min_value
            || ' MAXVALUE ' || max_value
            || ' START WITH ' || start_value
            || CASE WHEN cycle THEN ' CYCLE' ELSE ' NO CYCLE' END
            || ';' AS sql,
        FALSE AS has_foreign_keys
    FROM duckdb_sequences()
    WHERE database_name = current_database()
    UNION ALL
    SELECT
        schema_name,
        type_name AS object_name,
        'type' AS object_type,
        NULL AS table_name,
        CASE
            WHEN logical_type = 'ENUM' AND labels IS NOT NULL THEN
                'CREATE TYPE '
                    || CASE WHEN schema_name = 'main' THEN '' ELSE '"' || replace(schema_name, '"', '""') || '".' END
                    || '"' || replace(type_name, '"', '""') || '" AS ENUM ('
                    || array_to_string(
                        list_transform(labels, lambda x: '''' || replace(x, '''', '''''') || ''''),
                        ', '
                    ) || ');'
            ELSE NULL
        END AS sql,
        FALSE AS has_foreign_keys
    FROM duckdb_types()
    WHERE NOT internal
      AND database_name = current_database()
      AND logical_type = 'ENUM'
      AND labels IS NOT NULL
)
SELECT
    schema_name,
    object_name,
    object_type,
    table_name,
    sql AS ddl,
    has_foreign_keys,
    'native' AS fidelity
FROM ddl_objects
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND object_name = :object_name
  AND sql IS NOT NULL
  AND (:object_type IS NULL OR object_type = LOWER(:object_type))
ORDER BY schema_name, object_type, object_name;

-- name: by_schema
-- dialect: duckdb
WITH fk_tables AS (
    SELECT DISTINCT
        database_name,
        schema_name,
        table_name
    FROM duckdb_constraints()
    WHERE database_name = current_database()
      AND constraint_type = 'FOREIGN KEY'
),
ddl_objects AS (
    SELECT
        t.schema_name,
        t.table_name AS object_name,
        CASE WHEN t.temporary THEN 'temporary_table' ELSE 'table' END AS object_type,
        t.table_name AS table_name,
        t.sql,
        fk.table_name IS NOT NULL AS has_foreign_keys
    FROM duckdb_tables() AS t
    LEFT JOIN fk_tables AS fk
        ON fk.database_name = t.database_name
       AND fk.schema_name = t.schema_name
       AND fk.table_name = t.table_name
    WHERE NOT t.internal
      AND t.database_name = current_database()
      AND t.sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        view_name AS object_name,
        'view' AS object_type,
        NULL AS table_name,
        sql,
        FALSE AS has_foreign_keys
    FROM duckdb_views()
    WHERE NOT internal
      AND database_name = current_database()
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        index_name AS object_name,
        'index' AS object_type,
        table_name AS table_name,
        sql,
        FALSE AS has_foreign_keys
    FROM duckdb_indexes()
    WHERE database_name = current_database()
      AND sql IS NOT NULL
    UNION ALL
    SELECT
        schema_name,
        sequence_name AS object_name,
        'sequence' AS object_type,
        NULL AS table_name,
        'CREATE SEQUENCE '
            || CASE WHEN schema_name = 'main' THEN '' ELSE '"' || replace(schema_name, '"', '""') || '".' END
            || '"' || replace(sequence_name, '"', '""') || '"'
            || ' INCREMENT BY ' || increment_by
            || ' MINVALUE ' || min_value
            || ' MAXVALUE ' || max_value
            || ' START WITH ' || start_value
            || CASE WHEN cycle THEN ' CYCLE' ELSE ' NO CYCLE' END
            || ';' AS sql,
        FALSE AS has_foreign_keys
    FROM duckdb_sequences()
    WHERE database_name = current_database()
    UNION ALL
    SELECT
        schema_name,
        type_name AS object_name,
        'type' AS object_type,
        NULL AS table_name,
        CASE
            WHEN logical_type = 'ENUM' AND labels IS NOT NULL THEN
                'CREATE TYPE '
                    || CASE WHEN schema_name = 'main' THEN '' ELSE '"' || replace(schema_name, '"', '""') || '".' END
                    || '"' || replace(type_name, '"', '""') || '" AS ENUM ('
                    || array_to_string(
                        list_transform(labels, lambda x: '''' || replace(x, '''', '''''') || ''''),
                        ', '
                    ) || ');'
            ELSE NULL
        END AS sql,
        FALSE AS has_foreign_keys
    FROM duckdb_types()
    WHERE NOT internal
      AND database_name = current_database()
      AND logical_type = 'ENUM'
      AND labels IS NOT NULL
)
SELECT
    schema_name,
    object_name,
    object_type,
    table_name,
    sql AS ddl,
    has_foreign_keys,
    'native' AS fidelity
FROM ddl_objects
WHERE schema_name = COALESCE(:schema_name, current_schema())
  AND sql IS NOT NULL
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
