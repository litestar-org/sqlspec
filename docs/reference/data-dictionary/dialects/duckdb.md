# DuckDB Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("duckdb")
```

Introspects DuckDB table functions (`duckdb_tables()`, `duckdb_columns()`, `duckdb_indexes()`, `duckdb_constraints()`, `duckdb_views()`, `duckdb_sequences()`, `duckdb_types()`, `duckdb_extensions()`). See the [DuckDB Guide](../../../databases/duckdb.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `duckdb` |
| **Default Schema** | `None` |
| **Version Pattern** | `v?(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `duckdb_*().sql` catalog functions + AST synthesis |
| **Catalog `.sql` Files** | `17` files (`24` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `LIST` |
| `supports_cte` | `True` | `bigint` | `BIGINT` |
| `supports_for_update` | `False` | `blob` | `BLOB` |
| `supports_json` | `True` | `boolean` | `BOOLEAN` |
| `supports_maps` | `True` | `float` | `DOUBLE` |
| `supports_on_conflict` | `True` | `integer` | `INTEGER` |
| `supports_prepared_statements` | `True` | `json` | `JSON` |
| `supports_schemas` | `True` | `map` | `MAP` |
| `supports_skip_locked` | `False` | `struct` | `STRUCT` |
| `supports_structs` | `True` | `text` | `TEXT` |
| `supports_transactions` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_update_from` | `True` | `uuid` | `UUID` |
| `supports_uuid` | `True` | `varchar` | `TEXT` |
| `supports_window_functions` | `True` | - | - |
| `supports_returning` | `>= 0.8.0` | - | - |
| `supports_upsert` | `>= 0.8.0` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.duckdb.data_dictionary.DuckDBDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.adbc.data_dictionary.AdbcDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.arrow_odbc.data_dictionary.ArrowOdbcDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `duckdb` dialect pack defines `24` named queries across `17` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/duckdb/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `constraints.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `databases.sql` | `list` | *none* |
| `ddl.sql` | `by_object`, `by_schema` | `:object_name`, `:object_type`, `:schema_name` |
| `dependencies.sql` | `by_schema` | `:schema_name` |
| `extensions.sql` | `list` | *none* |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `functions.sql` | `by_schema` | `:schema_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `objects.sql` | `by_schema` | `:schema_name` |
| `schemas.sql` | `list` | *none* |
| `sequences.sql` | `by_schema` | `:schema_name` |
| `system.sql` | `logs`, `memory`, `settings` | *none* |
| `tables.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `types.sql` | `by_schema` | `:schema_name` |
| `version.sql` | `current` | *none* |
| `views.sql` | `by_schema` | `:schema_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
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
    ```

??? code "`constraints.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        table_name,
        constraint_name,
        constraint_type,
        constraint_text,
        expression,
        constraint_column_names,
        referenced_table,
        referenced_column_names
    FROM duckdb_constraints()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
    ORDER BY table_name, constraint_index;

    -- name: by_table
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        table_name,
        constraint_name,
        constraint_type,
        constraint_text,
        expression,
        constraint_column_names,
        referenced_table,
        referenced_column_names
    FROM duckdb_constraints()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
      AND table_name = :table_name
    ORDER BY constraint_index;
    ```

??? code "`databases.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: duckdb
    SELECT
        database_name,
        path,
        type,
        readonly,
        internal
    FROM duckdb_databases()
    WHERE NOT internal
    ORDER BY database_name;
    ```

??? code "`ddl.sql` (2 named queries)"

    ```sql
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
    ```

??? code "`dependencies.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    WITH object_inventory AS (
        SELECT
            database_name,
            schema_name,
            table_oid AS object_oid,
            table_name AS object_name,
            'table' AS object_type
        FROM duckdb_tables()
        WHERE schema_name = COALESCE(:schema_name, current_schema())
        UNION ALL
        SELECT
            database_name,
            schema_name,
            index_oid AS object_oid,
            index_name AS object_name,
            'index' AS object_type
        FROM duckdb_indexes()
        WHERE schema_name = COALESCE(:schema_name, current_schema())
        UNION ALL
        SELECT
            database_name,
            schema_name,
            view_oid AS object_oid,
            view_name AS object_name,
            'view' AS object_type
        FROM duckdb_views()
        WHERE schema_name = COALESCE(:schema_name, current_schema())
    )
    SELECT
        dependent.database_name,
        dependent.schema_name,
        dependent.object_name,
        dependent.object_type,
        referenced.object_name AS referenced_object_name,
        referenced.object_type AS referenced_object_type,
        dep.classid,
        dep.objid,
        dep.objsubid,
        dep.refclassid,
        dep.refobjid,
        dep.refobjsubid,
        dep.deptype
    FROM duckdb_dependencies() AS dep
    LEFT JOIN object_inventory AS dependent ON dependent.object_oid = dep.objid
    LEFT JOIN object_inventory AS referenced ON referenced.object_oid = dep.refobjid
    WHERE dependent.object_oid IS NOT NULL
       OR referenced.object_oid IS NOT NULL
    ORDER BY dependent.object_type, dependent.object_name, referenced.object_type, referenced.object_name;
    ```

??? code "`extensions.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: duckdb
    SELECT
        extension_name,
        loaded,
        installed,
        install_path,
        description,
        aliases,
        extension_version,
        install_mode,
        installed_from
    FROM duckdb_extensions()
    ORDER BY extension_name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: duckdb
    SELECT
        kcu.table_name,
        kcu.column_name,
        pk_kcu.table_name AS referenced_table_name,
        pk_kcu.column_name AS referenced_column_name,
        kcu.constraint_name,
        kcu.table_schema,
        pk_kcu.table_schema AS referenced_table_schema,
        rc.delete_rule,
        rc.update_rule
    FROM information_schema.key_column_usage kcu
    JOIN information_schema.referential_constraints rc
      ON kcu.constraint_name = rc.constraint_name
      AND kcu.constraint_schema = rc.constraint_schema
    JOIN information_schema.key_column_usage pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
      AND kcu.ordinal_position = pk_kcu.ordinal_position
    WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
      AND kcu.table_name = :table_name
    ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position;

    -- name: by_schema
    -- dialect: duckdb
    SELECT
        kcu.table_name,
        kcu.column_name,
        pk_kcu.table_name AS referenced_table_name,
        pk_kcu.column_name AS referenced_column_name,
        kcu.constraint_name,
        kcu.table_schema,
        pk_kcu.table_schema AS referenced_table_schema,
        rc.delete_rule,
        rc.update_rule
    FROM information_schema.key_column_usage kcu
    JOIN information_schema.referential_constraints rc
      ON kcu.constraint_name = rc.constraint_name
      AND kcu.constraint_schema = rc.constraint_schema
    JOIN information_schema.key_column_usage pk_kcu
      ON rc.unique_constraint_name = pk_kcu.constraint_name
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
      AND kcu.ordinal_position = pk_kcu.ordinal_position
    WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
    ORDER BY kcu.table_name, kcu.constraint_name, kcu.ordinal_position;
    ```

??? code "`functions.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        function_name,
        function_type,
        return_type,
        parameters,
        parameter_types,
        macro_definition,
        has_side_effects,
        internal,
        comment,
        tags
    FROM duckdb_functions()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
    ORDER BY function_type, function_name;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        table_name,
        index_name,
        is_unique,
        is_primary,
        expressions AS columns,
        comment,
        tags,
        sql AS native_sql
    FROM duckdb_indexes()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
    ORDER BY table_name, index_name;

    -- name: by_table
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        table_name,
        index_name,
        is_unique,
        is_primary,
        expressions AS columns,
        comment,
        tags,
        sql AS native_sql
    FROM duckdb_indexes()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
      AND table_name = :table_name
    ORDER BY index_name;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        table_name AS object_name,
        'table' AS object_type,
        comment,
        tags,
        internal,
        temporary,
        sql AS native_sql
    FROM duckdb_tables()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
      AND NOT internal
    UNION ALL
    SELECT
        database_name,
        schema_name,
        view_name AS object_name,
        'view' AS object_type,
        comment,
        tags,
        internal,
        temporary,
        sql AS native_sql
    FROM duckdb_views()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
      AND NOT internal
    ORDER BY object_type, object_name;
    ```

??? code "`schemas.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        internal,
        sql AS native_sql
    FROM duckdb_schemas()
    WHERE NOT internal
    ORDER BY database_name, schema_name;
    ```

??? code "`sequences.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        sequence_name,
        temporary,
        start_value,
        min_value,
        max_value,
        increment_by,
        cycle,
        last_value,
        comment,
        tags,
        sql AS native_sql
    FROM duckdb_sequences()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
    ORDER BY sequence_name;
    ```

??? code "`system.sql` (3 named queries)"

    ```sql
    -- name: logs
    -- dialect: duckdb
    SELECT *
    FROM duckdb_logs();

    -- name: memory
    -- dialect: duckdb
    SELECT *
    FROM duckdb_memory();

    -- name: settings
    -- dialect: duckdb
    SELECT
        name,
        value,
        description,
        input_type,
        scope,
        aliases
    FROM duckdb_settings()
    ORDER BY name;
    ```

??? code "`tables.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    WITH RECURSIVE dependency_tree AS (
        SELECT
            t.table_name,
            0 AS level,
            [t.table_name] AS path
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema = COALESCE(:schema_name, current_schema())
          AND NOT EXISTS (
              SELECT 1
              FROM information_schema.key_column_usage kcu
              JOIN information_schema.referential_constraints rc
                ON kcu.constraint_name = rc.constraint_name
                AND kcu.constraint_schema = rc.constraint_schema
              JOIN information_schema.key_column_usage pk_kcu
                ON rc.unique_constraint_name = pk_kcu.constraint_name
                AND rc.unique_constraint_schema = pk_kcu.constraint_schema
              WHERE kcu.table_name = t.table_name
                AND kcu.table_schema = t.table_schema
                AND pk_kcu.table_name <> t.table_name
          )

        UNION ALL

        SELECT
            kcu.table_name,
            dt.level + 1,
            list_append(dt.path, kcu.table_name)
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc
          ON kcu.constraint_name = rc.constraint_name
          AND kcu.constraint_schema = rc.constraint_schema
        JOIN information_schema.key_column_usage pk_kcu
          ON rc.unique_constraint_name = pk_kcu.constraint_name
          AND rc.unique_constraint_schema = pk_kcu.constraint_schema
        JOIN dependency_tree dt ON dt.table_name = pk_kcu.table_name
        WHERE kcu.table_schema = COALESCE(:schema_name, current_schema())
          AND pk_kcu.table_name <> kcu.table_name
          AND NOT list_contains(dt.path, kcu.table_name)
    ),
    dependency_levels AS (
        SELECT
            table_name,
            MAX(level) AS dependency_level
        FROM dependency_tree
        GROUP BY table_name
    )
    SELECT
        t.table_name,
        COALESCE(dl.dependency_level, 0) AS level,
        COALESCE(dl.dependency_level, 0) AS dependency_level
    FROM information_schema.tables t
    LEFT JOIN dependency_levels dl ON dl.table_name = t.table_name
    WHERE t.table_type = 'BASE TABLE'
      AND t.table_schema = COALESCE(:schema_name, current_schema())
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY level, t.table_name;
    ```

??? code "`types.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        type_name,
        logical_type,
        labels
    FROM duckdb_types()
    WHERE NOT internal
      AND schema_name = COALESCE(:schema_name, current_schema())
    ORDER BY type_name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: duckdb
    SELECT version() AS version;
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: duckdb
    SELECT
        database_name,
        schema_name,
        view_name,
        comment,
        tags,
        internal,
        temporary,
        sql AS native_sql
    FROM duckdb_views()
    WHERE schema_name = COALESCE(:schema_name, current_schema())
      AND NOT internal
    ORDER BY view_name;
    ```
