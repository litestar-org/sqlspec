# MariaDB Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("mariadb")
```

Extends MySQL `information_schema` catalog queries with MariaDB native `SEQUENCE` objects (`TABLE_TYPE = 'SEQUENCE'`), native `UUID` / `VECTOR` types, and MariaDB version detection. See the [MariaDB Guide](../../../databases/mysql.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `mariadb` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `SHOW CREATE` + MariaDB sequence & constraint reflection |
| **Catalog `.sql` Files** | `17` files (`27` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `BIGINT` |
| `supports_for_update` | `True` | `blob` | `BLOB` |
| `supports_invisible_columns` | `False` | `boolean` | `TINYINT(1)` |
| `supports_invisible_indexes` | `False` | `float` | `DOUBLE` |
| `supports_on_conflict` | `False` | `integer` | `INT` |
| `supports_prepared_statements` | `True` | `json` | `JSON` |
| `supports_resource_groups` | `False` | `text` | `TEXT` |
| `supports_returning` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_schemas` | `True` | `uuid` | `VARCHAR(36)` |
| `supports_transactions` | `True` | `varchar` | `VARCHAR({length})` |
| `supports_update_from` | `False` | - | - |
| `supports_upsert` | `True` | - | - |
| `supports_uuid` | `False` | - | - |
| `supports_check_constraints` | `>= 10.2.1` | - | - |
| `supports_cte` | `>= 10.2.1` | - | - |
| `supports_events` | `>= 5.1.6` | - | - |
| `supports_generated_columns` | `>= 10.2.0` | - | - |
| `supports_json` | `>= 10.2.7` | - | - |
| `supports_roles` | `>= 10.0.5` | - | - |
| `supports_sequences` | `>= 10.3.0` | - | - |
| `supports_skip_locked` | `>= 10.6.0` | - | - |
| `supports_system_versioned_tables` | `>= 10.3.0` | - | - |
| `supports_window_functions` | `>= 10.2.0` | - | - |

## Catalog SQL Queries

The `mariadb` dialect pack defines `27` named queries across `17` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/mariadb/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `constraints.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `ddl.sql` | `event_by_name`, `function_by_name`, `procedure_by_name`, `sequence_by_name`, `table_by_name`, `trigger_by_name`, `view_by_name` | *none* |
| `events.sql` | `by_schema` | `:schema_name` |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `objects.sql` | `by_schema` | `:schema_name` |
| `partitions.sql` | `by_schema` | `:schema_name` |
| `plugins.sql` | `list` | *none* |
| `privileges.sql` | `by_schema` | `:schema_name` |
| `routines.sql` | `by_schema` | `:schema_name` |
| `schemas.sql` | `list` | `:schema_name` |
| `system.sql` | `performance_schema_tables` | *none* |
| `tables.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `triggers.sql` | `by_schema` | `:schema_name` |
| `version.sql` | `current`, `version` | *none* |
| `views.sql` | `by_schema` | `:schema_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        table_schema AS schema_name,
        table_name,
        column_name,
        ordinal_position,
        column_default,
        is_nullable,
        data_type,
        column_type,
        character_maximum_length AS max_length,
        numeric_precision,
        numeric_scale,
        character_set_name,
        collation_name,
        column_key,
        column_key = 'PRI' AS is_primary,
        extra,
        extra LIKE '%auto_increment%' AS is_identity,
        generation_expression,
        CASE WHEN generation_expression IS NOT NULL AND generation_expression <> '' THEN 1 ELSE 0 END AS is_generated,
        column_comment
    FROM information_schema.columns
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY table_schema, table_name, ordinal_position;

    -- name: by_table
    -- dialect: mysql
    SELECT
        column_name AS `column_name`,
        data_type AS `data_type`,
        column_type AS `column_type`,
        is_nullable AS `is_nullable`,
        column_default AS `column_default`,
        column_key AS `column_key`,
        column_key = 'PRI' AS `is_primary`,
        extra AS `extra`,
        extra LIKE '%auto_increment%' AS `is_identity`,
        generation_expression AS `generation_expression`,
        CASE WHEN generation_expression IS NOT NULL AND generation_expression <> '' THEN 1 ELSE 0 END AS `is_generated`
    FROM information_schema.columns
    WHERE table_name = :table_name
      AND table_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY ordinal_position;
    ```

??? code "`constraints.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        tc.constraint_schema AS schema_name,
        tc.table_name,
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        kcu.ordinal_position,
        kcu.referenced_table_schema,
        kcu.referenced_table_name,
        kcu.referenced_column_name,
        rc.update_rule,
        rc.delete_rule,
        cc.check_clause
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_schema = tc.constraint_schema
     AND kcu.constraint_name = tc.constraint_name
     AND kcu.table_schema = tc.table_schema
     AND kcu.table_name = tc.table_name
    LEFT JOIN information_schema.referential_constraints rc
      ON rc.constraint_schema = tc.constraint_schema
     AND rc.constraint_name = tc.constraint_name
    LEFT JOIN information_schema.check_constraints cc
      ON cc.constraint_schema = tc.constraint_schema
     AND cc.constraint_name = tc.constraint_name
    WHERE tc.table_schema = COALESCE(:schema_name, DATABASE())
      AND (:table_name IS NULL OR tc.table_name = :table_name)
    ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;
    ```

??? code "`ddl.sql` (7 named queries)"

    ```sql
    -- name: event_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE EVENT' AS statement_template;

    -- name: function_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE FUNCTION' AS statement_template;

    -- name: procedure_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE PROCEDURE' AS statement_template;

    -- name: sequence_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE SEQUENCE' AS statement_template;

    -- name: table_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE TABLE' AS statement_template;

    -- name: trigger_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE TRIGGER' AS statement_template;

    -- name: view_by_name
    -- dialect: mysql
    SELECT 'SHOW CREATE VIEW' AS statement_template;
    ```

??? code "`events.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        event_schema AS schema_name,
        event_name,
        definer,
        time_zone,
        event_body,
        event_definition,
        event_type,
        execute_at,
        interval_value,
        interval_field,
        sql_mode,
        starts,
        ends,
        status,
        on_completion,
        created,
        last_altered,
        last_executed,
        event_comment,
        originator,
        character_set_client,
        collation_connection,
        database_collation
    FROM information_schema.events
    WHERE event_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY event_schema, event_name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: mariadb
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
    -- dialect: mariadb
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
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        table_schema AS schema_name,
        table_name,
        index_name,
        non_unique = 0 AS is_unique,
        index_name = 'PRIMARY' AS is_primary,
        index_type,
        ignored,
        GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns,
        GROUP_CONCAT(sub_part ORDER BY seq_in_index) AS prefix_lengths,
        GROUP_CONCAT(collation ORDER BY seq_in_index) AS column_orders,
        index_comment
    FROM information_schema.statistics
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
      AND (:table_name IS NULL OR table_name = :table_name)
    GROUP BY table_schema, table_name, index_name, non_unique, index_type, ignored, index_comment
    ORDER BY table_schema, table_name, index_name;

    -- name: by_table
    -- dialect: mysql
    SELECT
        s.index_name AS index_name,
        s.table_name AS table_name,
        CASE WHEN s.non_unique = 0 THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN s.index_name = 'PRIMARY' THEN 1 ELSE 0 END AS is_primary,
        GROUP_CONCAT(s.column_name ORDER BY s.seq_in_index) AS columns
    FROM information_schema.statistics s
    WHERE s.table_schema = COALESCE(:schema_name, DATABASE())
      AND s.table_name = :table_name
    GROUP BY s.index_name, s.table_name, s.non_unique
    ORDER BY s.index_name;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        table_schema AS object_schema,
        table_name AS object_name,
        CASE
            WHEN table_type = 'SEQUENCE' THEN 'SEQUENCE'
            ELSE table_type
        END AS object_type,
        engine,
        table_comment AS comment
    FROM information_schema.tables
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
    UNION ALL
    SELECT
        sequence_schema AS object_schema,
        sequence_name AS object_name,
        'SEQUENCE' AS object_type,
        NULL AS engine,
        NULL AS comment
    FROM information_schema.sequences
    WHERE sequence_schema = COALESCE(:schema_name, DATABASE())
    UNION ALL
    SELECT
        routine_schema AS object_schema,
        routine_name AS object_name,
        routine_type AS object_type,
        NULL AS engine,
        routine_comment AS comment
    FROM information_schema.routines
    WHERE routine_schema = COALESCE(:schema_name, DATABASE())
    UNION ALL
    SELECT
        trigger_schema AS object_schema,
        trigger_name AS object_name,
        'TRIGGER' AS object_type,
        NULL AS engine,
        NULL AS comment
    FROM information_schema.triggers
    WHERE trigger_schema = COALESCE(:schema_name, DATABASE())
    UNION ALL
    SELECT
        event_schema AS object_schema,
        event_name AS object_name,
        'EVENT' AS object_type,
        NULL AS engine,
        event_comment AS comment
    FROM information_schema.events
    WHERE event_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY object_schema, object_name, object_type;
    ```

??? code "`partitions.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        table_schema AS schema_name,
        table_name,
        partition_name,
        subpartition_name,
        partition_ordinal_position,
        subpartition_ordinal_position,
        partition_method,
        subpartition_method,
        partition_expression,
        subpartition_expression,
        partition_description,
        table_rows,
        avg_row_length,
        data_length,
        index_length,
        create_time,
        update_time,
        check_time,
        check_sum,
        partition_comment
    FROM information_schema.partitions
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
      AND partition_name IS NOT NULL
    ORDER BY table_schema, table_name, partition_ordinal_position, subpartition_ordinal_position;
    ```

??? code "`plugins.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: mysql
    SELECT
        plugin_name,
        plugin_version,
        plugin_status,
        plugin_type,
        plugin_type_version,
        plugin_library,
        plugin_library_version,
        plugin_author,
        plugin_description,
        plugin_license,
        load_option
    FROM information_schema.plugins
    ORDER BY plugin_type, plugin_name;
    ```

??? code "`privileges.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        grantee,
        table_catalog,
        table_schema AS schema_name,
        table_name,
        privilege_type,
        is_grantable,
        NULL AS column_name
    FROM information_schema.table_privileges
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
    UNION ALL
    SELECT
        grantee,
        table_catalog,
        table_schema AS schema_name,
        table_name,
        privilege_type,
        is_grantable,
        column_name
    FROM information_schema.column_privileges
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY schema_name, table_name, column_name, grantee, privilege_type;
    ```

??? code "`routines.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        routine_schema AS schema_name,
        routine_name,
        routine_type,
        data_type,
        routine_definition,
        external_language,
        is_deterministic,
        sql_data_access,
        security_type,
        created,
        last_altered,
        definer,
        character_set_client,
        collation_connection,
        database_collation
    FROM information_schema.routines
    WHERE routine_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY routine_schema, routine_name;
    ```

??? code "`schemas.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: mysql
    SELECT
        schema_name,
        default_character_set_name,
        default_collation_name,
        sql_path
    FROM information_schema.schemata
    WHERE :schema_name IS NULL OR schema_name = :schema_name
    ORDER BY schema_name;
    ```

??? code "`system.sql` (1 named queries)"

    ```sql
    -- name: performance_schema_tables
    -- dialect: mysql
    SELECT
        table_schema,
        table_name,
        table_rows,
        avg_row_length,
        data_length,
        index_length
    FROM information_schema.tables
    WHERE table_schema = 'performance_schema'
    ORDER BY table_name;
    ```

??? code "`tables.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    WITH RECURSIVE dependency_tree AS (
        SELECT
            table_name,
            0 AS level,
            CAST(table_name AS CHAR(4000)) AS path
        FROM information_schema.tables AS root_table
        WHERE root_table.table_type = 'BASE TABLE'
          AND root_table.table_schema = COALESCE(:schema_name, DATABASE())
          AND NOT EXISTS (
              SELECT 1
              FROM information_schema.key_column_usage AS root_key
              WHERE root_key.table_name = root_table.table_name
                AND root_key.table_schema = root_table.table_schema
                AND root_key.referenced_table_name IS NOT NULL
                AND root_key.referenced_table_name <> root_table.table_name
          )

        UNION ALL

        SELECT
            child_key.table_name,
            dependency_tree.level + 1,
            CONCAT(dependency_tree.path, ',', child_key.table_name)
        FROM information_schema.key_column_usage AS child_key
        JOIN dependency_tree ON child_key.referenced_table_name = dependency_tree.table_name
        WHERE child_key.table_schema = COALESCE(:schema_name, DATABASE())
          AND child_key.referenced_table_name IS NOT NULL
          AND child_key.referenced_table_name <> child_key.table_name
          AND NOT FIND_IN_SET(child_key.table_name, dependency_tree.path)
    ), dependency_levels AS (
        SELECT table_name, MAX(level) AS dependency_level
        FROM dependency_tree
        GROUP BY table_name
    )
    SELECT
        catalog_table.table_schema AS schema_name,
        CAST(catalog_table.table_name AS CHAR) AS table_name,
        catalog_table.table_type AS table_type,
        catalog_table.engine AS engine,
        catalog_table.version AS version,
        catalog_table.row_format AS row_format,
        catalog_table.table_rows AS table_rows,
        catalog_table.avg_row_length AS avg_row_length,
        catalog_table.data_length AS data_length,
        catalog_table.max_data_length AS max_data_length,
        catalog_table.index_length AS index_length,
        catalog_table.data_free AS data_free,
        catalog_table.auto_increment AS auto_increment,
        catalog_table.create_time AS create_time,
        catalog_table.update_time AS update_time,
        catalog_table.check_time AS check_time,
        catalog_table.table_collation AS table_collation,
        catalog_table.checksum AS checksum,
        catalog_table.create_options AS create_options,
        catalog_table.table_comment AS table_comment,
        COALESCE(dependency_levels.dependency_level, 0) AS dependency_level,
        COALESCE(dependency_levels.dependency_level, 0) AS level
    FROM information_schema.tables AS catalog_table
    LEFT JOIN dependency_levels ON dependency_levels.table_name = catalog_table.table_name
    WHERE catalog_table.table_schema = COALESCE(:schema_name, DATABASE())
      AND (:table_name IS NULL OR catalog_table.table_name = :table_name)
    ORDER BY dependency_level, catalog_table.table_name;
    ```

??? code "`triggers.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        trigger_schema AS schema_name,
        trigger_name,
        event_manipulation,
        event_object_schema,
        event_object_table,
        action_order,
        action_condition,
        action_statement,
        action_orientation,
        action_timing,
        sql_mode,
        definer,
        character_set_client,
        collation_connection,
        database_collation
    FROM information_schema.triggers
    WHERE trigger_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY trigger_schema, event_object_table, action_timing, event_manipulation, action_order;
    ```

??? code "`version.sql` (2 named queries)"

    ```sql
    -- name: current
    -- dialect: mysql
    SELECT VERSION() AS version;

    -- name: version
    -- dialect: mysql
    SELECT VERSION() AS version;
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mysql
    SELECT
        table_schema AS schema_name,
        table_name AS view_name,
        view_definition,
        check_option,
        is_updatable,
        definer,
        security_type,
        character_set_client,
        collation_connection
    FROM information_schema.views
    WHERE table_schema = COALESCE(:schema_name, DATABASE())
    ORDER BY table_schema, table_name;
    ```
