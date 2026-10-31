# SQL Server Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("mssql")
```

Introspects `sys.tables`, `sys.columns`, `sys.types`, `sys.indexes`, `sys.foreign_keys`, `sys.check_constraints`, `sys.default_constraints`, `sys.sequences`, and `sys.sql_modules` across `mssql_python` and `pymssql`. See the [SQL Server Guide](../../../databases/mssql.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `mssql` |
| **Default Schema** | `"dbo"` |
| **Version Pattern** | `(\d+)` |
| **DDL Strategy** | AST synthesis + `OBJECT_DEFINITION()` for views/routines/triggers |
| **Catalog `.sql` Files** | `19` files (`31` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_for_update` | `False` | `bigint` | `BIGINT` |
| `supports_in_memory` | `True` | `blob` | `VARBINARY(MAX)` |
| `supports_on_conflict` | `False` | `boolean` | `BIT` |
| `supports_prepared_statements` | `True` | `bytea` | `VARBINARY(MAX)` |
| `supports_schemas` | `True` | `float` | `FLOAT` |
| `supports_skip_locked` | `False` | `integer` | `INT` |
| `supports_transactions` | `True` | `json` | `NVARCHAR(MAX)` |
| `supports_update_from` | `True` | `jsonb` | `NVARCHAR(MAX)` |
| - | - | `text` | `NVARCHAR(MAX)` |
| - | - | `timestamp` | `DATETIME2(6)` |
| - | - | `timestamptz` | `DATETIMEOFFSET(6)` |
| - | - | `uuid` | `UNIQUEIDENTIFIER` |
| - | - | `varchar` | `NVARCHAR({length})` |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.mssql_python.data_dictionary.MssqlPythonSyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.pymssql.data_dictionary.PymssqlSyncDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `mssql` dialect pack defines `31` named queries across `19` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/mssql/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `constraints.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `ddl.sql` | `index_inputs_by_table`, `table_inputs_by_table` | `:ddl_index_inputs`, `:ddl_table_inputs`, `:mssql`, `:schema_name`, `:table_name` |
| `dependencies.sql` | `by_schema` | `:schema_name` |
| `extended_properties.sql` | `by_schema` | `:schema_name` |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `modules.sql` | `by_schema` | `:schema_name` |
| `objects.sql` | `by_schema` | `:schema_name` |
| `permissions.sql` | `by_schema`, `role_members` | `:schema_name` |
| `routines.sql` | `by_schema`, `parameters_by_schema` | `:schema_name` |
| `schemas.sql` | `list`, `current` | *none* |
| `sequences.sql` | `by_schema` | `:schema_name` |
| `storage.sql` | `by_schema` | `:schema_name` |
| `system.sql` | `dmv_exec_requests`, `query_store_runtime` | `:dmv_exec_requests`, `:mssql`, `:query_store_runtime`, `@SPID` |
| `tables.sql` | `all_by_schema`, `by_schema`, `details_by_table`, `names_by_schema` | `:schema_name`, `:table_name` |
| `triggers.sql` | `by_schema` | `:schema_name` |
| `version.sql` | `current` | `@VERSION` |
| `views.sql` | `by_schema` | `:schema_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    WITH pk_cols AS (
        SELECT DISTINCT
            idx_col.object_id,
            idx_col.column_id
        FROM sys.indexes AS idx
        INNER JOIN sys.index_columns AS idx_col
          ON idx.object_id = idx_col.object_id
         AND idx.index_id = idx_col.index_id
        WHERE idx.is_primary_key = 1
    ),
    unique_cols AS (
        SELECT DISTINCT
            idx_col.object_id,
            idx_col.column_id
        FROM sys.indexes AS idx
        INNER JOIN sys.index_columns AS idx_col
          ON idx.object_id = idx_col.object_id
         AND idx.index_id = idx_col.index_id
        WHERE idx.is_unique = 1
    )
    SELECT
        s.name AS schema_name,
        tab.name AS table_name,
        c.column_id AS ordinal_position,
        c.name AS column_name,
        typ.name AS data_type,
        CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
        OBJECT_DEFINITION(c.default_object_id) AS column_default,
        c.max_length,
        c.precision AS numeric_precision,
        c.scale AS numeric_scale,
        CAST(CASE WHEN pk_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_primary,
        CAST(CASE WHEN unique_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_unique,
        c.collation_name,
        CAST(c.is_sparse AS BIT) AS is_sparse,
        CAST(c.is_column_set AS BIT) AS is_column_set,
        CAST(c.is_hidden AS BIT) AS is_hidden,
        CAST(c.is_masked AS BIT) AS is_masked,
        c.encryption_type_desc,
        c.encryption_algorithm_name,
        CAST(CASE WHEN cc.object_id IS NOT NULL OR c.generated_always_type <> 0 THEN 1 ELSE 0 END AS BIT) AS is_generated,
        cc.definition AS generation_expression,
        CAST(cc.is_persisted AS BIT) AS is_persisted_computed,
        CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
        CASE WHEN ic.object_id IS NOT NULL THEN 'ALWAYS' ELSE NULL END AS identity_generation,
        CONVERT(BIGINT, ic.seed_value) AS identity_seed,
        CONVERT(BIGINT, ic.increment_value) AS identity_increment
    FROM sys.columns AS c
    INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
    INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
    INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
    LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
    LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    LEFT JOIN pk_cols ON pk_cols.object_id = tab.object_id AND pk_cols.column_id = c.column_id
    LEFT JOIN unique_cols ON unique_cols.object_id = tab.object_id AND unique_cols.column_id = c.column_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, tab.name, c.column_id;

    -- name: by_table
    -- dialect: mssql
    WITH pk_cols AS (
        SELECT DISTINCT
            idx_col.object_id,
            idx_col.column_id
        FROM sys.indexes AS idx
        INNER JOIN sys.index_columns AS idx_col
          ON idx.object_id = idx_col.object_id
         AND idx.index_id = idx_col.index_id
        WHERE idx.is_primary_key = 1
    ),
    unique_cols AS (
        SELECT DISTINCT
            idx_col.object_id,
            idx_col.column_id
        FROM sys.indexes AS idx
        INNER JOIN sys.index_columns AS idx_col
          ON idx.object_id = idx_col.object_id
         AND idx.index_id = idx_col.index_id
        WHERE idx.is_unique = 1
    )
    SELECT
        s.name AS schema_name,
        tab.name AS table_name,
        c.column_id AS ordinal_position,
        c.name AS column_name,
        typ.name AS data_type,
        CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
        OBJECT_DEFINITION(c.default_object_id) AS column_default,
        c.max_length,
        c.precision AS numeric_precision,
        c.scale AS numeric_scale,
        CAST(CASE WHEN pk_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_primary,
        CAST(CASE WHEN unique_cols.column_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_unique,
        c.collation_name,
        CAST(c.is_sparse AS BIT) AS is_sparse,
        CAST(c.is_column_set AS BIT) AS is_column_set,
        CAST(c.is_hidden AS BIT) AS is_hidden,
        CAST(c.is_masked AS BIT) AS is_masked,
        c.encryption_type_desc,
        c.encryption_algorithm_name,
        CAST(CASE WHEN cc.object_id IS NOT NULL OR c.generated_always_type <> 0 THEN 1 ELSE 0 END AS BIT) AS is_generated,
        cc.definition AS generation_expression,
        CAST(cc.is_persisted AS BIT) AS is_persisted_computed,
        CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
        CASE WHEN ic.object_id IS NOT NULL THEN 'ALWAYS' ELSE NULL END AS identity_generation,
        CONVERT(BIGINT, ic.seed_value) AS identity_seed,
        CONVERT(BIGINT, ic.increment_value) AS identity_increment
    FROM sys.columns AS c
    INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
    INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
    INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
    LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
    LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    LEFT JOIN pk_cols ON pk_cols.object_id = tab.object_id AND pk_cols.column_id = c.column_id
    LEFT JOIN unique_cols ON unique_cols.object_id = tab.object_id AND unique_cols.column_id = c.column_id
    WHERE s.name = COALESCE(:schema_name, SCHEMA_NAME())
      AND tab.name = :table_name
    ORDER BY c.column_id;
    ```

??? code "`constraints.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        kc.name AS constraint_name,
        kc.type_desc AS constraint_type,
        CAST(kc.is_system_named AS BIT) AS is_system_named,
        STUFF((
            SELECT ',' + c2.name
            FROM sys.index_columns AS ic2
            INNER JOIN sys.columns AS c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
            WHERE ic2.object_id = kc.parent_object_id
              AND ic2.index_id = kc.unique_index_id
              AND ic2.is_included_column = 0
            ORDER BY ic2.key_ordinal
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS column_name,
        NULL AS definition
    FROM sys.key_constraints AS kc
    INNER JOIN sys.tables AS t ON kc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    UNION ALL
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        cc.name AS constraint_name,
        'CHECK_CONSTRAINT' AS constraint_type,
        CAST(cc.is_system_named AS BIT) AS is_system_named,
        NULL AS column_name,
        cc.definition
    FROM sys.check_constraints AS cc
    INNER JOIN sys.tables AS t ON cc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    UNION ALL
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        dc.name AS constraint_name,
        'DEFAULT_CONSTRAINT' AS constraint_type,
        CAST(dc.is_system_named AS BIT) AS is_system_named,
        c.name AS column_name,
        dc.definition
    FROM sys.default_constraints AS dc
    INNER JOIN sys.tables AS t ON dc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    INNER JOIN sys.columns AS c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY schema_name, table_name, constraint_name;

    -- name: by_table
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        kc.name AS constraint_name,
        kc.type_desc AS constraint_type,
        CAST(kc.is_system_named AS BIT) AS is_system_named,
        STUFF((
            SELECT ',' + c2.name
            FROM sys.index_columns AS ic2
            INNER JOIN sys.columns AS c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
            WHERE ic2.object_id = kc.parent_object_id
              AND ic2.index_id = kc.unique_index_id
              AND ic2.is_included_column = 0
            ORDER BY ic2.key_ordinal
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS column_name,
        NULL AS definition
    FROM sys.key_constraints AS kc
    INNER JOIN sys.tables AS t ON kc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE s.name = :schema_name
      AND t.name = :table_name
    UNION ALL
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        cc.name AS constraint_name,
        'CHECK_CONSTRAINT' AS constraint_type,
        CAST(cc.is_system_named AS BIT) AS is_system_named,
        NULL AS column_name,
        cc.definition
    FROM sys.check_constraints AS cc
    INNER JOIN sys.tables AS t ON cc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE s.name = :schema_name
      AND t.name = :table_name
    UNION ALL
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        dc.name AS constraint_name,
        'DEFAULT_CONSTRAINT' AS constraint_type,
        CAST(dc.is_system_named AS BIT) AS is_system_named,
        c.name AS column_name,
        dc.definition
    FROM sys.default_constraints AS dc
    INNER JOIN sys.tables AS t ON dc.parent_object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    INNER JOIN sys.columns AS c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
    WHERE s.name = :schema_name
      AND t.name = :table_name
    ORDER BY schema_name, table_name, constraint_name;
    ```

??? code "`ddl.sql` (2 named queries)"

    ```sql
    -- name: index_inputs_by_table
    -- dialect: mssql
    /* sqlspec:mssql:ddl_index_inputs */
    WITH index_key_columns AS (
        SELECT
            ic.object_id,
            ic.index_id,
            STRING_AGG(CAST(c.name AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
        FROM sys.index_columns AS ic
        INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.is_included_column = 0
        GROUP BY ic.object_id, ic.index_id
    ),
    index_included_columns AS (
        SELECT
            ic.object_id,
            ic.index_id,
            STRING_AGG(CAST(c.name AS NVARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY ic.index_column_id) AS included_columns
        FROM sys.index_columns AS ic
        INNER JOIN sys.columns AS c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE ic.is_included_column = 1
        GROUP BY ic.object_id, ic.index_id
    )
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        i.name AS index_name,
        i.type_desc,
        CAST(i.is_unique AS BIT) AS is_unique,
        CAST(i.has_filter AS BIT) AS has_filter,
        i.filter_definition,
        kc.columns AS columns,
        inc.included_columns AS included_columns
    FROM sys.indexes AS i
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN index_key_columns AS kc ON kc.object_id = i.object_id AND kc.index_id = i.index_id
    LEFT JOIN index_included_columns AS inc ON inc.object_id = i.object_id AND inc.index_id = i.index_id
    WHERE s.name = :schema_name
      AND t.name = :table_name
      AND i.name IS NOT NULL
      AND i.is_primary_key = 0
      AND i.is_unique_constraint = 0
    ORDER BY i.name;

    -- name: table_inputs_by_table
    -- dialect: mssql
    /* sqlspec:mssql:ddl_table_inputs */
    WITH primary_key_columns AS (
        SELECT
            pk_col.object_id,
            pk_col.column_id,
            pk.name AS primary_key_name,
            pk_col.key_ordinal AS primary_key_ordinal
        FROM sys.indexes AS pk
        INNER JOIN sys.index_columns AS pk_col
            ON pk.object_id = pk_col.object_id
           AND pk.index_id = pk_col.index_id
        WHERE pk.is_primary_key = 1
          AND pk_col.is_included_column = 0
    )
    SELECT
        s.name AS schema_name,
        tab.name AS table_name,
        c.name AS column_name,
        typ.name AS data_type,
        c.max_length,
        c.precision AS numeric_precision,
        c.scale AS numeric_scale,
        CAST(c.is_nullable AS BIT) AS is_nullable,
        c.column_id AS ordinal_position,
        OBJECT_DEFINITION(c.default_object_id) AS column_default,
        CAST(CASE WHEN cc.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_computed,
        cc.definition AS computed_definition,
        CAST(CASE WHEN ic.object_id IS NULL THEN 0 ELSE 1 END AS BIT) AS is_identity,
        CONVERT(BIGINT, ic.seed_value) AS identity_seed,
        CONVERT(BIGINT, ic.increment_value) AS identity_increment,
        pk.primary_key_name AS primary_key_name,
        pk.primary_key_ordinal AS primary_key_ordinal
    FROM sys.columns AS c
    INNER JOIN sys.types AS typ ON c.user_type_id = typ.user_type_id
    INNER JOIN sys.tables AS tab ON c.object_id = tab.object_id
    INNER JOIN sys.schemas AS s ON tab.schema_id = s.schema_id
    LEFT JOIN sys.computed_columns AS cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
    LEFT JOIN sys.identity_columns AS ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    LEFT JOIN primary_key_columns AS pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
    WHERE s.name = :schema_name
      AND tab.name = :table_name
    ORDER BY c.column_id;
    ```

??? code "`dependencies.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        OBJECT_SCHEMA_NAME(dep.referencing_id) AS referencing_schema_name,
        OBJECT_NAME(dep.referencing_id) AS referencing_object_name,
        dep.referencing_class_desc,
        dep.referenced_server_name,
        dep.referenced_database_name,
        dep.referenced_schema_name,
        dep.referenced_entity_name,
        dep.referenced_class_desc,
        CAST(dep.is_schema_bound_reference AS BIT) AS is_schema_bound_reference,
        CAST(dep.is_caller_dependent AS BIT) AS is_caller_dependent,
        CAST(dep.is_ambiguous AS BIT) AS is_ambiguous
    FROM sys.sql_expression_dependencies AS dep
    WHERE (:schema_name IS NULL OR OBJECT_SCHEMA_NAME(dep.referencing_id) = :schema_name)
    ORDER BY OBJECT_SCHEMA_NAME(dep.referencing_id), OBJECT_NAME(dep.referencing_id), dep.referenced_entity_name;
    ```

??? code "`extended_properties.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        o.name AS object_name,
        o.type_desc AS object_type,
        c.name AS column_name,
        ep.name AS property_name,
        CONVERT(NVARCHAR(MAX), ep.value) AS property_value,
        ep.class_desc
    FROM sys.extended_properties AS ep
    LEFT JOIN sys.objects AS o ON ep.major_id = o.object_id
    LEFT JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    LEFT JOIN sys.columns AS c ON ep.major_id = c.object_id AND ep.minor_id = c.column_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, o.name, c.name, ep.name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: mssql
    SELECT
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS table_schema,
        fk.name AS constraint_name,
        OBJECT_NAME(fk.parent_object_id) AS table_name,
        cpa.name AS column_name,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
        OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
        cref.name AS referenced_column
    FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns cpa
        ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
    INNER JOIN sys.columns cref
        ON fkc.referenced_object_id = cref.object_id AND fkc.referenced_column_id = cref.column_id
    WHERE OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name
      AND OBJECT_NAME(fk.parent_object_id) = :table_name
    ORDER BY fk.name, fkc.constraint_column_id;

    -- name: by_schema
    -- dialect: mssql
    SELECT
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS schema_name,
        OBJECT_SCHEMA_NAME(fk.parent_object_id) AS table_schema,
        fk.name AS constraint_name,
        OBJECT_NAME(fk.parent_object_id) AS table_name,
        cpa.name AS column_name,
        OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS referenced_schema,
        OBJECT_NAME(fk.referenced_object_id) AS referenced_table,
        cref.name AS referenced_column
    FROM sys.foreign_keys fk
    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN sys.columns cpa
        ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
    INNER JOIN sys.columns cref
        ON fkc.referenced_object_id = cref.object_id AND fkc.referenced_column_id = cref.column_id
    WHERE (:schema_name IS NULL OR OBJECT_SCHEMA_NAME(fk.parent_object_id) = :schema_name)
    ORDER BY OBJECT_SCHEMA_NAME(fk.parent_object_id), OBJECT_NAME(fk.parent_object_id), fk.name, fkc.constraint_column_id;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        i.name AS index_name,
        i.type_desc,
        CAST(i.is_unique AS BIT) AS is_unique,
        CAST(i.is_primary_key AS BIT) AS is_primary,
        CAST(i.is_unique_constraint AS BIT) AS is_unique_constraint,
        CAST(i.has_filter AS BIT) AS has_filter,
        i.filter_definition,
        ds.name AS data_space_name,
        ps.name AS partition_scheme_name,
        STUFF((
            SELECT ',' + c2.name
            FROM sys.index_columns AS ic2
            INNER JOIN sys.columns AS c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
            WHERE ic2.object_id = i.object_id
              AND ic2.index_id = i.index_id
              AND ic2.is_included_column = 0
            ORDER BY ic2.key_ordinal
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS columns,
        STUFF((
            SELECT ',' + c3.name
            FROM sys.index_columns AS ic3
            INNER JOIN sys.columns AS c3 ON ic3.object_id = c3.object_id AND ic3.column_id = c3.column_id
            WHERE ic3.object_id = i.object_id
              AND ic3.index_id = i.index_id
              AND ic3.is_included_column = 1
            ORDER BY ic3.index_column_id
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS included_columns
    FROM sys.indexes AS i
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
    LEFT JOIN sys.partition_schemes AS ps ON ds.data_space_id = ps.data_space_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND i.name IS NOT NULL
    ORDER BY s.name, t.name, i.name;

    -- name: by_table
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        i.name AS index_name,
        i.type_desc,
        CAST(i.is_unique AS BIT) AS is_unique,
        CAST(i.is_primary_key AS BIT) AS is_primary,
        CAST(i.is_unique_constraint AS BIT) AS is_unique_constraint,
        CAST(i.has_filter AS BIT) AS has_filter,
        i.filter_definition,
        ds.name AS data_space_name,
        ps.name AS partition_scheme_name,
        STUFF((
            SELECT ',' + c2.name
            FROM sys.index_columns AS ic2
            INNER JOIN sys.columns AS c2 ON ic2.object_id = c2.object_id AND ic2.column_id = c2.column_id
            WHERE ic2.object_id = i.object_id
              AND ic2.index_id = i.index_id
              AND ic2.is_included_column = 0
            ORDER BY ic2.key_ordinal
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS columns,
        STUFF((
            SELECT ',' + c3.name
            FROM sys.index_columns AS ic3
            INNER JOIN sys.columns AS c3 ON ic3.object_id = c3.object_id AND ic3.column_id = c3.column_id
            WHERE ic3.object_id = i.object_id
              AND ic3.index_id = i.index_id
              AND ic3.is_included_column = 1
            ORDER BY ic3.index_column_id
            FOR XML PATH(''), TYPE
        ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') AS included_columns
    FROM sys.indexes AS i
    INNER JOIN sys.tables AS t ON i.object_id = t.object_id
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
    LEFT JOIN sys.partition_schemes AS ps ON ds.data_space_id = ps.data_space_id
    WHERE s.name = :schema_name
      AND t.name = :table_name
      AND i.name IS NOT NULL
    ORDER BY i.name;
    ```

??? code "`modules.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        DB_NAME() AS catalog_name,
        s.name AS schema_name,
        o.name AS object_name,
        o.type AS object_type,
        o.type_desc,
        sm.definition,
        OBJECT_DEFINITION(o.object_id) AS object_definition,
        CAST(sm.uses_ansi_nulls AS BIT) AS uses_ansi_nulls,
        CAST(sm.uses_quoted_identifier AS BIT) AS uses_quoted_identifier,
        CAST(sm.is_schema_bound AS BIT) AS is_schema_bound
    FROM sys.sql_modules AS sm
    INNER JOIN sys.objects AS o ON sm.object_id = o.object_id
    INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND o.is_ms_shipped = 0
    ORDER BY s.name, o.name;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        DB_NAME() AS catalog_name,
        s.name AS schema_name,
        o.name AS object_name,
        o.type AS object_type,
        o.type_desc,
        o.create_date,
        o.modify_date,
        CAST(o.is_ms_shipped AS BIT) AS is_system_object
    FROM sys.objects AS o
    INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND o.is_ms_shipped = 0
    ORDER BY s.name, o.name;
    ```

??? code "`permissions.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        o.name AS object_name,
        o.type_desc AS object_type,
        grantee.name AS grantee_name,
        grantor.name AS grantor_name,
        perm.permission_name,
        perm.state_desc,
        perm.class_desc,
        perm.major_id,
        perm.minor_id
    FROM sys.database_permissions AS perm
    LEFT JOIN sys.objects AS o ON perm.major_id = o.object_id
    LEFT JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    LEFT JOIN sys.database_principals AS grantee ON perm.grantee_principal_id = grantee.principal_id
    LEFT JOIN sys.database_principals AS grantor ON perm.grantor_principal_id = grantor.principal_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, o.name, grantee.name, perm.permission_name;

    -- name: role_members
    -- dialect: mssql
    SELECT
        role_principal.name AS role_name,
        member_principal.name AS member_name,
        member_principal.type_desc AS member_type
    FROM sys.database_role_members AS drm
    INNER JOIN sys.database_principals AS role_principal ON drm.role_principal_id = role_principal.principal_id
    INNER JOIN sys.database_principals AS member_principal ON drm.member_principal_id = member_principal.principal_id
    ORDER BY role_principal.name, member_principal.name;
    ```

??? code "`routines.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        DB_NAME() AS catalog_name,
        s.name AS schema_name,
        p.name AS routine_name,
        p.type AS routine_type,
        p.type_desc,
        sm.definition,
        p.create_date,
        p.modify_date
    FROM sys.procedures AS p
    INNER JOIN sys.schemas AS s ON p.schema_id = s.schema_id
    LEFT JOIN sys.sql_modules AS sm ON p.object_id = sm.object_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND p.is_ms_shipped = 0
    ORDER BY s.name, p.name;

    -- name: parameters_by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        o.name AS routine_name,
        p.parameter_id,
        p.name AS parameter_name,
        typ.name AS data_type,
        p.max_length,
        p.precision AS numeric_precision,
        p.scale AS numeric_scale,
        CAST(p.is_output AS BIT) AS is_output,
        CAST(p.has_default_value AS BIT) AS has_default_value,
        CONVERT(NVARCHAR(MAX), p.default_value) AS default_value
    FROM sys.parameters AS p
    INNER JOIN sys.objects AS o ON p.object_id = o.object_id
    INNER JOIN sys.schemas AS s ON o.schema_id = s.schema_id
    INNER JOIN sys.types AS typ ON p.user_type_id = typ.user_type_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND o.is_ms_shipped = 0
    ORDER BY s.name, o.name, p.parameter_id;
    ```

??? code "`schemas.sql` (2 named queries)"

    ```sql
    -- name: list
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        s.schema_id,
        USER_NAME(s.principal_id) AS owner_name
    FROM sys.schemas AS s
    WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
    ORDER BY s.name;

    -- name: current
    -- dialect: mssql
    SELECT SCHEMA_NAME() AS schema_name;
    ```

??? code "`sequences.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        seq.name AS sequence_name,
        typ.name AS data_type,
        CONVERT(NVARCHAR(128), seq.start_value) AS start_value,
        CONVERT(NVARCHAR(128), seq.increment) AS increment_value,
        CONVERT(NVARCHAR(128), seq.minimum_value) AS minimum_value,
        CONVERT(NVARCHAR(128), seq.maximum_value) AS maximum_value,
        CAST(seq.is_cycling AS BIT) AS is_cycling,
        CAST(seq.is_cached AS BIT) AS is_cached,
        seq.cache_size
    FROM sys.sequences AS seq
    INNER JOIN sys.schemas AS s ON seq.schema_id = s.schema_id
    INNER JOIN sys.types AS typ ON seq.user_type_id = typ.user_type_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, seq.name;
    ```

??? code "`storage.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        i.name AS index_name,
        ds.name AS data_space_name,
        ds.type_desc AS data_space_type,
        ps.name AS partition_scheme_name,
        pf.name AS partition_function_name,
        pf.type_desc AS partition_function_type,
        CAST(p.partition_number AS INT) AS partition_number,
        p.rows AS row_count
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN sys.indexes AS i ON t.object_id = i.object_id AND i.index_id IN (0, 1)
    LEFT JOIN sys.data_spaces AS ds ON i.data_space_id = ds.data_space_id
    LEFT JOIN sys.partition_schemes AS ps ON ds.data_space_id = ps.data_space_id
    LEFT JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
    LEFT JOIN sys.partitions AS p ON i.object_id = p.object_id AND i.index_id = p.index_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, t.name, i.name, p.partition_number;
    ```

??? code "`system.sql` (2 named queries)"

    ```sql
    -- name: dmv_exec_requests
    -- dialect: mssql
    /* sqlspec:mssql:dmv_exec_requests */
    SELECT
        req.session_id,
        sess.login_name,
        req.status,
        req.command,
        req.cpu_time,
        req.total_elapsed_time,
        req.logical_reads,
        req.reads,
        req.writes,
        DB_NAME(req.database_id) AS database_name,
        txt.text AS sql_text
    FROM sys.dm_exec_requests AS req
    INNER JOIN sys.dm_exec_sessions AS sess ON req.session_id = sess.session_id
    OUTER APPLY sys.dm_exec_sql_text(req.sql_handle) AS txt
    WHERE req.session_id <> @@SPID
    ORDER BY req.total_elapsed_time DESC;

    -- name: query_store_runtime
    -- dialect: mssql
    /* sqlspec:mssql:query_store_runtime */
    SELECT
        qry.query_id,
        plan.plan_id,
        runtime_stats.count_executions,
        runtime_stats.avg_duration,
        runtime_stats.avg_cpu_time,
        runtime_stats.avg_logical_io_reads,
        runtime_stats.last_execution_time,
        query_text.query_sql_text AS sql_text
    FROM sys.query_store_query AS qry
    INNER JOIN sys.query_store_query_text AS query_text ON qry.query_text_id = query_text.query_text_id
    INNER JOIN sys.query_store_plan AS plan ON qry.query_id = plan.query_id
    INNER JOIN sys.query_store_runtime_stats AS runtime_stats ON plan.plan_id = runtime_stats.plan_id
    ORDER BY runtime_stats.last_execution_time DESC;
    ```

??? code "`tables.sql` (4 named queries)"

    ```sql
    -- name: all_by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        'BASE TABLE' AS table_type,
        DB_NAME() AS table_catalog,
        t.create_date,
        t.modify_date,
        CAST(t.is_memory_optimized AS BIT) AS is_memory_optimized,
        CAST(t.temporal_type AS INT) AS temporal_type,
        t.temporal_type_desc
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, t.name;

    -- name: by_schema
    -- dialect: mssql
    WITH dependency_tree AS (
        SELECT
            s.name AS schema_name,
            t.name AS table_name,
            t.object_id,
            0 AS level,
            CAST(CONCAT('|', t.object_id, '|') AS NVARCHAR(MAX)) AS path
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
        WHERE (:schema_name IS NULL OR s.name = :schema_name)
          AND NOT EXISTS (
              SELECT 1
              FROM sys.foreign_keys AS fk
              WHERE fk.parent_object_id = t.object_id
                AND fk.referenced_object_id <> t.object_id
          )

        UNION ALL

        SELECT
            child_schema.name AS schema_name,
            child_table.name AS table_name,
            child_table.object_id,
            dependency_tree.level + 1 AS level,
            CAST(CONCAT(dependency_tree.path, child_table.object_id, '|') AS NVARCHAR(MAX)) AS path
        FROM sys.foreign_keys AS fk
        INNER JOIN sys.tables AS child_table ON fk.parent_object_id = child_table.object_id
        INNER JOIN sys.schemas AS child_schema ON child_table.schema_id = child_schema.schema_id
        INNER JOIN dependency_tree ON fk.referenced_object_id = dependency_tree.object_id
        WHERE fk.referenced_object_id <> fk.parent_object_id
          AND (:schema_name IS NULL OR child_schema.name = :schema_name)
          AND CHARINDEX(CONCAT('|', child_table.object_id, '|'), dependency_tree.path) = 0
    )
    SELECT
        schema_name,
        table_name,
        'BASE TABLE' AS table_type,
        DB_NAME() AS table_catalog,
        MAX(level) AS level
    FROM dependency_tree
    GROUP BY schema_name, table_name
    ORDER BY level, schema_name, table_name;

    -- name: details_by_table
    -- dialect: mssql
    SELECT
        DB_NAME() AS catalog_name,
        s.name AS schema_name,
        t.name AS table_name,
        'BASE TABLE' AS table_type,
        t.create_date,
        t.modify_date,
        CAST(t.is_memory_optimized AS BIT) AS is_memory_optimized,
        CAST(t.durability AS INT) AS durability,
        t.durability_desc,
        CAST(t.temporal_type AS INT) AS temporal_type,
        t.temporal_type_desc,
        history_schema.name AS history_schema_name,
        history_table.name AS history_table_name
    FROM sys.tables AS t
    INNER JOIN sys.schemas AS s ON t.schema_id = s.schema_id
    LEFT JOIN sys.tables AS history_table ON t.history_table_id = history_table.object_id
    LEFT JOIN sys.schemas AS history_schema ON history_table.schema_id = history_schema.schema_id
    WHERE s.name = :schema_name
      AND t.name = :table_name
    ORDER BY s.name, t.name;

    -- name: names_by_schema
    -- dialect: mssql
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        t.create_date,
        t.modify_date
    FROM sys.tables t
    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
    ORDER BY s.name, t.name;
    ```

??? code "`triggers.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        trigger_schema.name AS schema_name,
        tr.name AS trigger_name,
        parent_schema.name AS parent_schema_name,
        parent_object.name AS parent_object_name,
        tr.type_desc,
        sm.definition,
        CAST(tr.is_disabled AS BIT) AS is_disabled,
        CAST(tr.is_instead_of_trigger AS BIT) AS is_instead_of_trigger,
        tr.create_date,
        tr.modify_date
    FROM sys.triggers AS tr
    LEFT JOIN sys.objects AS parent_object ON tr.parent_id = parent_object.object_id
    LEFT JOIN sys.schemas AS parent_schema ON parent_object.schema_id = parent_schema.schema_id
    LEFT JOIN sys.schemas AS trigger_schema ON parent_object.schema_id = trigger_schema.schema_id
    LEFT JOIN sys.sql_modules AS sm ON tr.object_id = sm.object_id
    WHERE (:schema_name IS NULL OR parent_schema.name = :schema_name)
      AND tr.is_ms_shipped = 0
    ORDER BY parent_schema.name, tr.name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: mssql
    SELECT
        @@VERSION AS version_string,
        CONVERT(NVARCHAR(128), SERVERPROPERTY('ProductVersion')) AS product_version,
        CONVERT(NVARCHAR(256), SERVERPROPERTY('Edition')) AS edition,
        CONVERT(INT, SERVERPROPERTY('EngineEdition')) AS engine_edition;
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: mssql
    SELECT
        DB_NAME() AS catalog_name,
        s.name AS schema_name,
        v.name AS view_name,
        CAST(v.is_ms_shipped AS BIT) AS is_system_object,
        sm.definition,
        CAST(sm.uses_ansi_nulls AS BIT) AS uses_ansi_nulls,
        CAST(sm.uses_quoted_identifier AS BIT) AS uses_quoted_identifier,
        v.create_date,
        v.modify_date
    FROM sys.views AS v
    INNER JOIN sys.schemas AS s ON v.schema_id = s.schema_id
    LEFT JOIN sys.sql_modules AS sm ON v.object_id = sm.object_id
    WHERE (:schema_name IS NULL OR s.name = :schema_name)
      AND v.is_ms_shipped = 0
    ORDER BY s.name, v.name;
    ```
