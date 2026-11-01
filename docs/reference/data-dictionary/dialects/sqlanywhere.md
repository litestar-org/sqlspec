# SAP SQL Anywhere Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("sqlanywhere")
```

Introspects SAP SQL Anywhere `SYS` catalog views (`SYS.SYSTAB`, `SYS.SYSTABCOL`, `SYS.SYSIDX`, `SYS.SYSIDXCOL`, `SYS.SYSFKEY`, `SYS.SYSCONSTRAINT`, `SYS.SYSVIEW`, `SYS.SYSPROCEDURE`, `SYS.SYSTRIGGER`, `SYS.SYSSEQUENCE`). See the [SAP SQL Anywhere Guide](../../../databases/sqlanywhere.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `sqlanywhere` |
| **Default Schema** | `"DBA"` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | AST synthesis + `SYS.SYSTAB` / `SYS.SYSPROCEDURE` definitions |
| **Catalog `.sql` Files** | `9` files (`16` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `AutoIncrement` | `INTEGER DEFAULT AUTOINCREMENT` |
| `supports_clustering` | `False` | `bigint` | `BIGINT` |
| `supports_cte` | `True` | `blob` | `LONG BINARY` |
| `supports_for_update` | `True` | `boolean` | `BIT` |
| `supports_in_memory` | `False` | `bytes` | `VARBINARY(32767)` |
| `supports_json` | `False` | `date` | `DATE` |
| `supports_merge` | `True` | `datetime` | `TIMESTAMP` |
| `supports_on_conflict` | `False` | `decimal` | `DECIMAL` |
| `supports_prepared_statements` | `True` | `float` | `DOUBLE` |
| `supports_returning` | `False` | `integer` | `INTEGER` |
| `supports_schemas` | `True` | `json` | `LONG VARCHAR` |
| `supports_sequences` | `True` | `smallint` | `SMALLINT` |
| `supports_skip_locked` | `True` | `string` | `VARCHAR(255)` |
| `supports_transactional_ddl` | `False` | `text` | `LONG VARCHAR` |
| `supports_transactions` | `True` | `time` | `TIME` |
| `supports_update_from` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_upsert` | `True` | `uuid` | `UNIQUEIDENTIFIER` |
| `supports_uuid` | `True` | `varchar` | `VARCHAR(255)` |
| `supports_window_functions` | `True` | - | - |
| `supports_cte` | `>= 10.0.0` | - | - |
| `supports_merge` | `>= 11.0.0` | - | - |
| `supports_window_functions` | `>= 10.0.0` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.sqlanywhere.data_dictionary.SqlanywhereSyncDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `sqlanywhere` dialect pack defines `16` named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/sqlanywhere/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `constraints.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `objects.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `schemas.sql` | `by_schema`, `current` | `:schema_name` |
| `tables.sql` | `by_schema`, `all_by_schema`, `names_by_schema` | `:schema_name`, `:table_name` |
| `version.sql` | `current` | *none* |
| `views.sql` | `by_schema` | `:schema_name`, `:view_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    WITH pk_cols AS (
        SELECT DISTINCT
            i.table_id,
            ic.column_id
        FROM SYS.SYSIDX i
        JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
        WHERE i.index_category = 1
    ),
    unique_cols AS (
        SELECT DISTINCT
            i.table_id,
            ic.column_id
        FROM SYS.SYSIDX i
        JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
        WHERE i."unique" IN (1, 2)
    )
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        c.column_name AS column_name,
        d.domain_name AS data_type,
        CASE c.nulls WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
        c."default" AS column_default,
        c.column_id AS ordinal_position,
        c.width AS char_length,
        c.width AS max_length,
        c.width AS numeric_precision,
        c.scale AS numeric_scale,
        CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        CASE WHEN uq.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN c."default" LIKE '%autoincrement%' THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN c."default" LIKE '%autoincrement%' THEN 'ALWAYS' ELSE NULL END AS identity_generation,
        CASE WHEN c.column_type = 'C' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN c.column_type = 'C' THEN c."default" ELSE NULL END AS generation_expression,
        0 AS is_hidden,
        NULL AS column_comment
    FROM SYS.SYSTABCOL c
    JOIN SYS.SYSTAB t ON c.table_id = t.table_id
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    JOIN SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
    LEFT JOIN pk_cols pk ON pk.table_id = t.table_id AND pk.column_id = c.column_id
    LEFT JOIN unique_cols uq ON uq.table_id = t.table_id AND uq.column_id = c.column_id
    WHERE t.table_type IN (1, 21)
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY u.user_name, t.table_name, c.column_id;

    -- name: by_table
    -- dialect: sqlanywhere
    WITH pk_cols AS (
        SELECT DISTINCT
            i.table_id,
            ic.column_id
        FROM SYS.SYSIDX i
        JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
        WHERE i.index_category = 1
    ),
    unique_cols AS (
        SELECT DISTINCT
            i.table_id,
            ic.column_id
        FROM SYS.SYSIDX i
        JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
        WHERE i."unique" IN (1, 2)
    )
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        c.column_name AS column_name,
        d.domain_name AS data_type,
        CASE c.nulls WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
        c."default" AS column_default,
        c.column_id AS ordinal_position,
        c.width AS char_length,
        c.width AS max_length,
        c.width AS numeric_precision,
        c.scale AS numeric_scale,
        CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        CASE WHEN uq.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN c."default" LIKE '%autoincrement%' THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN c."default" LIKE '%autoincrement%' THEN 'ALWAYS' ELSE NULL END AS identity_generation,
        CASE WHEN c.column_type = 'C' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN c.column_type = 'C' THEN c."default" ELSE NULL END AS generation_expression,
        0 AS is_hidden,
        NULL AS column_comment
    FROM SYS.SYSTABCOL c
    JOIN SYS.SYSTAB t ON c.table_id = t.table_id
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    JOIN SYS.SYSDOMAIN d ON c.domain_id = d.domain_id
    LEFT JOIN pk_cols pk ON pk.table_id = t.table_id AND pk.column_id = c.column_id
    LEFT JOIN unique_cols uq ON uq.table_id = t.table_id AND uq.column_id = c.column_id
    WHERE t.table_type IN (1, 21)
      AND t.table_name = :table_name
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
    ORDER BY c.column_id;
    ```

??? code "`constraints.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        i.index_name AS constraint_name,
        CASE i.index_category
            WHEN 1 THEN 'PRIMARY KEY'
            WHEN 2 THEN 'FOREIGN KEY'
            ELSE 'UNIQUE'
        END AS constraint_type,
        NULL AS check_clause,
        'Y' AS is_enforced
    FROM SYS.SYSIDX i
    JOIN SYS.SYSTAB t ON i.table_id = t.table_id
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    WHERE t.table_type = 1
      AND (i.index_category IN (1, 2) OR i."unique" IN (1, 2))
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY u.user_name, t.table_name, i.index_name;

    -- name: by_table
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        i.index_name AS constraint_name,
        CASE i.index_category
            WHEN 1 THEN 'PRIMARY KEY'
            WHEN 2 THEN 'FOREIGN KEY'
            ELSE 'UNIQUE'
        END AS constraint_type,
        NULL AS check_clause,
        'Y' AS is_enforced
    FROM SYS.SYSIDX i
    JOIN SYS.SYSTAB t ON i.table_id = t.table_id
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    WHERE t.table_type = 1
      AND (i.index_category IN (1, 2) OR i."unique" IN (1, 2))
      AND t.table_name = :table_name
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
    ORDER BY i.index_name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    SELECT
        fu.user_name AS schema_name,
        ft.table_name AS table_name,
        fi.index_name AS constraint_name,
        fc.column_name AS column_name,
        pu.user_name AS referenced_schema,
        pt.table_name AS referenced_table,
        pc.column_name AS referenced_column
    FROM SYS.SYSFKEY fk
    JOIN SYS.SYSTAB ft ON fk.foreign_table_id = ft.table_id
    JOIN SYS.SYSUSER fu ON ft.creator = fu.user_id
    JOIN SYS.SYSIDX fi ON fk.foreign_table_id = fi.table_id AND fk.foreign_index_id = fi.index_id
    JOIN SYS.SYSIDXCOL fic ON fi.table_id = fic.table_id AND fi.index_id = fic.index_id
    JOIN SYS.SYSTABCOL fc ON fic.table_id = fc.table_id AND fic.column_id = fc.column_id
    JOIN SYS.SYSTAB pt ON fk.primary_table_id = pt.table_id
    JOIN SYS.SYSUSER pu ON pt.creator = pu.user_id
    JOIN SYS.SYSTABCOL pc ON pt.table_id = pc.table_id AND fic.primary_column_id = pc.column_id
    WHERE fu.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR ft.table_name = :table_name)
    ORDER BY fu.user_name, ft.table_name, fi.index_name, fic.sequence;

    -- name: by_table
    -- dialect: sqlanywhere
    SELECT
        fu.user_name AS schema_name,
        ft.table_name AS table_name,
        fi.index_name AS constraint_name,
        fc.column_name AS column_name,
        pu.user_name AS referenced_schema,
        pt.table_name AS referenced_table,
        pc.column_name AS referenced_column
    FROM SYS.SYSFKEY fk
    JOIN SYS.SYSTAB ft ON fk.foreign_table_id = ft.table_id
    JOIN SYS.SYSUSER fu ON ft.creator = fu.user_id
    JOIN SYS.SYSIDX fi ON fk.foreign_table_id = fi.table_id AND fk.foreign_index_id = fi.index_id
    JOIN SYS.SYSIDXCOL fic ON fi.table_id = fic.table_id AND fi.index_id = fic.index_id
    JOIN SYS.SYSTABCOL fc ON fic.table_id = fc.table_id AND fic.column_id = fc.column_id
    JOIN SYS.SYSTAB pt ON fk.primary_table_id = pt.table_id
    JOIN SYS.SYSUSER pu ON pt.creator = pu.user_id
    JOIN SYS.SYSTABCOL pc ON pt.table_id = pc.table_id AND fic.primary_column_id = pc.column_id
    WHERE ft.table_name = :table_name
      AND fu.user_name = COALESCE(:schema_name, CURRENT USER)
    ORDER BY fi.index_name, fic.sequence;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        i.index_name AS index_name,
        c.column_name AS column_name,
        ic.sequence + 1 AS column_position,
        CASE WHEN i."unique" IN (1, 2) THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN i.index_category = 1 THEN 1 ELSE 0 END AS is_primary
    FROM SYS.SYSIDX i
    JOIN SYS.SYSTAB t ON i.table_id = t.table_id
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
    JOIN SYS.SYSTABCOL c ON ic.table_id = c.table_id AND ic.column_id = c.column_id
    WHERE t.table_type = 1
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY u.user_name, t.table_name, i.index_name, ic.sequence;

    -- name: by_table
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        i.index_name AS index_name,
        c.column_name AS column_name,
        ic.sequence + 1 AS column_position,
        CASE WHEN i."unique" IN (1, 2) THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN i.index_category = 1 THEN 1 ELSE 0 END AS is_primary
    FROM SYS.SYSIDX i
    JOIN SYS.SYSTAB t ON i.table_id = t.table_id
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    JOIN SYS.SYSIDXCOL ic ON i.table_id = ic.table_id AND i.index_id = ic.index_id
    JOIN SYS.SYSTABCOL c ON ic.table_id = c.table_id AND ic.column_id = c.column_id
    WHERE t.table_type = 1
      AND t.table_name = :table_name
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
    ORDER BY i.index_name, ic.sequence;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS object_name,
        CASE t.table_type
            WHEN 1 THEN 'TABLE'
            WHEN 21 THEN 'VIEW'
            ELSE 'OBJECT'
        END AS object_type,
        NULL AS created,
        NULL AS remarks
    FROM SYS.SYSTAB t
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    WHERE t.table_type IN (1, 21)
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:object_name IS NULL OR t.table_name = :object_name)
    ORDER BY u.user_name, object_type, t.table_name;
    ```

??? code "`schemas.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        u.user_name AS owner_name,
        NULL AS create_time,
        NULL AS comment
    FROM SYS.SYSUSER u
    WHERE u.user_name NOT IN ('SYS', 'PUBLIC', 'dbo', 'diagnostics', 'rs_systabgroup', 'SA_DEBUG', 'SYS_SPATIAL_ADMIN_ROLE')
      AND (:schema_name IS NULL OR u.user_name = :schema_name)
    ORDER BY u.user_name;

    -- name: current
    -- dialect: sqlanywhere
    SELECT CURRENT USER AS schema_name;
    ```

??? code "`tables.sql` (3 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    WITH RECURSIVE dependency_tree (table_id, schema_name, table_name, dependency_level) AS (
        SELECT
            t.table_id,
            u.user_name AS schema_name,
            t.table_name AS table_name,
            0 AS dependency_level
        FROM SYS.SYSTAB t
        JOIN SYS.SYSUSER u ON t.creator = u.user_id
        WHERE t.table_type = 1
          AND u.user_name = COALESCE(:schema_name, CURRENT USER)
          AND (:table_name IS NULL OR t.table_name = :table_name)
          AND NOT EXISTS (
              SELECT 1
              FROM SYS.SYSFKEY fk
              WHERE fk.foreign_table_id = t.table_id
                AND fk.primary_table_id <> t.table_id
          )
        UNION ALL
        SELECT
            child.table_id,
            u.user_name AS schema_name,
            child.table_name AS table_name,
            parent.dependency_level + 1 AS dependency_level
        FROM SYS.SYSFKEY fk
        JOIN SYS.SYSTAB child ON child.table_id = fk.foreign_table_id
        JOIN SYS.SYSUSER u ON child.creator = u.user_id
        JOIN dependency_tree parent ON parent.table_id = fk.primary_table_id
        WHERE child.table_type = 1
          AND fk.primary_table_id <> fk.foreign_table_id
          AND u.user_name = COALESCE(:schema_name, CURRENT USER)
          AND (:table_name IS NULL OR child.table_name = :table_name)
          AND parent.dependency_level < 20
    ),
    dependency_levels AS (
        SELECT
            table_id,
            MAX(dependency_level) AS dependency_level
        FROM dependency_tree
        GROUP BY table_id
    )
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        'BASE TABLE' AS table_type,
        COALESCE(dl.dependency_level, 0) AS dependency_level,
        COALESCE(dl.dependency_level, 0) AS level
    FROM SYS.SYSTAB t
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    LEFT JOIN dependency_levels dl ON dl.table_id = t.table_id
    WHERE t.table_type = 1
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY dependency_level, u.user_name, t.table_name;

    -- name: all_by_schema
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS table_name,
        CASE t.table_type
            WHEN 1 THEN 'BASE TABLE'
            WHEN 21 THEN 'VIEW'
            ELSE 'TABLE'
        END AS table_type,
        0 AS dependency_level,
        0 AS level,
        t.count AS estimated_row_count,
        NULL AS table_comment
    FROM SYS.SYSTAB t
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    WHERE t.table_type IN (1, 21)
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY u.user_name, t.table_name;

    -- name: names_by_schema
    -- dialect: sqlanywhere
    SELECT
        t.table_name AS table_name
    FROM SYS.SYSTAB t
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    WHERE t.table_type = 1
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
    ORDER BY t.table_name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: sqlanywhere
    SELECT PROPERTY('ProductVersion') AS version_string;
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlanywhere
    SELECT
        u.user_name AS schema_name,
        t.table_name AS view_name,
        v.view_def AS definition,
        0 AS is_read_only,
        1 AS is_valid
    FROM SYS.SYSTAB t
    JOIN SYS.SYSUSER u ON t.creator = u.user_id
    LEFT JOIN SYS.SYSVIEW v ON t.object_id = v.view_object_id
    WHERE t.table_type = 21
      AND u.user_name = COALESCE(:schema_name, CURRENT USER)
      AND (:view_name IS NULL OR t.table_name = :view_name)
    ORDER BY u.user_name, t.table_name;
    ```
