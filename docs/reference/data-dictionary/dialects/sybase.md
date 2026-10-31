# Sybase ASE Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("sybase")
```

Introspects SAP Adaptive Server Enterprise system tables (`dbo.sysobjects`, `dbo.syscolumns`, `dbo.sysindexes`, `dbo.sysreferences`, `dbo.syscomments`, `dbo.systypes`, `dbo.sysusers`). See the [Sybase ASE Guide](../../../databases/sybase.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `sybase` |
| **Default Schema** | `"dbo"` |
| **Version Pattern** | `Adaptive Server Enterprise/(\d+)\.(\d+)(?:\s+SP(\d+)|\.\d+)?` |
| **DDL Strategy** | AST synthesis + `dbo.syscomments` object text reassembly |
| **Catalog `.sql` Files** | `9` files (`16` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `AutoIncrement` | `INT IDENTITY` |
| `supports_clustering` | `False` | `bigint` | `BIGINT` |
| `supports_cte` | `False` | `blob` | `IMAGE` |
| `supports_for_update` | `False` | `boolean` | `BIT` |
| `supports_in_memory` | `False` | `bytes` | `VARBINARY(16384)` |
| `supports_json` | `False` | `date` | `DATE` |
| `supports_merge` | `False` | `datetime` | `BIGDATETIME` |
| `supports_on_conflict` | `False` | `decimal` | `DECIMAL` |
| `supports_prepared_statements` | `True` | `float` | `FLOAT` |
| `supports_returning` | `False` | `integer` | `INT` |
| `supports_schemas` | `True` | `json` | `TEXT` |
| `supports_sequences` | `False` | `smallint` | `SMALLINT` |
| `supports_skip_locked` | `False` | `string` | `VARCHAR(255)` |
| `supports_transactional_ddl` | `False` | `text` | `TEXT` |
| `supports_transactions` | `True` | `time` | `BIGTIME` |
| `supports_update_from` | `True` | `timestamp` | `BIGDATETIME` |
| `supports_upsert` | `False` | `uuid` | `VARCHAR(36)` |
| `supports_uuid` | `False` | `varchar` | `VARCHAR(255)` |
| `supports_window_functions` | `False` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.sybase.data_dictionary.SybaseSyncDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `sybase` dialect pack defines `16` named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/sybase/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `constraints.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `objects.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `schemas.sql` | `by_schema`, `current` | `:schema_name` |
| `tables.sql` | `by_schema`, `all_by_schema`, `names_by_schema` | `:schema_name`, `:table_name` |
| `version.sql` | `current` | `@version` |
| `views.sql` | `by_schema` | `:schema_name`, `:view_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        c.name AS column_name,
        t.name AS data_type,
        CASE WHEN (c.status & 8) = 8 THEN 'YES' ELSE 'NO' END AS is_nullable,
        cm.text AS column_default,
        c.colid AS ordinal_position,
        c.length AS char_length,
        c.length AS max_length,
        c.prec AS numeric_precision,
        c.scale AS numeric_scale,
        CASE WHEN pk.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        CASE WHEN uq.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN (c.status & 128) = 128 THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN (c.status & 128) = 128 THEN 'ALWAYS' ELSE NULL END AS identity_generation,
        0 AS is_generated,
        NULL AS generation_expression,
        0 AS is_hidden,
        NULL AS column_comment
    FROM dbo.syscolumns c
    JOIN dbo.sysobjects o ON c.id = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    JOIN dbo.systypes t ON c.usertype = t.usertype
    LEFT JOIN dbo.syscomments cm ON c.cdefault = cm.id AND cm.colid = 1
    LEFT JOIN (
        SELECT DISTINCT
            i.id,
            index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
        FROM dbo.sysindexes i
        JOIN dbo.sysobjects o ON i.id = o.id
        CROSS JOIN (
            SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
            UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
            UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
        ) k
        WHERE (i.status & 2048) = 2048
          AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
    ) pk ON pk.id = o.id AND pk.col_name = c.name
    LEFT JOIN (
        SELECT DISTINCT
            i.id,
            index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
        FROM dbo.sysindexes i
        JOIN dbo.sysobjects o ON i.id = o.id
        CROSS JOIN (
            SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
            UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
            UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
        ) k
        WHERE (i.status & 2) = 2
          AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
    ) uq ON uq.id = o.id AND uq.col_name = c.name
    WHERE o.type IN ('U', 'V')
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
    ORDER BY u.name, o.name, c.colid;

    -- name: by_table
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        c.name AS column_name,
        t.name AS data_type,
        CASE WHEN (c.status & 8) = 8 THEN 'YES' ELSE 'NO' END AS is_nullable,
        cm.text AS column_default,
        c.colid AS ordinal_position,
        c.length AS char_length,
        c.length AS max_length,
        c.prec AS numeric_precision,
        c.scale AS numeric_scale,
        CASE WHEN pk.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        CASE WHEN uq.col_name IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN (c.status & 128) = 128 THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN (c.status & 128) = 128 THEN 'ALWAYS' ELSE NULL END AS identity_generation,
        0 AS is_generated,
        NULL AS generation_expression,
        0 AS is_hidden,
        NULL AS column_comment
    FROM dbo.syscolumns c
    JOIN dbo.sysobjects o ON c.id = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    JOIN dbo.systypes t ON c.usertype = t.usertype
    LEFT JOIN dbo.syscomments cm ON c.cdefault = cm.id AND cm.colid = 1
    LEFT JOIN (
        SELECT DISTINCT
            i.id,
            index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
        FROM dbo.sysindexes i
        JOIN dbo.sysobjects o ON i.id = o.id
        CROSS JOIN (
            SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
            UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
            UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
        ) k
        WHERE (i.status & 2048) = 2048
          AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
    ) pk ON pk.id = o.id AND pk.col_name = c.name
    LEFT JOIN (
        SELECT DISTINCT
            i.id,
            index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) AS col_name
        FROM dbo.sysindexes i
        JOIN dbo.sysobjects o ON i.id = o.id
        CROSS JOIN (
            SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
            UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
            UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
            UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
        ) k
        WHERE (i.status & 2) = 2
          AND index_col(USER_NAME(o.uid) + '.' + o.name, i.indid, k.pos) IS NOT NULL
    ) uq ON uq.id = o.id AND uq.col_name = c.name
    WHERE o.type IN ('U', 'V')
      AND o.name = :table_name
      AND u.name = COALESCE(:schema_name, USER_NAME())
    ORDER BY c.colid;
    ```

??? code "`constraints.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        co.name AS constraint_name,
        CASE
            WHEN (c.status & 1) = 1 THEN 'PRIMARY KEY'
            WHEN (c.status & 2) = 2 THEN 'UNIQUE'
            WHEN (c.status & 64) = 64 THEN 'FOREIGN KEY'
            WHEN (c.status & 128) = 128 THEN 'CHECK'
            ELSE 'CONSTRAINT'
        END AS constraint_type,
        cm.text AS check_clause,
        'Y' AS is_enforced
    FROM dbo.sysconstraints c
    JOIN dbo.sysobjects o ON c.tableid = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    JOIN dbo.sysobjects co ON c.constrid = co.id
    LEFT JOIN dbo.syscomments cm ON co.id = cm.id AND cm.colid = 1
    WHERE u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
    UNION
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        i.name AS constraint_name,
        CASE
            WHEN (i.status & 2048) = 2048 THEN 'PRIMARY KEY'
            WHEN (i.status & 2) = 2 THEN 'UNIQUE'
        END AS constraint_type,
        NULL AS check_clause,
        'Y' AS is_enforced
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    WHERE o.type = 'U'
      AND ((i.status & 2048) = 2048 OR (i.status & 2) = 2)
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.sysconstraints sc
          JOIN dbo.sysobjects sco ON sc.constrid = sco.id
          WHERE sc.tableid = o.id AND sco.name = i.name
      )
    ORDER BY schema_name, table_name, constraint_name;

    -- name: by_table
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        co.name AS constraint_name,
        CASE
            WHEN (c.status & 1) = 1 THEN 'PRIMARY KEY'
            WHEN (c.status & 2) = 2 THEN 'UNIQUE'
            WHEN (c.status & 64) = 64 THEN 'FOREIGN KEY'
            WHEN (c.status & 128) = 128 THEN 'CHECK'
            ELSE 'CONSTRAINT'
        END AS constraint_type,
        cm.text AS check_clause,
        'Y' AS is_enforced
    FROM dbo.sysconstraints c
    JOIN dbo.sysobjects o ON c.tableid = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    JOIN dbo.sysobjects co ON c.constrid = co.id
    LEFT JOIN dbo.syscomments cm ON co.id = cm.id AND cm.colid = 1
    WHERE o.name = :table_name
      AND u.name = COALESCE(:schema_name, USER_NAME())
    UNION
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        i.name AS constraint_name,
        CASE
            WHEN (i.status & 2048) = 2048 THEN 'PRIMARY KEY'
            WHEN (i.status & 2) = 2 THEN 'UNIQUE'
        END AS constraint_type,
        NULL AS check_clause,
        'Y' AS is_enforced
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    WHERE o.type = 'U'
      AND ((i.status & 2048) = 2048 OR (i.status & 2) = 2)
      AND o.name = :table_name
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND NOT EXISTS (
          SELECT 1
          FROM dbo.sysconstraints sc
          JOIN dbo.sysobjects sco ON sc.constrid = sco.id
          WHERE sc.tableid = o.id AND sco.name = i.name
      )
    ORDER BY constraint_name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        co.name AS constraint_name,
        fc.name AS column_name,
        ru.name AS referenced_schema,
        ro.name AS referenced_table,
        rc.name AS referenced_column
    FROM dbo.sysreferences r
    JOIN dbo.sysobjects o ON r.tableid = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    JOIN dbo.sysobjects co ON r.constrid = co.id
    JOIN dbo.sysobjects ro ON r.reftabid = ro.id
    JOIN dbo.sysusers ru ON ro.uid = ru.uid
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    JOIN dbo.syscolumns fc ON fc.id = o.id AND fc.colid = CASE k.pos
        WHEN 1 THEN r.fokey1 WHEN 2 THEN r.fokey2 WHEN 3 THEN r.fokey3 WHEN 4 THEN r.fokey4
        WHEN 5 THEN r.fokey5 WHEN 6 THEN r.fokey6 WHEN 7 THEN r.fokey7 WHEN 8 THEN r.fokey8
        WHEN 9 THEN r.fokey9 WHEN 10 THEN r.fokey10 WHEN 11 THEN r.fokey11 WHEN 12 THEN r.fokey12
        WHEN 13 THEN r.fokey13 WHEN 14 THEN r.fokey14 WHEN 15 THEN r.fokey15 WHEN 16 THEN r.fokey16
    END
    JOIN dbo.syscolumns rc ON rc.id = ro.id AND rc.colid = CASE k.pos
        WHEN 1 THEN r.refkey1 WHEN 2 THEN r.refkey2 WHEN 3 THEN r.refkey3 WHEN 4 THEN r.refkey4
        WHEN 5 THEN r.refkey5 WHEN 6 THEN r.refkey6 WHEN 7 THEN r.refkey7 WHEN 8 THEN r.refkey8
        WHEN 9 THEN r.refkey9 WHEN 10 THEN r.refkey10 WHEN 11 THEN r.refkey11 WHEN 12 THEN r.refkey12
        WHEN 13 THEN r.refkey13 WHEN 14 THEN r.refkey14 WHEN 15 THEN r.refkey15 WHEN 16 THEN r.refkey16
    END
    WHERE k.pos <= r.frgcnt
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
    ORDER BY u.name, o.name, co.name, k.pos;

    -- name: by_table
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        co.name AS constraint_name,
        fc.name AS column_name,
        ru.name AS referenced_schema,
        ro.name AS referenced_table,
        rc.name AS referenced_column
    FROM dbo.sysreferences r
    JOIN dbo.sysobjects o ON r.tableid = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    JOIN dbo.sysobjects co ON r.constrid = co.id
    JOIN dbo.sysobjects ro ON r.reftabid = ro.id
    JOIN dbo.sysusers ru ON ro.uid = ru.uid
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    JOIN dbo.syscolumns fc ON fc.id = o.id AND fc.colid = CASE k.pos
        WHEN 1 THEN r.fokey1 WHEN 2 THEN r.fokey2 WHEN 3 THEN r.fokey3 WHEN 4 THEN r.fokey4
        WHEN 5 THEN r.fokey5 WHEN 6 THEN r.fokey6 WHEN 7 THEN r.fokey7 WHEN 8 THEN r.fokey8
        WHEN 9 THEN r.fokey9 WHEN 10 THEN r.fokey10 WHEN 11 THEN r.fokey11 WHEN 12 THEN r.fokey12
        WHEN 13 THEN r.fokey13 WHEN 14 THEN r.fokey14 WHEN 15 THEN r.fokey15 WHEN 16 THEN r.fokey16
    END
    JOIN dbo.syscolumns rc ON rc.id = ro.id AND rc.colid = CASE k.pos
        WHEN 1 THEN r.refkey1 WHEN 2 THEN r.refkey2 WHEN 3 THEN r.refkey3 WHEN 4 THEN r.refkey4
        WHEN 5 THEN r.refkey5 WHEN 6 THEN r.refkey6 WHEN 7 THEN r.refkey7 WHEN 8 THEN r.refkey8
        WHEN 9 THEN r.refkey9 WHEN 10 THEN r.refkey10 WHEN 11 THEN r.refkey11 WHEN 12 THEN r.refkey12
        WHEN 13 THEN r.refkey13 WHEN 14 THEN r.refkey14 WHEN 15 THEN r.refkey15 WHEN 16 THEN r.refkey16
    END
    WHERE k.pos <= r.frgcnt
      AND o.name = :table_name
      AND u.name = COALESCE(:schema_name, USER_NAME())
    ORDER BY co.name, k.pos;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        i.name AS index_name,
        index_col(u.name + '.' + o.name, i.indid, k.pos) AS column_name,
        k.pos AS column_position,
        CASE WHEN (i.status & 2) = 2 THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN (i.status & 2048) = 2048 THEN 1 ELSE 0 END AS is_primary
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    WHERE o.type = 'U'
      AND i.indid BETWEEN 1 AND 254
      AND index_col(u.name + '.' + o.name, i.indid, k.pos) IS NOT NULL
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
    ORDER BY u.name, o.name, i.name, k.pos;

    -- name: by_table
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        i.name AS index_name,
        index_col(u.name + '.' + o.name, i.indid, k.pos) AS column_name,
        k.pos AS column_position,
        CASE WHEN (i.status & 2) = 2 THEN 1 ELSE 0 END AS is_unique,
        CASE WHEN (i.status & 2048) = 2048 THEN 1 ELSE 0 END AS is_primary
    FROM dbo.sysindexes i
    JOIN dbo.sysobjects o ON i.id = o.id
    JOIN dbo.sysusers u ON o.uid = u.uid
    CROSS JOIN (
        SELECT 1 AS pos UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
        UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8
        UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11 UNION ALL SELECT 12
        UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15 UNION ALL SELECT 16
    ) k
    WHERE o.type = 'U'
      AND i.indid BETWEEN 1 AND 254
      AND index_col(u.name + '.' + o.name, i.indid, k.pos) IS NOT NULL
      AND o.name = :table_name
      AND u.name = COALESCE(:schema_name, USER_NAME())
    ORDER BY i.name, k.pos;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS object_name,
        CASE o.type
            WHEN 'U' THEN 'TABLE'
            WHEN 'V' THEN 'VIEW'
            WHEN 'P' THEN 'PROCEDURE'
            WHEN 'SF' THEN 'FUNCTION'
            WHEN 'TR' THEN 'TRIGGER'
            ELSE o.type
        END AS object_type,
        o.crdate AS created,
        NULL AS remarks
    FROM dbo.sysobjects o
    JOIN dbo.sysusers u ON o.uid = u.uid
    WHERE o.type IN ('U', 'V', 'P', 'SF', 'TR')
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:object_name IS NULL OR o.name = :object_name)
    ORDER BY u.name, object_type, o.name;
    ```

??? code "`schemas.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        u.name AS owner_name,
        NULL AS create_time,
        NULL AS comment
    FROM dbo.sysusers u
    WHERE u.uid < 16384
      AND u.name NOT IN ('public', 'probe', 'guest')
      AND (:schema_name IS NULL OR u.name = :schema_name)
    ORDER BY u.name;

    -- name: current
    -- dialect: sybase
    SELECT USER_NAME() AS schema_name;
    ```

??? code "`tables.sql` (3 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        'BASE TABLE' AS table_type,
        0 AS dependency_level,
        0 AS level
    FROM dbo.sysobjects o
    JOIN dbo.sysusers u ON o.uid = u.uid
    WHERE o.type = 'U'
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
    ORDER BY u.name, o.name;

    -- name: all_by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS table_name,
        CASE o.type
            WHEN 'U' THEN 'BASE TABLE'
            WHEN 'V' THEN 'VIEW'
            WHEN 'S' THEN 'SYSTEM TABLE'
            ELSE 'TABLE'
        END AS table_type,
        0 AS dependency_level,
        0 AS level,
        NULL AS estimated_row_count,
        NULL AS table_comment
    FROM dbo.sysobjects o
    JOIN dbo.sysusers u ON o.uid = u.uid
    WHERE o.type IN ('U', 'V')
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:table_name IS NULL OR o.name = :table_name)
    ORDER BY u.name, o.name;

    -- name: names_by_schema
    -- dialect: sybase
    SELECT
        o.name AS table_name
    FROM dbo.sysobjects o
    JOIN dbo.sysusers u ON o.uid = u.uid
    WHERE o.type = 'U'
      AND u.name = COALESCE(:schema_name, USER_NAME())
    ORDER BY o.name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: sybase
    SELECT @@version AS version_string;
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sybase
    SELECT
        u.name AS schema_name,
        o.name AS view_name,
        cm.text AS definition,
        0 AS is_read_only,
        1 AS is_valid
    FROM dbo.sysobjects o
    JOIN dbo.sysusers u ON o.uid = u.uid
    LEFT JOIN dbo.syscomments cm ON o.id = cm.id AND cm.colid = 1
    WHERE o.type = 'V'
      AND u.name = COALESCE(:schema_name, USER_NAME())
      AND (:view_name IS NULL OR o.name = :view_name)
    ORDER BY u.name, o.name;
    ```
