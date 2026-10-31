# SQLite Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("sqlite")
```

Introspects `sqlite_master` and table-valued `pragma_table_xinfo`, `pragma_index_list`, `pragma_index_info`, and `pragma_foreign_key_list` functions across `sqlite` and `aiosqlite`. See the [SQLite Guide](../../../databases/sqlite.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `sqlite` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `sqlite_master.sql` + `PRAGMA` synthesis |
| **Catalog `.sql` Files** | `15` files (`23` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `False` | `bigint` | `INTEGER` |
| `supports_for_update` | `False` | `blob` | `BLOB` |
| `supports_on_conflict` | `True` | `boolean` | `INTEGER` |
| `supports_prepared_statements` | `True` | `float` | `REAL` |
| `supports_schemas` | `False` | `integer` | `INTEGER` |
| `supports_skip_locked` | `False` | `json` | `JSON` |
| `supports_transactions` | `True` | `text` | `TEXT` |
| `supports_update_from` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_uuid` | `False` | `uuid` | `TEXT` |
| `supports_cte` | `>= 3.8.3` | `varchar` | `TEXT` |
| `supports_json` | `>= 3.38.0` | - | - |
| `supports_returning` | `>= 3.35.0` | - | - |
| `supports_upsert` | `>= 3.24.0` | - | - |
| `supports_window_functions` | `>= 3.25.0` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.sqlite.data_dictionary.SqliteDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.aiosqlite.data_dictionary.AiosqliteDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `sqlite` dialect pack defines `23` named queries across `15` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/sqlite/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `compile_options.sql` | `list` | *none* |
| `constraints.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `ddl.sql` | `by_object`, `by_schema` | `:object_name`, `:object_type`, `:schema_name` |
| `foreign_keys.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `functions.sql` | `list` | *none* |
| `indexes.sql` | `by_schema`, `by_table`, `columns_by_index`, `index_columns_by_index` | `:index_name`, `:schema_name`, `:table_name` |
| `modules.sql` | `list` | *none* |
| `objects.sql` | `by_schema` | `:schema_name` |
| `schemas.sql` | `list` | *none* |
| `system.sql` | `foreign_key_check`, `integrity_check` | *none* |
| `tables.sql` | `by_schema` | `:schema_name` |
| `triggers.sql` | `by_schema` | `:schema_name` |
| `version.sql` | `current` | *none* |
| `views.sql` | `by_schema` | `:schema_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        tl.schema AS schema_name,
        tl.name AS table_name,
        ti.name AS column_name,
        ti.type AS data_type,
        CASE WHEN ti."notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
        ti.dflt_value AS column_default,
        ti.cid + 1 AS ordinal_position,
        ti.pk AS is_primary,
        CASE
            WHEN ti.pk = 1 AND UPPER(ti.type) = 'INTEGER' AND UPPER(sm.sql) LIKE '%AUTOINCREMENT%' THEN 1
            ELSE 0
        END AS is_identity,
        CASE
            WHEN ti.hidden = 1 THEN 'hidden'
            WHEN ti.hidden = 2 THEN 'virtual generated'
            WHEN ti.hidden = 3 THEN 'stored generated'
            WHEN ti.pk = 1 AND UPPER(ti.type) = 'INTEGER' AND UPPER(sm.sql) LIKE '%AUTOINCREMENT%' THEN 'autoincrement'
            ELSE NULL
        END AS extra,
        ti.hidden IN (2, 3) AS is_generated,
        CASE WHEN ti.hidden = 1 THEN 1 ELSE 0 END AS is_hidden
    FROM pragma_table_list AS tl
    JOIN pragma_table_xinfo(tl.name, COALESCE(:schema_name, 'main')) AS ti
    LEFT JOIN {schema_prefix}sqlite_schema AS sm ON sm.type = 'table' AND sm.name = tl.name
    WHERE tl.schema = COALESCE(:schema_name, 'main')
      AND tl.type IN ('table', 'virtual')
      AND tl.name NOT LIKE 'sqlite_%'
    ORDER BY tl.name, ti.cid;

    -- name: by_table
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        :table_name AS table_name,
        ti.name AS column_name,
        ti.type AS data_type,
        CASE WHEN ti."notnull" THEN 'NO' ELSE 'YES' END AS is_nullable,
        ti.dflt_value AS column_default,
        ti.cid + 1 AS ordinal_position,
        ti.pk AS is_primary,
        CASE
            WHEN ti.pk = 1 AND UPPER(ti.type) = 'INTEGER' AND UPPER(sm.sql) LIKE '%AUTOINCREMENT%' THEN 1
            ELSE 0
        END AS is_identity,
        CASE
            WHEN ti.hidden = 1 THEN 'hidden'
            WHEN ti.hidden = 2 THEN 'virtual generated'
            WHEN ti.hidden = 3 THEN 'stored generated'
            WHEN ti.pk = 1 AND UPPER(ti.type) = 'INTEGER' AND UPPER(sm.sql) LIKE '%AUTOINCREMENT%' THEN 'autoincrement'
            ELSE NULL
        END AS extra,
        ti.hidden IN (2, 3) AS is_generated,
        CASE WHEN ti.hidden = 1 THEN 1 ELSE 0 END AS is_hidden
    FROM pragma_table_xinfo(:table_name, :schema_name) AS ti
    LEFT JOIN {schema_prefix}sqlite_schema AS sm ON sm.type = 'table' AND sm.name = :table_name
    ORDER BY ti.cid;
    ```

??? code "`compile_options.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: sqlite
    PRAGMA compile_options;
    ```

??? code "`constraints.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        COALESCE(:schema_name, 'main') AS schema_name,
        tl.name AS table_name,
        il.name AS index_name,
        CASE WHEN il.origin = 'pk' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
        ii.name AS column_name,
        ii.seqno AS ordinal_position
    FROM pragma_table_list AS tl
    JOIN pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il
    JOIN pragma_index_info(il.name, COALESCE(:schema_name, 'main')) AS ii
    WHERE tl.schema = COALESCE(:schema_name, 'main')
      AND tl.type IN ('table', 'virtual')
      AND tl.name NOT LIKE 'sqlite_%'
      AND il.origin IN ('pk', 'u')
      AND (:table_name IS NULL OR tl.name = :table_name)
    UNION ALL
    SELECT
        COALESCE(:schema_name, 'main') AS schema_name,
        tl.name AS table_name,
        NULL AS index_name,
        'PRIMARY KEY' AS constraint_type,
        ti.name AS column_name,
        ti.pk - 1 AS ordinal_position
    FROM pragma_table_list AS tl
    JOIN pragma_table_info(tl.name, COALESCE(:schema_name, 'main')) AS ti
    WHERE tl.schema = COALESCE(:schema_name, 'main')
      AND tl.type IN ('table', 'virtual')
      AND tl.name NOT LIKE 'sqlite_%'
      AND ti.pk > 0
      AND (:table_name IS NULL OR tl.name = :table_name)
      AND NOT EXISTS (
          SELECT 1
          FROM pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il_pk
          WHERE il_pk.origin = 'pk'
      )
    ORDER BY table_name, index_name, ordinal_position;

    -- name: by_table
    -- dialect: sqlite
    SELECT
        COALESCE(:schema_name, 'main') AS schema_name,
        :table_name AS table_name,
        il.name AS index_name,
        CASE WHEN il.origin = 'pk' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END AS constraint_type,
        ii.name AS column_name,
        ii.seqno AS ordinal_position
    FROM pragma_index_list(:table_name, COALESCE(:schema_name, 'main')) AS il
    JOIN pragma_index_info(il.name, COALESCE(:schema_name, 'main')) AS ii
    WHERE il.origin IN ('pk', 'u')
    UNION ALL
    SELECT
        COALESCE(:schema_name, 'main') AS schema_name,
        :table_name AS table_name,
        NULL AS index_name,
        'PRIMARY KEY' AS constraint_type,
        ti.name AS column_name,
        ti.pk - 1 AS ordinal_position
    FROM pragma_table_info(:table_name, COALESCE(:schema_name, 'main')) AS ti
    WHERE ti.pk > 0
      AND NOT EXISTS (
          SELECT 1
          FROM pragma_index_list(:table_name, COALESCE(:schema_name, 'main')) AS il_pk
          WHERE il_pk.origin = 'pk'
      )
    ORDER BY index_name, ordinal_position;
    ```

??? code "`ddl.sql` (2 named queries)"

    ```sql
    -- name: by_object
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        name AS object_name,
        type AS object_type,
        tbl_name AS table_name,
        sql AS native_sql,
        sql AS ddl,
        'native' AS fidelity
    FROM {schema_prefix}sqlite_schema
    WHERE name = :object_name
      AND (:object_type IS NULL OR LOWER(type) = LOWER(:object_type))
      AND sql IS NOT NULL;

    -- name: by_schema
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        name AS object_name,
        type AS object_type,
        tbl_name AS table_name,
        sql AS native_sql,
        sql AS ddl,
        'native' AS fidelity
    FROM {schema_prefix}sqlite_schema
    WHERE name NOT LIKE 'sqlite_%'
      AND sql IS NOT NULL
    ORDER BY
        CASE type
            WHEN 'table' THEN 1
            WHEN 'view' THEN 2
            WHEN 'index' THEN 3
            WHEN 'trigger' THEN 4
            ELSE 5
        END,
        name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        tl.schema AS schema_name,
        tl.schema AS table_schema,
        tl.name AS table_name,
        fk."from" AS column_name,
        fk."table" AS referenced_table_name,
        fk."to" AS referenced_column_name,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY tl.name, fk.id) > 1 THEN tl.name || '_fk_' || fk.id
            ELSE NULL
        END AS constraint_name,
        fk.id AS id,
        fk.seq AS ordinal_position,
        fk.on_update,
        fk.on_delete,
        fk.match
    FROM pragma_table_list AS tl
    JOIN pragma_foreign_key_list(tl.name, COALESCE(:schema_name, 'main')) AS fk
    WHERE tl.schema = COALESCE(:schema_name, 'main')
      AND tl.type IN ('table', 'virtual')
      AND tl.name NOT LIKE 'sqlite_%'
    ORDER BY tl.name, fk.id, fk.seq;

    -- name: by_table
    -- dialect: sqlite
    SELECT
        COALESCE(:schema_name, 'main') AS schema_name,
        COALESCE(:schema_name, 'main') AS table_schema,
        :table_name AS table_name,
        fk."from" AS column_name,
        fk."table" AS referenced_table_name,
        fk."to" AS referenced_column_name,
        CASE
            WHEN COUNT(*) OVER (PARTITION BY fk.id) > 1 THEN :table_name || '_fk_' || fk.id
            ELSE NULL
        END AS constraint_name,
        fk.id AS id,
        fk.seq AS ordinal_position,
        fk.on_update,
        fk.on_delete,
        fk.match
    FROM pragma_foreign_key_list(:table_name, :schema_name) AS fk
    ORDER BY fk.id, fk.seq;
    ```

??? code "`functions.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: sqlite
    PRAGMA function_list;
    ```

??? code "`indexes.sql` (4 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        tl.schema AS schema_name,
        tl.name AS table_name,
        il.name AS index_name,
        il."unique" AS is_unique,
        CASE WHEN il.origin = 'pk' THEN 1 ELSE 0 END AS is_primary,
        il.origin,
        il.partial AS is_partial,
        ix.seqno,
        ix.cid AS column_id,
        ix.name AS column_name,
        ix.desc AS is_descending,
        ix.coll AS collation,
        ix.key AS is_key_column,
        sm.sql AS index_sql
    FROM pragma_table_list AS tl
    JOIN pragma_index_list(tl.name, COALESCE(:schema_name, 'main')) AS il
    JOIN pragma_index_xinfo(il.name, COALESCE(:schema_name, 'main')) AS ix
    LEFT JOIN {schema_prefix}sqlite_schema AS sm
      ON sm.type = 'index'
     AND sm.name = il.name
    WHERE tl.schema = COALESCE(:schema_name, 'main')
      AND tl.type IN ('table', 'virtual')
      AND tl.name NOT LIKE 'sqlite_%'
      AND il.name NOT LIKE 'sqlite_%'
    ORDER BY tl.name, il.seq, ix.seqno;

    -- name: by_table
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        :table_name AS table_name,
        il.name AS index_name,
        il."unique" AS is_unique,
        CASE WHEN il.origin = 'pk' THEN 1 ELSE 0 END AS is_primary,
        il.origin,
        il.partial AS is_partial,
        ix.seqno,
        ix.cid AS column_id,
        ix.name AS column_name,
        ix.desc AS is_descending,
        ix.coll AS collation,
        ix.key AS is_key_column,
        sm.sql AS index_sql
    FROM pragma_index_list(:table_name, :schema_name) AS il
    JOIN pragma_index_xinfo(il.name, :schema_name) AS ix
    LEFT JOIN {schema_prefix}sqlite_schema AS sm
      ON sm.type = 'index'
     AND sm.name = il.name
    WHERE il.name NOT LIKE 'sqlite_%'
    ORDER BY il.seq, ix.seqno;

    -- name: columns_by_index
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        :index_name AS index_name,
        ix.seqno,
        ix.cid AS column_id,
        ix.name AS column_name,
        ix.desc AS is_descending,
        ix.coll AS collation,
        ix.key AS is_key_column
    FROM pragma_index_xinfo(:index_name, :schema_name) AS ix
    ORDER BY ix.seqno;

    -- name: index_columns_by_index
    -- dialect: sqlite
    SELECT
        seqno,
        cid,
        name
    FROM pragma_index_info(:index_name, COALESCE(:schema_name, 'main'))
    ORDER BY seqno;
    ```

??? code "`modules.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: sqlite
    PRAGMA module_list;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        name AS object_name,
        type AS object_type,
        tbl_name AS table_name,
        rootpage,
        sql AS native_sql
    FROM {schema_prefix}sqlite_schema
    WHERE type IN ('table', 'index', 'view', 'trigger')
      AND name NOT LIKE 'sqlite_%'
    ORDER BY type, name;
    ```

??? code "`schemas.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: sqlite
    PRAGMA database_list;
    ```

??? code "`system.sql` (2 named queries)"

    ```sql
    -- name: foreign_key_check
    -- dialect: sqlite
    PRAGMA foreign_key_check;

    -- name: integrity_check
    -- dialect: sqlite
    PRAGMA integrity_check;
    ```

??? code "`tables.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    WITH table_inventory AS (
        SELECT
            tl.schema AS schema_name,
            tl.name AS table_name,
            tl.type AS table_type,
            tl.ncol,
            tl.wr,
            tl.strict
        FROM pragma_table_list AS tl
        WHERE tl.schema = COALESCE(:schema_name, 'main')
          AND tl.type IN ('table', 'virtual')
          AND tl.name NOT LIKE 'sqlite_%'
    ),
    fk_edges AS MATERIALIZED (
        SELECT
            ti.table_name,
            fk."table" AS referenced_table
        FROM table_inventory AS ti
        JOIN pragma_foreign_key_list(ti.table_name, COALESCE(:schema_name, 'main')) AS fk
        WHERE fk."table" <> ti.table_name
    ),
    dependency_tree AS (
        SELECT
            ti.table_name,
            0 AS level,
            '/' || ti.table_name || '/' AS path
        FROM table_inventory AS ti
        WHERE NOT EXISTS (
            SELECT 1
            FROM fk_edges AS e
            WHERE e.table_name = ti.table_name
        )

        UNION ALL

        SELECT
            e.table_name,
            dt.level + 1,
            dt.path || e.table_name || '/'
        FROM fk_edges AS e
        JOIN dependency_tree AS dt ON e.referenced_table = dt.table_name
        WHERE instr(dt.path, '/' || e.table_name || '/') = 0
    ),
    dependency_levels AS (
        SELECT
            table_name,
            MAX(level) AS dependency_level
        FROM dependency_tree
        GROUP BY table_name
    )
    SELECT
        ti.schema_name,
        ti.table_name,
        ti.table_type,
        COALESCE(dl.dependency_level, 0) AS dependency_level,
        COALESCE(dl.dependency_level, 0) AS level,
        ti.ncol,
        ti.wr,
        ti.strict
    FROM table_inventory AS ti
    LEFT JOIN dependency_levels AS dl ON dl.table_name = ti.table_name
    ORDER BY COALESCE(dl.dependency_level, 0), ti.table_name;
    ```

??? code "`triggers.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        name AS trigger_name,
        tbl_name AS table_name,
        sql AS native_sql
    FROM {schema_prefix}sqlite_schema
    WHERE type = 'trigger'
    ORDER BY name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: sqlite
    SELECT sqlite_version();
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: sqlite
    SELECT
        :schema_name AS schema_name,
        name AS view_name,
        tbl_name AS table_name,
        sql AS native_sql
    FROM {schema_prefix}sqlite_schema
    WHERE type = 'view'
    ORDER BY name;
    ```
