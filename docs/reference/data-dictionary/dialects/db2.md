# IBM Db2 Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("db2")
```

Introspects IBM Db2 `SYSCAT` views (`SYSCAT.TABLES`, `SYSCAT.COLUMNS`, `SYSCAT.INDEXES`, `SYSCAT.REFERENCES`, `SYSCAT.TABCONST`, `SYSCAT.VIEWS`, `SYSCAT.ROUTINES`, `SYSCAT.TRIGGERS`, `SYSCAT.SEQUENCES`, `SYSCAT.DATATYPES`). See the [IBM Db2 Guide](../../../databases/db2.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `db2` |
| **Default Schema** | `None` |
| **Version Pattern** | `(?:v|V)?(\d+)\.(\d+)(?:\.(\d+))?` |
| **DDL Strategy** | AST synthesis + `SYSCAT` catalog text definitions |
| **Catalog `.sql` Files** | `9` files (`16` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_for_update` | `True` | `bigint` | `BIGINT` |
| `supports_in_memory` | `False` | `blob` | `BLOB` |
| `supports_on_conflict` | `False` | `boolean` | `BOOLEAN` |
| `supports_prepared_statements` | `True` | `decimal` | `DECIMAL(31, 10)` |
| `supports_returning` | `False` | `float` | `DOUBLE` |
| `supports_schemas` | `True` | `integer` | `INTEGER` |
| `supports_sequences` | `True` | `json` | `CLOB` |
| `supports_skip_locked` | `True` | `text` | `CLOB` |
| `supports_transactions` | `True` | `timestamp` | `TIMESTAMP` |
| `supports_update_from` | `False` | `uuid` | `VARCHAR(36)` |
| `supports_cte` | `>= 8.1.0` | `varchar` | `VARCHAR(255)` |
| `supports_generated_columns` | `>= 8.1.0` | - | - |
| `supports_window_functions` | `>= 8.1.0` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.db2.data_dictionary.Db2SyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.db2.data_dictionary.Db2AsyncDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `db2` dialect pack defines `16` named queries across `9` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/db2/`):

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
    -- dialect: db2
    WITH unique_single_cols AS (
        SELECT DISTINCT
            RTRIM(TABSCHEMA) AS TABSCHEMA,
            RTRIM(TABNAME) AS TABNAME,
            RTRIM(SUBSTR(COLNAMES, 2)) AS COLNAME
        FROM SYSCAT.INDEXES
        WHERE UNIQUERULE IN ('U', 'P')
          AND COLCOUNT = 1
          AND TABSCHEMA NOT LIKE 'SYS%'
          AND TABSCHEMA NOT LIKE 'NULLID%'
          AND TABSCHEMA NOT LIKE 'SQLJ%'
          AND TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
          AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR TABNAME = :table_name)
    )
    SELECT
        RTRIM(c.TABSCHEMA) AS schema_name,
        RTRIM(c.TABNAME) AS table_name,
        RTRIM(c.COLNAME) AS column_name,
        RTRIM(c.TYPENAME) AS data_type,
        CASE c.NULLS WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable,
        c.DEFAULT AS column_default,
        c.COLNO + 1 AS ordinal_position,
        c.LENGTH AS max_length,
        c.SCALE AS numeric_scale,
        CASE WHEN c.KEYSEQ IS NOT NULL AND c.KEYSEQ > 0 THEN 1 ELSE 0 END AS is_primary,
        CASE WHEN u.COLNAME IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
        CASE
            WHEN c.IDENTITY = 'Y' AND c.GENERATED = 'A' THEN 'ALWAYS'
            WHEN c.IDENTITY = 'Y' AND c.GENERATED = 'D' THEN 'BY DEFAULT'
            WHEN c.IDENTITY = 'Y' THEN 'BY DEFAULT'
            ELSE NULL
        END AS identity_generation,
        CASE WHEN c.IDENTITY = 'Y' THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN c.GENERATED IN ('A', 'D') AND COALESCE(c.IDENTITY, 'N') <> 'Y' THEN 1 ELSE 0 END AS is_generated,
        c.REMARKS AS column_comment
    FROM SYSCAT.COLUMNS c
    JOIN SYSCAT.TABLES t ON c.TABSCHEMA = t.TABSCHEMA AND c.TABNAME = t.TABNAME
    LEFT JOIN unique_single_cols u
      ON u.TABSCHEMA = RTRIM(c.TABSCHEMA)
     AND u.TABNAME = RTRIM(c.TABNAME)
     AND u.COLNAME = RTRIM(c.COLNAME)
    WHERE c.TABSCHEMA NOT LIKE 'SYS%'
      AND c.TABSCHEMA NOT LIKE 'NULLID%'
      AND c.TABSCHEMA NOT LIKE 'SQLJ%'
      AND c.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR c.TABNAME = :table_name)
    ORDER BY c.TABSCHEMA, c.TABNAME, c.COLNO;

    -- name: by_table
    -- dialect: db2
    WITH unique_single_cols AS (
        SELECT DISTINCT
            RTRIM(TABSCHEMA) AS TABSCHEMA,
            RTRIM(TABNAME) AS TABNAME,
            RTRIM(SUBSTR(COLNAMES, 2)) AS COLNAME
        FROM SYSCAT.INDEXES
        WHERE UNIQUERULE IN ('U', 'P')
          AND COLCOUNT = 1
          AND TABSCHEMA NOT LIKE 'SYS%'
          AND TABSCHEMA NOT LIKE 'NULLID%'
          AND TABSCHEMA NOT LIKE 'SQLJ%'
          AND TABNAME = :table_name
          AND TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
    )
    SELECT
        RTRIM(c.TABSCHEMA) AS schema_name,
        RTRIM(c.TABNAME) AS table_name,
        RTRIM(c.COLNAME) AS column_name,
        RTRIM(c.TYPENAME) AS data_type,
        CASE c.NULLS WHEN 'Y' THEN 1 ELSE 0 END AS is_nullable,
        c.DEFAULT AS column_default,
        c.COLNO + 1 AS ordinal_position,
        c.LENGTH AS max_length,
        c.SCALE AS numeric_scale,
        CASE WHEN c.KEYSEQ IS NOT NULL AND c.KEYSEQ > 0 THEN 1 ELSE 0 END AS is_primary,
        CASE WHEN u.COLNAME IS NOT NULL THEN 1 ELSE 0 END AS is_unique,
        CASE
            WHEN c.IDENTITY = 'Y' AND c.GENERATED = 'A' THEN 'ALWAYS'
            WHEN c.IDENTITY = 'Y' AND c.GENERATED = 'D' THEN 'BY DEFAULT'
            WHEN c.IDENTITY = 'Y' THEN 'BY DEFAULT'
            ELSE NULL
        END AS identity_generation,
        CASE WHEN c.IDENTITY = 'Y' THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN c.GENERATED IN ('A', 'D') AND COALESCE(c.IDENTITY, 'N') <> 'Y' THEN 1 ELSE 0 END AS is_generated,
        c.REMARKS AS column_comment
    FROM SYSCAT.COLUMNS c
    LEFT JOIN unique_single_cols u
      ON u.TABSCHEMA = RTRIM(c.TABSCHEMA)
     AND u.TABNAME = RTRIM(c.TABNAME)
     AND u.COLNAME = RTRIM(c.COLNAME)
    WHERE c.TABSCHEMA NOT LIKE 'SYS%'
      AND c.TABSCHEMA NOT LIKE 'NULLID%'
      AND c.TABSCHEMA NOT LIKE 'SQLJ%'
      AND c.TABNAME = :table_name
      AND c.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
    ORDER BY c.COLNO;
    ```

??? code "`constraints.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    SELECT
        RTRIM(tc.TABSCHEMA) AS schema_name,
        RTRIM(tc.TABNAME) AS table_name,
        RTRIM(tc.CONSTNAME) AS constraint_name,
        CASE tc.TYPE
            WHEN 'P' THEN 'PRIMARY KEY'
            WHEN 'U' THEN 'UNIQUE'
            WHEN 'F' THEN 'FOREIGN KEY'
            WHEN 'K' THEN 'CHECK'
            ELSE tc.TYPE
        END AS constraint_type,
        RTRIM(ck.TEXT) AS check_clause,
        tc.ENFORCED AS is_enforced
    FROM SYSCAT.TABCONST tc
    LEFT JOIN SYSCAT.CHECKS ck
      ON tc.TABSCHEMA = ck.TABSCHEMA AND tc.TABNAME = ck.TABNAME AND tc.CONSTNAME = ck.CONSTNAME
    WHERE tc.TABSCHEMA NOT LIKE 'SYS%'
      AND tc.TABSCHEMA NOT LIKE 'NULLID%'
      AND tc.TABSCHEMA NOT LIKE 'SQLJ%'
      AND tc.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR tc.TABNAME = :table_name)
    ORDER BY tc.TABSCHEMA, tc.TABNAME, tc.CONSTNAME;

    -- name: by_table
    -- dialect: db2
    SELECT
        RTRIM(tc.TABSCHEMA) AS schema_name,
        RTRIM(tc.TABNAME) AS table_name,
        RTRIM(tc.CONSTNAME) AS constraint_name,
        CASE tc.TYPE
            WHEN 'P' THEN 'PRIMARY KEY'
            WHEN 'U' THEN 'UNIQUE'
            WHEN 'F' THEN 'FOREIGN KEY'
            WHEN 'K' THEN 'CHECK'
            ELSE tc.TYPE
        END AS constraint_type,
        RTRIM(ck.TEXT) AS check_clause,
        tc.ENFORCED AS is_enforced
    FROM SYSCAT.TABCONST tc
    LEFT JOIN SYSCAT.CHECKS ck
      ON tc.TABSCHEMA = ck.TABSCHEMA AND tc.TABNAME = ck.TABNAME AND tc.CONSTNAME = ck.CONSTNAME
    WHERE tc.TABSCHEMA NOT LIKE 'SYS%'
      AND tc.TABSCHEMA NOT LIKE 'NULLID%'
      AND tc.TABSCHEMA NOT LIKE 'SQLJ%'
      AND tc.TABNAME = :table_name
      AND tc.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
    ORDER BY tc.CONSTNAME;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    SELECT
        RTRIM(r.TABSCHEMA) AS schema_name,
        RTRIM(r.TABNAME) AS table_name,
        RTRIM(r.CONSTNAME) AS constraint_name,
        RTRIM(k.COLNAME) AS column_name,
        RTRIM(r.REFTABSCHEMA) AS referenced_schema,
        RTRIM(r.REFTABNAME) AS referenced_table,
        RTRIM(refk.COLNAME) AS referenced_column
    FROM SYSCAT.REFERENCES r
    JOIN SYSCAT.KEYCOLUSE k
      ON r.TABSCHEMA = k.TABSCHEMA AND r.CONSTNAME = k.CONSTNAME
    JOIN SYSCAT.KEYCOLUSE refk
      ON r.REFTABSCHEMA = refk.TABSCHEMA AND r.REFKEYNAME = refk.CONSTNAME AND k.COLSEQ = refk.COLSEQ
    WHERE r.TABSCHEMA NOT LIKE 'SYS%'
      AND r.TABSCHEMA NOT LIKE 'NULLID%'
      AND r.TABSCHEMA NOT LIKE 'SQLJ%'
      AND r.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR r.TABNAME = :table_name)
    ORDER BY r.TABSCHEMA, r.TABNAME, r.CONSTNAME, k.COLSEQ;

    -- name: by_table
    -- dialect: db2
    SELECT
        RTRIM(r.TABSCHEMA) AS schema_name,
        RTRIM(r.TABNAME) AS table_name,
        RTRIM(r.CONSTNAME) AS constraint_name,
        RTRIM(k.COLNAME) AS column_name,
        RTRIM(r.REFTABSCHEMA) AS referenced_schema,
        RTRIM(r.REFTABNAME) AS referenced_table,
        RTRIM(refk.COLNAME) AS referenced_column
    FROM SYSCAT.REFERENCES r
    JOIN SYSCAT.KEYCOLUSE k
      ON r.TABSCHEMA = k.TABSCHEMA AND r.CONSTNAME = k.CONSTNAME
    JOIN SYSCAT.KEYCOLUSE refk
      ON r.REFTABSCHEMA = refk.TABSCHEMA AND r.REFKEYNAME = refk.CONSTNAME AND k.COLSEQ = refk.COLSEQ
    WHERE r.TABSCHEMA NOT LIKE 'SYS%'
      AND r.TABSCHEMA NOT LIKE 'NULLID%'
      AND r.TABSCHEMA NOT LIKE 'SQLJ%'
      AND r.TABNAME = :table_name
      AND r.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
    ORDER BY r.CONSTNAME, k.COLSEQ;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    SELECT
        RTRIM(i.TABSCHEMA) AS schema_name,
        RTRIM(i.TABNAME) AS table_name,
        RTRIM(i.INDNAME) AS index_name,
        RTRIM(c.COLNAME) AS column_name,
        c.COLSEQ AS column_position,
        CASE i.UNIQUERULE WHEN 'D' THEN 0 ELSE 1 END AS is_unique,
        CASE i.UNIQUERULE WHEN 'P' THEN 1 ELSE 0 END AS is_primary
    FROM SYSCAT.INDEXES i
    JOIN SYSCAT.INDEXCOLUSE c
      ON i.INDSCHEMA = c.INDSCHEMA AND i.INDNAME = c.INDNAME
    WHERE i.TABSCHEMA NOT LIKE 'SYS%'
      AND i.TABSCHEMA NOT LIKE 'NULLID%'
      AND i.TABSCHEMA NOT LIKE 'SQLJ%'
      AND i.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR i.TABNAME = :table_name)
    ORDER BY i.TABSCHEMA, i.TABNAME, i.INDNAME, c.COLSEQ;

    -- name: by_table
    -- dialect: db2
    SELECT
        RTRIM(i.TABSCHEMA) AS schema_name,
        RTRIM(i.TABNAME) AS table_name,
        RTRIM(i.INDNAME) AS index_name,
        RTRIM(c.COLNAME) AS column_name,
        c.COLSEQ AS column_position,
        CASE i.UNIQUERULE WHEN 'D' THEN 0 ELSE 1 END AS is_unique,
        CASE i.UNIQUERULE WHEN 'P' THEN 1 ELSE 0 END AS is_primary
    FROM SYSCAT.INDEXES i
    JOIN SYSCAT.INDEXCOLUSE c
      ON i.INDSCHEMA = c.INDSCHEMA AND i.INDNAME = c.INDNAME
    WHERE i.TABSCHEMA NOT LIKE 'SYS%'
      AND i.TABSCHEMA NOT LIKE 'NULLID%'
      AND i.TABSCHEMA NOT LIKE 'SQLJ%'
      AND i.TABNAME = :table_name
      AND i.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
    ORDER BY i.INDNAME, c.COLSEQ;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    SELECT schema_name, object_name, object_type, created, remarks
    FROM (
        SELECT
            RTRIM(t.TABSCHEMA) AS schema_name,
            RTRIM(t.TABNAME) AS object_name,
            CASE t.TYPE
                WHEN 'T' THEN 'TABLE'
                WHEN 'V' THEN 'VIEW'
                WHEN 'A' THEN 'ALIAS'
                WHEN 'S' THEN 'MATERIALIZED QUERY TABLE'
                WHEN 'N' THEN 'NICKNAME'
                WHEN 'G' THEN 'CREATED TEMPORARY TABLE'
                ELSE t.TYPE
            END AS object_type,
            t.CREATE_TIME AS created,
            t.REMARKS AS remarks
        FROM SYSCAT.TABLES t
        WHERE t.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
          AND (CAST(:object_name AS VARCHAR(128)) IS NULL OR t.TABNAME = :object_name)

        UNION ALL

        SELECT
            RTRIM(s.SEQSCHEMA) AS schema_name,
            RTRIM(s.SEQNAME) AS object_name,
            'SEQUENCE' AS object_type,
            s.CREATE_TIME AS created,
            s.REMARKS AS remarks
        FROM SYSCAT.SEQUENCES s
        WHERE s.SEQTYPE = 'S'
          AND s.SEQSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
          AND (CAST(:object_name AS VARCHAR(128)) IS NULL OR s.SEQNAME = :object_name)

        UNION ALL

        SELECT
            RTRIM(r.ROUTINESCHEMA) AS schema_name,
            RTRIM(r.ROUTINENAME) AS object_name,
            CASE r.ROUTINETYPE
                WHEN 'F' THEN 'FUNCTION'
                WHEN 'P' THEN 'PROCEDURE'
                WHEN 'M' THEN 'METHOD'
                ELSE r.ROUTINETYPE
            END AS object_type,
            r.CREATE_TIME AS created,
            r.REMARKS AS remarks
        FROM SYSCAT.ROUTINES r
        WHERE r.ROUTINESCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
          AND (CAST(:object_name AS VARCHAR(128)) IS NULL OR r.ROUTINENAME = :object_name)
    ) objects
    ORDER BY schema_name, object_type, object_name;
    ```

??? code "`schemas.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    SELECT
        RTRIM(s.SCHEMANAME) AS schema_name,
        RTRIM(s.OWNER) AS owner_name,
        s.CREATE_TIME AS create_time,
        s.REMARKS AS comment
    FROM SYSCAT.SCHEMATA s
    WHERE s.SCHEMANAME NOT LIKE 'SYS%'
      AND s.SCHEMANAME NOT LIKE 'NULLID%'
      AND s.SCHEMANAME NOT LIKE 'SQLJ%'
      AND (:schema_name IS NULL OR s.SCHEMANAME = :schema_name)
    ORDER BY s.SCHEMANAME;

    -- name: current
    -- dialect: db2
    VALUES CURRENT SCHEMA;
    ```

??? code "`tables.sql` (3 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    WITH dependency_tree (schema_name, table_name, level, path) AS (
        SELECT
            RTRIM(t.TABSCHEMA) AS schema_name,
            RTRIM(t.TABNAME) AS table_name,
            0 AS level,
            VARCHAR('/' || RTRIM(t.TABSCHEMA) || '.' || RTRIM(t.TABNAME) || '/', 2000) AS path
        FROM SYSCAT.TABLES t
        WHERE t.TYPE = 'T'
          AND t.TABSCHEMA NOT LIKE 'SYS%'
          AND t.TABSCHEMA NOT LIKE 'NULLID%'
          AND t.TABSCHEMA NOT LIKE 'SQLJ%'
          AND t.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
          AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR t.TABNAME = :table_name)
          AND NOT EXISTS (
              SELECT 1
              FROM SYSCAT.REFERENCES r
              WHERE r.TABSCHEMA = t.TABSCHEMA
                AND r.TABNAME = t.TABNAME
                AND (r.REFTABSCHEMA <> t.TABSCHEMA OR r.REFTABNAME <> t.TABNAME)
          )

        UNION ALL

        SELECT
            RTRIM(child.TABSCHEMA) AS schema_name,
            RTRIM(child.TABNAME) AS table_name,
            dt.level + 1 AS level,
            VARCHAR(dt.path || RTRIM(child.TABSCHEMA) || '.' || RTRIM(child.TABNAME) || '/', 2000) AS path
        FROM SYSCAT.REFERENCES r, SYSCAT.TABLES child, dependency_tree dt
        WHERE r.TABSCHEMA = child.TABSCHEMA
          AND r.TABNAME = child.TABNAME
          AND r.REFTABSCHEMA = dt.schema_name
          AND r.REFTABNAME = dt.table_name
          AND (r.REFTABSCHEMA <> child.TABSCHEMA OR r.REFTABNAME <> child.TABNAME)
          AND child.TYPE = 'T'
          AND child.TABSCHEMA NOT LIKE 'SYS%'
          AND child.TABSCHEMA NOT LIKE 'NULLID%'
          AND child.TABSCHEMA NOT LIKE 'SQLJ%'
          AND child.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
          AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR child.TABNAME = :table_name)
          AND LOCATE('/' || RTRIM(child.TABSCHEMA) || '.' || RTRIM(child.TABNAME) || '/', dt.path) = 0
    )
    SELECT
        dt.schema_name,
        dt.table_name,
        'BASE TABLE' AS table_type,
        MAX(dt.level) AS dependency_level,
        MAX(dt.level) AS level
    FROM dependency_tree dt
    GROUP BY dt.schema_name, dt.table_name
    ORDER BY dependency_level, dt.schema_name, dt.table_name;

    -- name: all_by_schema
    -- dialect: db2
    SELECT
        RTRIM(t.TABSCHEMA) AS schema_name,
        RTRIM(t.TABNAME) AS table_name,
        CASE t.TYPE
            WHEN 'T' THEN 'BASE TABLE'
            WHEN 'V' THEN 'VIEW'
            WHEN 'A' THEN 'ALIAS'
            WHEN 'N' THEN 'NICKNAME'
            ELSE 'TABLE'
        END AS table_type,
        0 AS dependency_level,
        0 AS level,
        t.CARD AS estimated_row_count,
        t.REMARKS AS table_comment
    FROM SYSCAT.TABLES t
    WHERE t.TABSCHEMA NOT LIKE 'SYS%'
      AND t.TABSCHEMA NOT LIKE 'NULLID%'
      AND t.TABSCHEMA NOT LIKE 'SQLJ%'
      AND t.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:table_name AS VARCHAR(128)) IS NULL OR t.TABNAME = :table_name)
    ORDER BY t.TABSCHEMA, t.TABNAME;

    -- name: names_by_schema
    -- dialect: db2
    SELECT
        RTRIM(t.TABNAME) AS table_name
    FROM SYSCAT.TABLES t
    WHERE t.TYPE = 'T'
      AND t.TABSCHEMA NOT LIKE 'SYS%'
      AND t.TABSCHEMA NOT LIKE 'NULLID%'
      AND t.TABSCHEMA NOT LIKE 'SQLJ%'
      AND t.TABSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
    ORDER BY t.TABNAME;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: db2
    SELECT
        SERVICE_LEVEL AS service_level,
        BLD_LEVEL AS bld_level,
        FIXPACK_NUM AS fixpack_num,
        INST_NAME AS inst_name
    FROM TABLE(SYSPROC.ENV_GET_INST_INFO()) AS T;
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: db2
    SELECT
        RTRIM(v.VIEWSCHEMA) AS schema_name,
        RTRIM(v.VIEWNAME) AS view_name,
        v.TEXT AS definition,
        CASE v.READONLY WHEN 'Y' THEN 1 ELSE 0 END AS is_read_only,
        CASE v.VALID WHEN 'Y' THEN 1 ELSE 0 END AS is_valid
    FROM SYSCAT.VIEWS v
    WHERE v.VIEWSCHEMA NOT LIKE 'SYS%'
      AND v.VIEWSCHEMA NOT LIKE 'NULLID%'
      AND v.VIEWSCHEMA NOT LIKE 'SQLJ%'
      AND v.VIEWSCHEMA = COALESCE(CAST(:schema_name AS VARCHAR(128)), CURRENT SCHEMA)
      AND (CAST(:view_name AS VARCHAR(128)) IS NULL OR v.VIEWNAME = :view_name)
    ORDER BY v.VIEWSCHEMA, v.VIEWNAME;
    ```
