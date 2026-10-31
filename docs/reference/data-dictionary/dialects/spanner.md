# Google Cloud Spanner Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("spanner")
```

Provides dual SQL catalog packs (`googlesql` and `postgresql`) introspecting `INFORMATION_SCHEMA` tables, columns, indexes (`STORING`, `SEARCH`, `VECTOR`), foreign keys, parent-child interleaving, and row deletion policies. See the [Google Cloud Spanner Guide](../../../databases/spanner.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `spanner` |
| **Default Schema** | `None` |
| **Version Pattern** | `.*` |
| **DDL Strategy** | AST synthesis with `INTERLEAVE IN PARENT` & `ROW DELETION POLICY` (`GoogleSQL` & `PostgreSQL` modes) |
| **Catalog `.sql` Files** | `5` files (`8` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_for_update` | `False` | `array` | `ARRAY` |
| `supports_generators` | `False` | `bigint` | `INT64` |
| `supports_index_clustering` | `True` | `bignumeric` | `NUMERIC` |
| `supports_interleaved_tables` | `True` | `blob` | `BYTES(MAX)` |
| `supports_json` | `True` | `boolean` | `BOOL` |
| `supports_on_conflict` | `True` | `float` | `FLOAT64` |
| `supports_skip_locked` | `False` | `float32` | `FLOAT32` |
| `supports_update_from` | `False` | `integer` | `INT64` |
| - | - | `json` | `JSON` |
| - | - | `numeric` | `NUMERIC` |
| - | - | `text` | `STRING(MAX)` |
| - | - | `timestamp` | `TIMESTAMP` |
| - | - | `tokenlist` | `TOKENLIST` |
| - | - | `uuid` | `BYTES(16)` |
| - | - | `varchar` | `STRING({length})` |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.spanner.data_dictionary.SpannerSyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.spanner.data_dictionary.SpannerAsyncDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `spanner` dialect pack defines `8` named queries across `5` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/spanner/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `tables.sql` | `by_schema` | `:schema_name` |
| `version.sql` | `current` | *none* |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: spanner
    WITH pk_cols AS (
        SELECT
            ic.table_schema,
            ic.table_name,
            ic.column_name
        FROM information_schema.index_columns AS ic
        WHERE ic.index_name = 'PRIMARY_KEY'
          AND ic.table_name = :table_name
          AND (CAST(:schema_name AS STRING) IS NULL OR ic.table_schema = :schema_name)
    )
    SELECT
        c.column_name,
        c.spanner_type AS data_type,
        c.is_nullable,
        c.column_default,
        CASE WHEN pk_cols.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary,
        CASE WHEN c.is_identity = 'YES' THEN TRUE ELSE FALSE END AS is_identity,
        CASE WHEN c.is_generated = 'ALWAYS' OR c.generation_expression IS NOT NULL THEN TRUE ELSE FALSE END AS is_generated,
        c.generation_expression
    FROM information_schema.columns AS c
    LEFT JOIN pk_cols
        ON c.table_schema = pk_cols.table_schema
        AND c.table_name = pk_cols.table_name
        AND c.column_name = pk_cols.column_name
    WHERE c.table_name = :table_name
      AND (CAST(:schema_name AS STRING) IS NULL OR c.table_schema = :schema_name)
    ORDER BY c.ordinal_position;

    -- name: by_schema
    -- dialect: spanner
    WITH pk_cols AS (
        SELECT
            ic.table_schema,
            ic.table_name,
            ic.column_name
        FROM information_schema.index_columns AS ic
        WHERE ic.index_name = 'PRIMARY_KEY'
          AND (CAST(:schema_name AS STRING) IS NULL OR ic.table_schema = :schema_name)
    )
    SELECT
        c.table_name,
        c.column_name,
        c.spanner_type AS data_type,
        c.is_nullable,
        c.column_default,
        CASE WHEN pk_cols.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS is_primary,
        CASE WHEN c.is_identity = 'YES' THEN TRUE ELSE FALSE END AS is_identity,
        CASE WHEN c.is_generated = 'ALWAYS' OR c.generation_expression IS NOT NULL THEN TRUE ELSE FALSE END AS is_generated,
        c.generation_expression
    FROM information_schema.columns AS c
    LEFT JOIN pk_cols
        ON c.table_schema = pk_cols.table_schema
        AND c.table_name = pk_cols.table_name
        AND c.column_name = pk_cols.column_name
    WHERE (CAST(:schema_name AS STRING) IS NULL OR c.table_schema = :schema_name)
    ORDER BY c.table_name, c.ordinal_position;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: spanner
    WITH fk_constraints AS (
        SELECT
            constraint_name,
            unique_constraint_name
        FROM information_schema.referential_constraints
        WHERE (CAST(:schema_name AS STRING) IS NULL OR constraint_schema = :schema_name)
    ),
    fk_columns AS (
        SELECT
            constraint_name,
            table_name,
            column_name,
            ordinal_position,
            table_schema
        FROM information_schema.key_column_usage
        WHERE (CAST(:schema_name AS STRING) IS NULL OR constraint_schema = :schema_name)
          AND (CAST(:table_name AS STRING) IS NULL OR table_name = :table_name)
    ),
    pk_columns AS (
        SELECT
            constraint_name,
            table_name,
            column_name,
            ordinal_position,
            table_schema
        FROM information_schema.key_column_usage
        WHERE (CAST(:schema_name AS STRING) IS NULL OR constraint_schema = :schema_name)
    )
    SELECT
        fk.table_name,
        fk.column_name,
        pk.table_name AS referenced_table_name,
        pk.column_name AS referenced_column_name,
        fk.constraint_name,
        fk.table_schema AS table_schema,
        pk.table_schema AS referenced_table_schema
    FROM fk_columns fk
    JOIN fk_constraints rc
      ON fk.constraint_name = rc.constraint_name
    JOIN pk_columns pk
      ON pk.constraint_name = rc.unique_constraint_name
      AND pk.ordinal_position = fk.ordinal_position
    WHERE (CAST(:table_name AS STRING) IS NULL OR fk.table_name = :table_name)
    ORDER BY fk.table_name, fk.ordinal_position;

    -- name: by_schema
    -- dialect: spanner
    WITH fk_constraints AS (
        SELECT
            constraint_name,
            unique_constraint_name
        FROM information_schema.referential_constraints
        WHERE (CAST(:schema_name AS STRING) IS NULL OR constraint_schema = :schema_name)
    ),
    fk_columns AS (
        SELECT
            constraint_name,
            table_name,
            column_name,
            ordinal_position,
            table_schema
        FROM information_schema.key_column_usage
        WHERE (CAST(:schema_name AS STRING) IS NULL OR constraint_schema = :schema_name)
    ),
    pk_columns AS (
        SELECT
            constraint_name,
            table_name,
            column_name,
            ordinal_position,
            table_schema
        FROM information_schema.key_column_usage
        WHERE (CAST(:schema_name AS STRING) IS NULL OR constraint_schema = :schema_name)
    )
    SELECT
        fk.table_name,
        fk.column_name,
        pk.table_name AS referenced_table_name,
        pk.column_name AS referenced_column_name,
        fk.constraint_name,
        fk.table_schema AS table_schema,
        pk.table_schema AS referenced_table_schema
    FROM fk_columns fk
    JOIN fk_constraints rc
      ON fk.constraint_name = rc.constraint_name
    JOIN pk_columns pk
      ON pk.constraint_name = rc.unique_constraint_name
      AND pk.ordinal_position = fk.ordinal_position
    ORDER BY fk.table_name, fk.ordinal_position;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: spanner
    SELECT
        i.index_name,
        i.table_name,
        i.is_unique AS is_unique,
        i.is_primary_key AS is_primary,
        ARRAY_AGG(ic.column_name ORDER BY ic.ordinal_position) AS columns
    FROM information_schema.indexes i
    JOIN information_schema.index_columns ic
      ON i.index_name = ic.index_name
      AND i.table_name = ic.table_name
    WHERE i.table_name = :table_name
      AND (CAST(:schema_name AS STRING) IS NULL OR i.table_schema = :schema_name)
    GROUP BY i.index_name, i.table_name, i.is_unique, i.is_primary_key;

    -- name: by_schema
    -- dialect: spanner
    SELECT
        i.index_name,
        i.table_name,
        i.is_unique AS is_unique,
        i.is_primary_key AS is_primary,
        ARRAY_AGG(ic.column_name ORDER BY ic.ordinal_position) AS columns
    FROM information_schema.indexes i
    JOIN information_schema.index_columns ic
      ON i.index_name = ic.index_name
      AND i.table_name = ic.table_name
    WHERE (CAST(:schema_name AS STRING) IS NULL OR i.table_schema = :schema_name)
    GROUP BY i.index_name, i.table_name, i.is_unique, i.is_primary_key;
    ```

??? code "`tables.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: spanner
    SELECT
        table_name,
        0 AS level
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND (CAST(:schema_name AS STRING) IS NULL OR table_schema = :schema_name)
    ORDER BY table_name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: spanner
    SELECT NULL AS version;
    ```
