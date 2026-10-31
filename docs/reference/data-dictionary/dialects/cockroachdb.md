# CockroachDB Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("cockroachdb")
```

Introspects `crdb_internal` (`create_statements`, `tables`) and `pg_catalog` compatibility views across `cockroach_asyncpg` and `cockroach_psycopg`. See the [CockroachDB Guide](../../../databases/cockroachdb.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `cockroachdb` |
| **Default Schema** | `"public"` |
| **Version Pattern** | `CockroachDB (?:CCL )?v(\d+)\.(\d+)\.(\d+)` |
| **DDL Strategy** | Native `crdb_internal.create_statement` + AST fallback |
| **Catalog `.sql` Files** | `15` files (`18` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_crdb_internal_metadata` | `False` | `bigint` | `INT8` |
| `supports_for_update` | `True` | `blob` | `BYTES` |
| `supports_on_conflict` | `True` | `boolean` | `BOOL` |
| `supports_prepared_statements` | `True` | `float` | `FLOAT8` |
| `supports_schemas` | `True` | `integer` | `INT8` |
| `supports_skip_locked` | `True` | `json` | `JSONB` |
| `supports_transactions` | `True` | `text` | `STRING` |
| `supports_update_from` | `True` | `timestamp` | `TIMESTAMPTZ` |
| `supports_uuid` | `True` | `uuid` | `UUID` |
| `supports_cte` | `>= 19.1.0` | `varchar` | `VARCHAR({length})` |
| `supports_json` | `>= 20.1.0` | - | - |
| `supports_returning` | `>= 20.1.0` | - | - |
| `supports_upsert` | `>= 19.2.0` | - | - |
| `supports_window_functions` | `>= 19.1.0` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.cockroach_asyncpg.data_dictionary.CockroachAsyncpgDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgSyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.cockroach_psycopg.data_dictionary.CockroachPsycopgAsyncDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `cockroachdb` dialect pack defines `18` named queries across `15` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/cockroachdb/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `comments.sql` | `by_schema` | `:schema_name` |
| `constraints.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `crdb_internal.sql` | `ranges` | `:schema_name`, `:table_name` |
| `ddl.sql` | `by_object` | `:object_name`, `:object_type`, `:schema_name` |
| `dependencies.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `objects.sql` | `by_schema` | `:schema_name` |
| `privileges.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `schemas.sql` | `list` | *none* |
| `sequences.sql` | `by_schema` | `:schema_name` |
| `tables.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `version.sql` | `current` | *none* |
| `views.sql` | `by_schema` | `:schema_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    WITH target_schema AS (
        SELECT COALESCE(
            :schema_name::text,
            (
                SELECT rn.nspname::text
                FROM pg_catalog.pg_class rc
                JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
                WHERE rc.oid = pg_catalog.to_regclass(pg_catalog.quote_ident(:table_name::text))
            ),
            current_schema()
        ) AS schema_name
    ),
    pk_columns AS (
        SELECT DISTINCT
            tc.table_schema,
            tc.table_name,
            kcu.column_name
        FROM target_schema ts
        JOIN information_schema.table_constraints tc
          ON tc.table_schema = ts.schema_name
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
         AND kcu.table_name = tc.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND (:table_name::text IS NULL OR tc.table_name = :table_name)
    ),
    identity_attrs AS (
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            a.attname AS column_name,
            CASE
                WHEN a.attidentity = 'a' THEN 'ALWAYS'
                WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
                ELSE NULL
            END AS identity_generation,
            CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity
        FROM target_schema ts
        JOIN pg_catalog.pg_namespace n ON n.nspname = ts.schema_name
        JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE a.attnum > 0
          AND NOT a.attisdropped
          AND (:table_name::text IS NULL OR c.relname = :table_name)
    )
    SELECT
        c.table_catalog::text AS table_catalog,
        c.table_schema::text AS table_schema,
        c.table_schema::text AS schema_name,
        c.table_name::text AS table_name,
        c.column_name::text AS column_name,
        c.ordinal_position,
        c.data_type::text AS data_type,
        c.is_nullable::text AS is_nullable,
        c.column_default::text AS column_default,
        c.generation_expression::text AS generation_expression,
        CASE WHEN c.generation_expression IS NOT NULL AND c.generation_expression <> '' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN c.is_hidden = 'YES' THEN 1 ELSE 0 END AS is_hidden,
        CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        ia.identity_generation,
        COALESCE(ia.is_identity, 0) AS is_identity,
        pg_catalog.pg_get_serial_sequence(
            pg_catalog.quote_ident(c.table_schema) || '.' || pg_catalog.quote_ident(c.table_name), c.column_name
        )::text AS sequence_name
    FROM target_schema ts
    JOIN information_schema.columns AS c
      ON c.table_schema = ts.schema_name
    LEFT JOIN pk_columns pk
      ON pk.table_schema = c.table_schema
     AND pk.table_name = c.table_name
     AND pk.column_name = c.column_name
    LEFT JOIN identity_attrs ia
      ON ia.table_schema = c.table_schema
     AND ia.table_name = c.table_name
     AND ia.column_name = c.column_name
    WHERE (:table_name::text IS NULL OR c.table_name = :table_name)
    ORDER BY c.table_schema, c.table_name, c.ordinal_position;

    -- name: by_table
    -- dialect: cockroachdb
    WITH target_schema AS (
        SELECT COALESCE(
            :schema_name::text,
            (
                SELECT rn.nspname::text
                FROM pg_catalog.pg_class rc
                JOIN pg_catalog.pg_namespace rn ON rn.oid = rc.relnamespace
                WHERE rc.oid = pg_catalog.to_regclass(pg_catalog.quote_ident(:table_name::text))
            ),
            current_schema()
        ) AS schema_name
    ),
    pk_columns AS (
        SELECT DISTINCT
            tc.table_schema,
            tc.table_name,
            kcu.column_name
        FROM target_schema ts
        JOIN information_schema.table_constraints tc
          ON tc.table_schema = ts.schema_name
        JOIN information_schema.key_column_usage kcu
          ON kcu.constraint_schema = tc.constraint_schema
         AND kcu.constraint_name = tc.constraint_name
         AND kcu.table_name = tc.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_name = :table_name
    ),
    identity_attrs AS (
        SELECT
            n.nspname AS table_schema,
            c.relname AS table_name,
            a.attname AS column_name,
            CASE
                WHEN a.attidentity = 'a' THEN 'ALWAYS'
                WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
                ELSE NULL
            END AS identity_generation,
            CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity
        FROM target_schema ts
        JOIN pg_catalog.pg_namespace n ON n.nspname = ts.schema_name
        JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE a.attnum > 0
          AND NOT a.attisdropped
          AND c.relname = :table_name
    )
    SELECT
        c.table_catalog::text AS table_catalog,
        c.table_schema::text AS table_schema,
        c.table_schema::text AS schema_name,
        c.table_name::text AS table_name,
        c.column_name::text AS column_name,
        c.ordinal_position,
        c.data_type::text AS data_type,
        c.is_nullable::text AS is_nullable,
        c.column_default::text AS column_default,
        c.generation_expression::text AS generation_expression,
        CASE WHEN c.generation_expression IS NOT NULL AND c.generation_expression <> '' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN c.is_hidden = 'YES' THEN 1 ELSE 0 END AS is_hidden,
        CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        ia.identity_generation,
        COALESCE(ia.is_identity, 0) AS is_identity,
        pg_catalog.pg_get_serial_sequence(
            pg_catalog.quote_ident(c.table_schema) || '.' || pg_catalog.quote_ident(c.table_name), c.column_name
        )::text AS sequence_name
    FROM target_schema ts
    JOIN information_schema.columns AS c
      ON c.table_schema = ts.schema_name
    LEFT JOIN pk_columns pk
      ON pk.table_schema = c.table_schema
     AND pk.table_name = c.table_name
     AND pk.column_name = c.column_name
    LEFT JOIN identity_attrs ia
      ON ia.table_schema = c.table_schema
     AND ia.table_name = c.table_name
     AND ia.column_name = c.column_name
    WHERE c.table_name = :table_name
    ORDER BY c.ordinal_position;
    ```

??? code "`comments.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        table_schema::text AS schema_name,
        table_name::text AS object_name,
        'table'::text AS object_type,
        NULL::text AS comment
    FROM information_schema.tables
    WHERE table_schema = :schema_name
    ORDER BY table_schema, table_name;
    ```

??? code "`constraints.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        tc.constraint_catalog::text AS constraint_catalog,
        tc.constraint_schema::text AS constraint_schema,
        tc.table_schema::text AS schema_name,
        tc.table_name::text AS table_name,
        tc.constraint_name::text AS constraint_name,
        tc.constraint_type::text AS constraint_type,
        tc.is_deferrable::text AS is_deferrable,
        tc.initially_deferred::text AS initially_deferred,
        ARRAY(
            SELECT kcu.column_name::text
            FROM information_schema.key_column_usage kcu
            WHERE kcu.constraint_catalog = tc.constraint_catalog
              AND kcu.constraint_schema = tc.constraint_schema
              AND kcu.constraint_name = tc.constraint_name
              AND kcu.table_schema = tc.table_schema
              AND kcu.table_name = tc.table_name
            ORDER BY kcu.ordinal_position
        )::text[] AS columns,
        rc.unique_constraint_schema::text AS referenced_schema,
        pk_tc.table_name::text AS referenced_table,
        ARRAY(
            SELECT pk_kcu.column_name::text
            FROM information_schema.key_column_usage pk_kcu
            WHERE pk_kcu.constraint_catalog = rc.unique_constraint_catalog
              AND pk_kcu.constraint_schema = rc.unique_constraint_schema
              AND pk_kcu.constraint_name = rc.unique_constraint_name
            ORDER BY pk_kcu.ordinal_position
        )::text[] AS referenced_columns
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.referential_constraints rc
      ON rc.constraint_catalog = tc.constraint_catalog
     AND rc.constraint_schema = tc.constraint_schema
     AND rc.constraint_name = tc.constraint_name
    LEFT JOIN information_schema.table_constraints pk_tc
      ON pk_tc.constraint_catalog = rc.unique_constraint_catalog
     AND pk_tc.constraint_schema = rc.unique_constraint_schema
     AND pk_tc.constraint_name = rc.unique_constraint_name
    WHERE tc.table_schema = :schema_name
      AND (:table_name::text IS NULL OR tc.table_name = :table_name)
    ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
    ```

??? code "`crdb_internal.sql` (1 named queries)"

    ```sql
    -- name: ranges
    -- dialect: cockroachdb
    SELECT
        database_name::text AS database_name,
        schema_name::text AS schema_name,
        table_name::text AS table_name,
        range_id::text AS range_id
    FROM crdb_internal.ranges
    WHERE (:schema_name::text IS NULL OR schema_name = :schema_name)
      AND (:table_name::text IS NULL OR table_name = :table_name)
    ORDER BY database_name, schema_name, table_name, range_id;
    ```

??? code "`ddl.sql` (1 named queries)"

    ```sql
    -- name: by_object
    -- dialect: cockroachdb
    SELECT
        :schema_name::text AS schema_name,
        :object_name::text AS object_name,
        :object_type::text AS object_type,
        NULL::text AS ddl,
        'lossy'::text AS fidelity,
        'SHOW CREATE requires a safely quoted identifier and is not emitted by the bind-only query pack'::text AS warning
    WHERE :object_name::text IS NOT NULL;
    ```

??? code "`dependencies.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        tc.table_schema::text AS schema_name,
        tc.table_name::text AS object_name,
        'table'::text AS object_type,
        ccu.table_schema::text AS referenced_schema,
        ccu.table_name::text AS referenced_object,
        'table'::text AS referenced_object_type,
        'foreign_key'::text AS dependency_type,
        tc.constraint_name::text AS constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON ccu.constraint_catalog = tc.constraint_catalog
     AND ccu.constraint_schema = tc.constraint_schema
     AND ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = :schema_name
      AND (:object_name::text IS NULL OR tc.table_name = :object_name)
    ORDER BY tc.table_schema, tc.table_name, tc.constraint_name;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: cockroachdb
    SELECT
        tc.constraint_name::text AS constraint_name,
        kcu.column_name::text AS column_name,
        pk_kcu.table_name::text AS referenced_table,
        pk_kcu.column_name::text AS referenced_column,
        tc.table_name::text AS table_name,
        tc.table_schema::text AS table_schema,
        pk_kcu.table_schema::text AS referenced_schema,
        pk_kcu.table_schema::text AS referenced_table_schema,
        rc.delete_rule::text AS on_delete,
        rc.update_rule::text AS on_update
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name
    JOIN information_schema.referential_constraints rc
      ON tc.constraint_catalog = rc.constraint_catalog
      AND tc.constraint_schema = rc.constraint_schema
      AND tc.constraint_name = rc.constraint_name
    JOIN information_schema.key_column_usage pk_kcu
      ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
      AND rc.unique_constraint_name = pk_kcu.constraint_name
      AND COALESCE(kcu.position_in_unique_constraint, kcu.ordinal_position) = pk_kcu.ordinal_position
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = :schema_name
      AND tc.table_name = :table_name
    ORDER BY tc.constraint_name, kcu.ordinal_position;

    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        tc.constraint_name::text AS constraint_name,
        kcu.column_name::text AS column_name,
        pk_kcu.table_name::text AS referenced_table,
        pk_kcu.column_name::text AS referenced_column,
        tc.table_name::text AS table_name,
        tc.table_schema::text AS table_schema,
        pk_kcu.table_schema::text AS referenced_schema,
        pk_kcu.table_schema::text AS referenced_table_schema,
        rc.delete_rule::text AS on_delete,
        rc.update_rule::text AS on_update
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name
    JOIN information_schema.referential_constraints rc
      ON tc.constraint_catalog = rc.constraint_catalog
      AND tc.constraint_schema = rc.constraint_schema
      AND tc.constraint_name = rc.constraint_name
    JOIN information_schema.key_column_usage pk_kcu
      ON rc.unique_constraint_catalog = pk_kcu.constraint_catalog
      AND rc.unique_constraint_schema = pk_kcu.constraint_schema
      AND rc.unique_constraint_name = pk_kcu.constraint_name
      AND COALESCE(kcu.position_in_unique_constraint, kcu.ordinal_position) = pk_kcu.ordinal_position
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND tc.table_schema = :schema_name
    ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        table_schema::text AS schema_name,
        table_name::text AS table_name,
        index_name::text AS index_name,
        array_agg(column_name::text ORDER BY seq_in_index::int) AS columns,
        (non_unique = false OR non_unique::text IN ('0', 'f', 'false', 'NO')) AS is_unique,
        index_name = 'primary' AS is_primary
    FROM information_schema.statistics
    WHERE table_schema = :schema_name
      AND (:table_name::text IS NULL OR table_name = :table_name)
    GROUP BY table_schema, table_name, index_name, non_unique
    ORDER BY table_schema, table_name, index_name;

    -- name: by_table
    -- dialect: cockroachdb
    SELECT
        i.relname::text AS index_name,
        t.relname::text AS table_name,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        array_agg(a.attname::text ORDER BY array_position(ix.indkey, a.attnum)) AS columns
    FROM pg_catalog.pg_class t
    JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
    JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
    JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
    WHERE t.relname = :table_name
      AND n.nspname = :schema_name
    GROUP BY t.relname, i.relname, ix.indisunique, ix.indisprimary
    ORDER BY i.relname;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        table_catalog::text AS catalog_name,
        table_schema::text AS schema_name,
        table_name::text AS object_name,
        CASE table_type
            WHEN 'BASE TABLE' THEN 'table'
            WHEN 'VIEW' THEN 'view'
            ELSE table_type::text
        END AS object_type,
        table_type::text AS native_object_type
    FROM information_schema.tables
    WHERE table_schema = :schema_name
    ORDER BY table_schema, table_name;
    ```

??? code "`privileges.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        table_schema::text AS schema_name,
        table_name::text AS object_name,
        grantee::text AS grantee,
        privilege_type::text AS privilege_type,
        is_grantable::text AS is_grantable,
        with_hierarchy::text AS with_hierarchy
    FROM information_schema.table_privileges
    WHERE table_schema = :schema_name
      AND (:object_name::text IS NULL OR table_name = :object_name)
    ORDER BY table_schema, table_name, grantee, privilege_type;
    ```

??? code "`schemas.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: cockroachdb
    SELECT
        catalog_name::text AS catalog_name,
        schema_name::text AS schema_name,
        schema_owner::text AS owner_name
    FROM information_schema.schemata
    WHERE schema_name NOT LIKE 'pg\_%'
      AND schema_name <> 'information_schema'
    ORDER BY schema_name;
    ```

??? code "`sequences.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        sequence_schema::text AS schema_name,
        sequence_name::text AS sequence_name,
        data_type::text AS data_type,
        start_value::text AS start_value,
        minimum_value::text AS minimum_value,
        maximum_value::text AS maximum_value,
        increment::text AS increment,
        cycle_option::text AS cycle_option
    FROM information_schema.sequences
    WHERE sequence_schema = :schema_name
    ORDER BY sequence_schema, sequence_name;
    ```

??? code "`tables.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        table_catalog::text AS table_catalog,
        table_schema::text AS table_schema,
        table_schema::text AS schema_name,
        table_name::text AS table_name,
        table_type::text AS table_type,
        NULL::int AS dependency_level,
        NULL::int AS level
    FROM information_schema.tables
    WHERE table_schema = :schema_name
      AND (:table_name::text IS NULL OR table_name = :table_name)
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: cockroachdb
    SELECT version();
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: cockroachdb
    SELECT
        table_schema::text AS schema_name,
        table_name::text AS view_name,
        view_definition::text AS definition,
        check_option::text AS check_option,
        is_updatable::text AS is_updatable
    FROM information_schema.views
    WHERE table_schema = :schema_name
    ORDER BY table_schema, table_name;
    ```
