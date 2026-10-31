# PostgreSQL Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("postgres")
```

Introspects `pg_catalog` (`pg_class`, `pg_attribute`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_type`, `pg_extension`) and `information_schema` across `asyncpg`, `psycopg`, and `psqlpy`. See the [PostgreSQL Guide](../../../databases/postgres/index.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `postgres` |
| **Default Schema** | `"public"` |
| **Version Pattern** | `PostgreSQL (\d+)\.(\d+)(?:\.(\d+))?` |
| **DDL Strategy** | AST synthesis + `pg_get_*def` catalog functions |
| **Catalog `.sql` Files** | `21` files (`26` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_arrays` | `True` | `array` | `ARRAY` |
| `supports_for_update` | `True` | `bigint` | `BIGINT` |
| `supports_on_conflict` | `True` | `blob` | `BYTEA` |
| `supports_prepared_statements` | `True` | `boolean` | `BOOLEAN` |
| `supports_schemas` | `True` | `float` | `DOUBLE PRECISION` |
| `supports_transactions` | `True` | `integer` | `INTEGER` |
| `supports_update_from` | `True` | `json` | `JSONB` |
| `supports_uuid` | `True` | `text` | `TEXT` |
| `supports_cte` | `>= 8.4.0` | `timestamp` | `TIMESTAMP WITH TIME ZONE` |
| `supports_json` | `>= 9.2.0` | `uuid` | `UUID` |
| `supports_jsonb` | `>= 9.4.0` | `varchar` | `VARCHAR({length})` |
| `supports_partitioning` | `>= 10.0.0` | - | - |
| `supports_returning` | `>= 8.2.0` | - | - |
| `supports_skip_locked` | `>= 9.5.0` | - | - |
| `supports_upsert` | `>= 9.5.0` | - | - |
| `supports_window_functions` | `>= 8.4.0` | - | - |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.asyncpg.data_dictionary.AsyncpgDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.psycopg.data_dictionary.PsycopgSyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.psycopg.data_dictionary.PsycopgAsyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.psqlpy.data_dictionary.PsqlpyDataDictionary
    options:
      heading_level: 3

## Catalog SQL Queries

The `postgres` dialect pack defines `26` named queries across `21` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/postgres/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `comments.sql` | `by_schema` | `:schema_name` |
| `constraints.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `ddl.sql` | `by_object` | `:object_name`, `:object_type`, `:schema_name` |
| `dependencies.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `extensions.sql` | `list` | *none* |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `indexes.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `materialized_views.sql` | `by_schema` | `:schema_name` |
| `objects.sql` | `by_schema` | `:schema_name` |
| `partitions.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `privileges.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `routines.sql` | `by_schema` | `:schema_name` |
| `schemas.sql` | `list` | *none* |
| `sequences.sql` | `by_schema` | `:schema_name` |
| `system.sql` | `pg_stat_statements`, `settings`, `table_stats` | `:limit`, `:schema_name`, `:table_name` |
| `tables.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `triggers.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `types.sql` | `by_schema` | `:schema_name` |
| `version.sql` | `current` | *none* |
| `views.sql` | `by_schema` | `:schema_name` |

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
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
    table_constraints AS (
        SELECT
            con.conrelid,
            unnest(con.conkey) AS attnum,
            bool_or(con.contype = 'p') AS is_primary,
            bool_or(con.contype = 'u') AS is_unique
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN target_schema ts ON ts.schema_name = n.nspname
        WHERE con.contype IN ('p', 'u')
          AND (:table_name::text IS NULL OR c.relname = :table_name)
        GROUP BY con.conrelid, unnest(con.conkey)
    )
    SELECT
        current_database()::text AS table_catalog,
        n.nspname::text AS table_schema,
        n.nspname::text AS schema_name,
        c.relname::text AS table_name,
        a.attname::text AS column_name,
        a.attnum::integer AS ordinal_position,
        pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
        t.typname::text AS type_name,
        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
        CASE WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS column_default,
        CASE
            WHEN a.attidentity = 'a' THEN 'ALWAYS'
            WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
            ELSE NULL
        END AS identity_generation,
        CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN a.attgenerated <> '' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN a.attgenerated <> '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS generation_expression,
        col_description(c.oid, a.attnum)::text AS comment,
        COALESCE(tc.is_primary, false) AS is_primary,
        COALESCE(tc.is_unique, false) AS is_unique,
        pg_catalog.pg_get_serial_sequence(
            pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname), a.attname
        )::text AS sequence_name
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN target_schema ts ON ts.schema_name = n.nspname
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
    LEFT JOIN table_constraints tc ON tc.conrelid = c.oid AND tc.attnum = a.attnum
    WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
      AND (:table_name::text IS NULL OR c.relname = :table_name)
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY n.nspname, c.relname, a.attnum;

    -- name: by_table
    -- dialect: postgres
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
    table_constraints AS (
        SELECT
            con.conrelid,
            unnest(con.conkey) AS attnum,
            bool_or(con.contype = 'p') AS is_primary,
            bool_or(con.contype = 'u') AS is_unique
        FROM pg_catalog.pg_constraint con
        JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN target_schema ts ON ts.schema_name = n.nspname
        WHERE con.contype IN ('p', 'u')
          AND c.relname = :table_name::text
        GROUP BY con.conrelid, unnest(con.conkey)
    )
    SELECT
        current_database()::text AS table_catalog,
        n.nspname::text AS table_schema,
        n.nspname::text AS schema_name,
        c.relname::text AS table_name,
        a.attname::text AS column_name,
        a.attnum::integer AS ordinal_position,
        pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
        t.typname::text AS type_name,
        CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
        CASE WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS column_default,
        CASE
            WHEN a.attidentity = 'a' THEN 'ALWAYS'
            WHEN a.attidentity = 'd' THEN 'BY DEFAULT'
            ELSE NULL
        END AS identity_generation,
        CASE WHEN a.attidentity IN ('a', 'd') THEN 1 ELSE 0 END AS is_identity,
        CASE WHEN a.attgenerated <> '' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN a.attgenerated <> '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text ELSE NULL END AS generation_expression,
        col_description(c.oid, a.attnum)::text AS comment,
        COALESCE(tc.is_primary, false) AS is_primary,
        COALESCE(tc.is_unique, false) AS is_unique,
        pg_catalog.pg_get_serial_sequence(
            pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname), a.attname
        )::text AS sequence_name
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    JOIN target_schema ts ON ts.schema_name = n.nspname
    JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
    LEFT JOIN pg_catalog.pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum
    LEFT JOIN table_constraints tc ON tc.conrelid = c.oid AND tc.attnum = a.attnum
    WHERE c.relname = :table_name::text
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum;
    ```

??? code "`comments.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS object_name,
        'table'::text AS object_type,
        d.description::text AS comment,
        c.oid::text AS object_oid
    FROM pg_catalog.pg_description d
    JOIN pg_catalog.pg_class c ON c.oid = d.objoid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = :schema_name
      AND d.objsubid = 0
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text || '.' || a.attname::text AS object_name,
        'column'::text AS object_type,
        d.description::text AS comment,
        c.oid::text AS object_oid
    FROM pg_catalog.pg_description d
    JOIN pg_catalog.pg_class c ON c.oid = d.objoid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
    WHERE n.nspname = :schema_name
      AND d.objsubid > 0
    ORDER BY schema_name, object_type, object_name;
    ```

??? code "`constraints.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        current_database()::text AS constraint_catalog,
        n.nspname::text AS constraint_schema,
        n.nspname::text AS schema_name,
        c.relname::text AS table_name,
        con.conname::text AS constraint_name,
        CASE con.contype
            WHEN 'c' THEN 'CHECK'
            WHEN 'f' THEN 'FOREIGN KEY'
            WHEN 'p' THEN 'PRIMARY KEY'
            WHEN 'u' THEN 'UNIQUE'
            WHEN 'x' THEN 'EXCLUDE'
            ELSE con.contype::text
        END AS constraint_type,
        pg_catalog.pg_get_constraintdef(con.oid, true)::text AS definition,
        ARRAY(
            SELECT att.attname::text
            FROM unnest(con.conkey) WITH ORDINALITY AS keys(attnum, ord)
            JOIN pg_catalog.pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = keys.attnum
            ORDER BY keys.ord
        )::text[] AS columns,
        ref_ns.nspname::text AS referenced_schema,
        ref.relname::text AS referenced_table,
        ARRAY(
            SELECT att.attname::text
            FROM unnest(con.confkey) WITH ORDINALITY AS keys(attnum, ord)
            JOIN pg_catalog.pg_attribute att ON att.attrelid = con.confrelid AND att.attnum = keys.attnum
            ORDER BY keys.ord
        )::text[] AS referenced_columns,
        con.condeferrable AS is_deferrable,
        con.condeferred AS initially_deferred,
        dep.refobjid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_constraint con
    JOIN pg_catalog.pg_namespace n ON n.oid = con.connamespace
    LEFT JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
    LEFT JOIN pg_catalog.pg_class ref ON ref.oid = con.confrelid
    LEFT JOIN pg_catalog.pg_namespace ref_ns ON ref_ns.oid = ref.relnamespace
    LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = con.oid AND dep.deptype = 'e'
    WHERE n.nspname = :schema_name
      AND (:table_name::text IS NULL OR c.relname = :table_name)
    ORDER BY n.nspname, c.relname, con.conname;
    ```

??? code "`ddl.sql` (1 named queries)"

    ```sql
    -- name: by_object
    -- dialect: postgres
    WITH extension_deps AS (
        SELECT DISTINCT
            classid,
            objid
        FROM pg_catalog.pg_depend
        WHERE deptype = 'e'
    )
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS object_name,
        CASE c.relkind
            WHEN 'v' THEN pg_catalog.pg_get_viewdef(c.oid, true)::text
            WHEN 'm' THEN pg_catalog.pg_get_viewdef(c.oid, true)::text
            WHEN 'S' THEN 'CREATE SEQUENCE ' || pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname)
            ELSE NULL::text
        END AS ddl,
        CASE c.relkind
            WHEN 'v' THEN 'native'
            WHEN 'm' THEN 'native'
            WHEN 'S' THEN 'generated'
            ELSE 'unsupported'
        END AS fidelity,
        dep.objid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN extension_deps dep
        ON dep.classid = 'pg_catalog.pg_class'::regclass
       AND dep.objid = c.oid
    WHERE n.nspname = :schema_name
      AND c.relname = :object_name
      AND (:object_type::text IS NULL OR c.relkind = CASE :object_type
          WHEN 'view' THEN 'v'
          WHEN 'materialized_view' THEN 'm'
          WHEN 'sequence' THEN 'S'
          WHEN 'table' THEN 'r'
          ELSE c.relkind
      END)
    UNION ALL
    SELECT
        n.nspname::text AS schema_name,
        p.proname::text AS object_name,
        pg_catalog.pg_get_functiondef(p.oid)::text AS ddl,
        'native'::text AS fidelity,
        dep.objid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    LEFT JOIN extension_deps dep
        ON dep.classid = 'pg_catalog.pg_proc'::regclass
       AND dep.objid = p.oid
    WHERE n.nspname = :schema_name
      AND p.proname = :object_name
      AND (:object_type::text IS NULL OR :object_type IN ('routine', 'function', 'procedure'))
    ORDER BY schema_name, object_name;
    ```

??? code "`dependencies.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        src_ns.nspname::text AS schema_name,
        src.relname::text AS object_name,
        src.relkind::text AS object_type,
        target_ns.nspname::text AS referenced_schema,
        target.relname::text AS referenced_object,
        target.relkind::text AS referenced_object_type,
        dep.deptype::text AS dependency_type,
        ext.extname::text AS extension_name,
        dep.objid::text AS object_oid,
        dep.refobjid::text AS referenced_oid
    FROM pg_catalog.pg_depend dep
    JOIN pg_catalog.pg_class src ON src.oid = dep.objid
    JOIN pg_catalog.pg_namespace src_ns ON src_ns.oid = src.relnamespace
    LEFT JOIN pg_catalog.pg_class target ON target.oid = dep.refobjid
    LEFT JOIN pg_catalog.pg_namespace target_ns ON target_ns.oid = target.relnamespace
    LEFT JOIN pg_catalog.pg_extension ext ON ext.oid = dep.refobjid AND dep.deptype = 'e'
    WHERE src_ns.nspname = :schema_name
      AND (:object_name::text IS NULL OR src.relname = :object_name)
    UNION ALL
    SELECT
        src_ns.nspname::text AS schema_name,
        src.relname::text AS object_name,
        'table'::text AS object_type,
        target_ns.nspname::text AS referenced_schema,
        target.relname::text AS referenced_object,
        'table'::text AS referenced_object_type,
        'foreign_key'::text AS dependency_type,
        NULL::text AS extension_name,
        fk.oid::text AS object_oid,
        fk.confrelid::text AS referenced_oid
    FROM pg_catalog.pg_constraint fk
    JOIN pg_catalog.pg_class src ON src.oid = fk.conrelid
    JOIN pg_catalog.pg_namespace src_ns ON src_ns.oid = src.relnamespace
    JOIN pg_catalog.pg_class target ON target.oid = fk.confrelid
    JOIN pg_catalog.pg_namespace target_ns ON target_ns.oid = target.relnamespace
    WHERE fk.contype = 'f'
      AND src_ns.nspname = :schema_name
      AND (:object_name::text IS NULL OR src.relname = :object_name)
    ORDER BY schema_name, object_name, dependency_type, referenced_object;
    ```

??? code "`extensions.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: postgres
    SELECT
        ext.extname::text AS extension_name,
        ext.extversion::text AS extension_version,
        n.nspname::text AS schema_name,
        ext.oid::text AS extension_oid,
        pg_catalog.obj_description(ext.oid, 'pg_extension')::text AS comment
    FROM pg_catalog.pg_extension ext
    JOIN pg_catalog.pg_namespace n ON n.oid = ext.extnamespace
    ORDER BY ext.extname;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: postgres
    SELECT
        c.relname::text AS table_name,
        att.attname::text AS column_name,
        ref.relname::text AS referenced_table_name,
        ref_att.attname::text AS referenced_column_name,
        con.conname::text AS constraint_name,
        n.nspname::text AS table_schema,
        ref_ns.nspname::text AS referenced_table_schema,
        CASE con.confdeltype
            WHEN 'a' THEN 'NO ACTION'
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT'
        END AS on_delete,
        CASE con.confupdtype
            WHEN 'a' THEN 'NO ACTION'
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT'
        END AS on_update
    FROM pg_catalog.pg_constraint AS con
    JOIN pg_catalog.pg_class AS c ON c.oid = con.conrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_class AS ref ON ref.oid = con.confrelid
    JOIN pg_catalog.pg_namespace AS ref_ns ON ref_ns.oid = ref.relnamespace
    CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS keys(attnum, ref_attnum, ord)
    JOIN pg_catalog.pg_attribute AS att ON att.attrelid = c.oid AND att.attnum = keys.attnum
    JOIN pg_catalog.pg_attribute AS ref_att ON ref_att.attrelid = ref.oid AND ref_att.attnum = keys.ref_attnum
    WHERE con.contype = 'f'
      AND (:schema_name::text IS NULL OR n.nspname = :schema_name)
      AND (:table_name::text IS NULL OR c.relname = :table_name)
    ORDER BY c.relname, con.conname, keys.ord;

    -- name: by_schema
    -- dialect: postgres
    SELECT
        c.relname::text AS table_name,
        att.attname::text AS column_name,
        ref.relname::text AS referenced_table_name,
        ref_att.attname::text AS referenced_column_name,
        con.conname::text AS constraint_name,
        n.nspname::text AS table_schema,
        ref_ns.nspname::text AS referenced_table_schema,
        CASE con.confdeltype
            WHEN 'a' THEN 'NO ACTION'
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT'
        END AS on_delete,
        CASE con.confupdtype
            WHEN 'a' THEN 'NO ACTION'
            WHEN 'r' THEN 'RESTRICT'
            WHEN 'c' THEN 'CASCADE'
            WHEN 'n' THEN 'SET NULL'
            WHEN 'd' THEN 'SET DEFAULT'
        END AS on_update
    FROM pg_catalog.pg_constraint AS con
    JOIN pg_catalog.pg_class AS c ON c.oid = con.conrelid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_class AS ref ON ref.oid = con.confrelid
    JOIN pg_catalog.pg_namespace AS ref_ns ON ref_ns.oid = ref.relnamespace
    CROSS JOIN LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS keys(attnum, ref_attnum, ord)
    JOIN pg_catalog.pg_attribute AS att ON att.attrelid = c.oid AND att.attnum = keys.attnum
    JOIN pg_catalog.pg_attribute AS ref_att ON ref_att.attrelid = ref.oid AND ref_att.attnum = keys.ref_attnum
    WHERE con.contype = 'f'
      AND (:schema_name::text IS NULL OR n.nspname = :schema_name)
    ORDER BY c.relname, con.conname, keys.ord;
    ```

??? code "`indexes.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        tn.nspname::text AS schema_name,
        tbl.relname::text AS table_name,
        idx.relname::text AS index_name,
        am.amname::text AS access_method,
        ix.indisunique AS is_unique,
        ix.indisprimary AS is_primary,
        ix.indisexclusion AS is_exclusion,
        ix.indisvalid AS is_valid,
        pg_catalog.pg_get_expr(ix.indpred, ix.indrelid)::text IS NOT NULL AS is_partial,
        pg_catalog.pg_get_expr(ix.indpred, ix.indrelid)::text AS predicate,
        pg_catalog.pg_get_indexdef(idx.oid)::text AS definition,
        ARRAY(
            SELECT COALESCE(att.attname::text, pg_catalog.pg_get_indexdef(idx.oid, keys.ord::integer, true)::text)
            FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord)
            LEFT JOIN pg_catalog.pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = keys.attnum
            WHERE keys.ord <= ix.indnkeyatts
            ORDER BY keys.ord
        )::text[] AS columns,
        ARRAY(
            SELECT pg_catalog.pg_get_indexdef(idx.oid, keys.ord::integer, true)::text
            FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ord)
            WHERE keys.ord > ix.indnkeyatts
            ORDER BY keys.ord
        )::text[] AS included_columns,
        ARRAY(
            SELECT opc.opcname::text
            FROM unnest(ix.indclass) WITH ORDINALITY AS classes(opclass_oid, ord)
            JOIN pg_catalog.pg_opclass opc ON opc.oid = classes.opclass_oid
            ORDER BY classes.ord
        )::text[] AS opclasses,
        dep.refobjid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_index ix
    JOIN pg_catalog.pg_class tbl ON tbl.oid = ix.indrelid
    JOIN pg_catalog.pg_namespace tn ON tn.oid = tbl.relnamespace
    JOIN pg_catalog.pg_class idx ON idx.oid = ix.indexrelid
    JOIN pg_catalog.pg_am am ON am.oid = idx.relam
    LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = idx.oid AND dep.deptype = 'e'
    WHERE tn.nspname = :schema_name
      AND (:table_name::text IS NULL OR tbl.relname = :table_name)
    ORDER BY tn.nspname, tbl.relname, idx.relname;

    -- name: by_table
    -- dialect: postgres
    SELECT
        i.relname as index_name,
        t.relname as table_name,
        ix.indisunique as is_unique,
        ix.indisprimary as is_primary,
        array_agg(a.attname::text ORDER BY array_position(ix.indkey, a.attnum)) as columns
    FROM
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a,
        pg_namespace n
    WHERE
        t.oid = ix.indrelid
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relnamespace = n.oid
        AND n.nspname = :schema_name
        AND t.relname = :table_name
    GROUP BY
        t.relname,
        i.relname,
        ix.indisunique,
        ix.indisprimary;
    ```

??? code "`materialized_views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS view_name,
        c.oid::text AS object_oid,
        pg_catalog.pg_get_viewdef(c.oid, true)::text AS definition,
        r.rolname::text AS owner_name,
        pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
        dep.refobjid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
    LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'
    WHERE n.nspname = :schema_name
      AND c.relkind = 'm'
    ORDER BY n.nspname, c.relname;
    ```

??? code "`objects.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        current_database()::text AS catalog_name,
        n.nspname::text AS schema_name,
        c.relname::text AS object_name,
        CASE c.relkind
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned_table'
            WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized_view'
            WHEN 'S' THEN 'sequence'
            WHEN 'f' THEN 'foreign_table'
            ELSE c.relkind::text
        END AS object_type,
        c.oid::text AS object_oid,
        r.rolname::text AS owner_name,
        c.relispartition AS is_partition,
        pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
    WHERE n.nspname = :schema_name
      AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')
    ORDER BY n.nspname, object_type, c.relname;
    ```

??? code "`partitions.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        parent_ns.nspname::text AS schema_name,
        parent.relname::text AS parent_table,
        child_ns.nspname::text AS partition_schema,
        child.relname::text AS partition_name,
        pg_catalog.pg_get_expr(child.relpartbound, child.oid, true)::text AS partition_bound,
        pt.partstrat::text AS partition_strategy,
        inherit.inhseqno AS inherit_sequence
    FROM pg_catalog.pg_inherits inherit
    JOIN pg_catalog.pg_class parent ON parent.oid = inherit.inhparent
    JOIN pg_catalog.pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
    JOIN pg_catalog.pg_class child ON child.oid = inherit.inhrelid
    JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace
    LEFT JOIN pg_catalog.pg_partitioned_table pt ON pt.partrelid = parent.oid
    WHERE parent_ns.nspname = :schema_name
      AND (:table_name::text IS NULL OR parent.relname = :table_name OR child.relname = :table_name)
    ORDER BY parent_ns.nspname, parent.relname, inherit.inhseqno, child.relname;
    ```

??? code "`privileges.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS object_name,
        CASE c.relkind
            WHEN 'r' THEN 'table'
            WHEN 'p' THEN 'partitioned_table'
            WHEN 'v' THEN 'view'
            WHEN 'm' THEN 'materialized_view'
            WHEN 'S' THEN 'sequence'
            ELSE c.relkind::text
        END AS object_type,
        grant_row.grantee::text AS grantee,
        grant_row.privilege_type::text AS privilege_type,
        grant_row.is_grantable AS is_grantable
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(c.relacl, pg_catalog.acldefault('r', c.relowner))) AS acl
    LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
    CROSS JOIN LATERAL (
        SELECT
            COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
            acl.privilege_type,
            acl.is_grantable
    ) AS grant_row
    WHERE n.nspname = :schema_name
      AND (:object_name::text IS NULL OR c.relname = :object_name)
    ORDER BY n.nspname, c.relname, grant_row.grantee, grant_row.privilege_type;
    ```

??? code "`routines.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        p.proname::text AS routine_name,
        p.oid::text AS routine_oid,
        CASE p.prokind
            WHEN 'f' THEN 'function'
            WHEN 'p' THEN 'procedure'
            WHEN 'a' THEN 'aggregate'
            WHEN 'w' THEN 'window'
            ELSE p.prokind::text
        END AS routine_type,
        pg_catalog.pg_get_function_arguments(p.oid)::text AS arguments,
        pg_catalog.pg_get_function_result(p.oid)::text AS result_type,
        lang.lanname::text AS language_name,
        pg_catalog.pg_get_functiondef(p.oid)::text AS definition,
        pg_catalog.obj_description(p.oid, 'pg_proc')::text AS comment,
        dep.refobjid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
    JOIN pg_catalog.pg_language lang ON lang.oid = p.prolang
    LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = p.oid AND dep.deptype = 'e'
    WHERE n.nspname = :schema_name
    ORDER BY n.nspname, p.proname, p.oid;
    ```

??? code "`schemas.sql` (1 named queries)"

    ```sql
    -- name: list
    -- dialect: postgres
    SELECT
        current_database()::text AS catalog_name,
        n.oid::text AS schema_oid,
        n.nspname::text AS schema_name,
        r.rolname::text AS owner_name,
        pg_catalog.obj_description(n.oid, 'pg_namespace')::text AS comment
    FROM pg_catalog.pg_namespace n
    LEFT JOIN pg_catalog.pg_roles r ON r.oid = n.nspowner
    WHERE n.nspname NOT LIKE 'pg\_%'
      AND n.nspname <> 'information_schema'
    ORDER BY n.nspname;
    ```

??? code "`sequences.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    WITH extension_deps AS (
        SELECT DISTINCT objid
        FROM pg_catalog.pg_depend
        WHERE classid = 'pg_catalog.pg_class'::regclass
          AND deptype = 'e'
    ),
    identity_deps AS (
        SELECT DISTINCT objid
        FROM pg_catalog.pg_depend
        WHERE classid = 'pg_catalog.pg_class'::regclass
          AND deptype = 'i'
    )
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS sequence_name,
        s.seqstart AS start_value,
        s.seqmin AS minimum_value,
        s.seqmax AS maximum_value,
        s.seqincrement AS increment,
        s.seqcycle AS cycles,
        pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
        ext_dep.objid IS NOT NULL AS is_extension_owned,
        id_dep.objid IS NOT NULL AS is_identity_owned
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
    LEFT JOIN extension_deps AS ext_dep ON ext_dep.objid = c.oid
    LEFT JOIN identity_deps AS id_dep ON id_dep.objid = c.oid
    WHERE n.nspname = :schema_name
    ORDER BY n.nspname, c.relname;
    ```

??? code "`system.sql` (3 named queries)"

    ```sql
    -- name: pg_stat_statements
    -- dialect: postgres
    SELECT
        userid::text AS user_oid,
        dbid::text AS database_oid,
        queryid::text AS query_id,
        query::text AS query_text,
        calls,
        total_exec_time,
        mean_exec_time,
        rows
    FROM pg_catalog.pg_stat_statements
    ORDER BY total_exec_time DESC
    LIMIT :limit;

    -- name: settings
    -- dialect: postgres
    SELECT
        name::text AS setting_name,
        setting::text AS setting_value,
        unit::text AS unit,
        category::text AS category,
        context::text AS context,
        vartype::text AS value_type,
        source::text AS source,
        boot_val::text AS boot_value,
        reset_val::text AS reset_value,
        pending_restart
    FROM pg_catalog.pg_settings
    ORDER BY name;

    -- name: table_stats
    -- dialect: postgres
    SELECT
        schemaname::text AS schema_name,
        relname::text AS table_name,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_live_tup,
        n_dead_tup,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count
    FROM pg_catalog.pg_stat_user_tables
    WHERE (:schema_name::text IS NULL OR schemaname = :schema_name)
      AND (:table_name::text IS NULL OR relname = :table_name)
    ORDER BY schemaname, relname;
    ```

??? code "`tables.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    WITH RECURSIVE dependency_tree AS (
        SELECT
            c.oid,
            n.nspname::text AS schema_name,
            c.relname::text AS table_name,
            0 AS dependency_level,
            ARRAY[c.oid] AS path
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema_name
          AND (:table_name::text IS NULL OR c.relname = :table_name)
          AND c.relkind IN ('r', 'p')
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.pg_constraint fk
              WHERE fk.conrelid = c.oid
                AND fk.contype = 'f'
                AND fk.confrelid <> c.oid
          )

        UNION ALL

        SELECT
            child.oid,
            child_ns.nspname::text AS schema_name,
            child.relname::text AS table_name,
            parent.dependency_level + 1,
            parent.path || child.oid
        FROM pg_catalog.pg_constraint fk
        JOIN pg_catalog.pg_class child ON child.oid = fk.conrelid
        JOIN pg_catalog.pg_namespace child_ns ON child_ns.oid = child.relnamespace
        JOIN dependency_tree parent ON parent.oid = fk.confrelid
        WHERE fk.contype = 'f'
          AND fk.confrelid <> fk.conrelid
          AND child_ns.nspname = :schema_name
          AND (:table_name::text IS NULL OR child.relname = :table_name)
          AND NOT child.oid = ANY(parent.path)
    ),
    dependency_levels AS (
        SELECT
            oid,
            MAX(dependency_level) AS dependency_level
        FROM dependency_tree
        GROUP BY oid
    )
    SELECT
        current_database()::text AS table_catalog,
        n.nspname::text AS table_schema,
        n.nspname::text AS schema_name,
        c.relname::text AS table_name,
        CASE c.relkind WHEN 'p' THEN 'PARTITIONED TABLE' ELSE 'BASE TABLE' END AS table_type,
        COALESCE(t.dependency_level, 0) AS dependency_level,
        COALESCE(t.dependency_level, 0) AS level,
        c.reltuples::bigint AS estimated_row_count,
        c.relispartition AS is_partition,
        r.rolname::text AS owner_name,
        pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
    LEFT JOIN dependency_levels t ON t.oid = c.oid
    WHERE n.nspname = :schema_name
      AND (:table_name::text IS NULL OR c.relname = :table_name)
      AND c.relkind IN ('r', 'p')
    ORDER BY dependency_level, c.relname;
    ```

??? code "`triggers.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS table_name,
        tg.tgname::text AS trigger_name,
        p.proname::text AS function_name,
        pg_catalog.pg_get_triggerdef(tg.oid, true)::text AS definition,
        NOT tg.tgenabled = 'D' AS is_enabled,
        pg_catalog.obj_description(tg.oid, 'pg_trigger')::text AS comment,
        dep.refobjid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_trigger tg
    JOIN pg_catalog.pg_class c ON c.oid = tg.tgrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_proc p ON p.oid = tg.tgfoid
    LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = tg.oid AND dep.deptype = 'e'
    WHERE n.nspname = :schema_name
      AND (:table_name::text IS NULL OR c.relname = :table_name)
      AND NOT tg.tgisinternal
    ORDER BY n.nspname, c.relname, tg.tgname;
    ```

??? code "`types.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        t.typname::text AS type_name,
        'ENUM'::text AS logical_type,
        array_agg(e.enumlabel::text ORDER BY e.enumsortorder) AS labels
    FROM pg_catalog.pg_type AS t
    JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
    JOIN pg_catalog.pg_enum AS e ON e.enumtypid = t.oid
    WHERE t.typtype = 'e'
      AND n.nspname = :schema_name
      AND NOT EXISTS (
          SELECT 1
          FROM pg_catalog.pg_depend AS dep
          WHERE dep.classid = 'pg_catalog.pg_type'::regclass
            AND dep.objid = t.oid
            AND dep.deptype = 'e'
      )
    GROUP BY n.nspname, t.typname
    ORDER BY t.typname;
    ```

??? code "`version.sql` (1 named queries)"

    ```sql
    -- name: current
    -- dialect: postgres
    SELECT version();
    ```

??? code "`views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: postgres
    SELECT
        n.nspname::text AS schema_name,
        c.relname::text AS view_name,
        c.oid::text AS object_oid,
        pg_catalog.pg_get_viewdef(c.oid, true)::text AS definition,
        r.rolname::text AS owner_name,
        pg_catalog.obj_description(c.oid, 'pg_class')::text AS comment,
        dep.refobjid IS NOT NULL AS is_extension_owned
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_roles r ON r.oid = c.relowner
    LEFT JOIN pg_catalog.pg_depend dep ON dep.objid = c.oid AND dep.deptype = 'e'
    WHERE n.nspname = :schema_name
      AND c.relkind = 'v'
    ORDER BY n.nspname, c.relname;
    ```
