# Oracle Database Data Dictionary & SQL Catalog

```python
from sqlspec.data_dictionary import get_data_dictionary_loader, get_dialect_config

config = get_dialect_config("oracle")
```

Introspects `ALL_TABLES`, `ALL_TAB_COLS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`, `ALL_CONSTRAINTS`, `ALL_CONS_COLUMNS`, `ALL_VIEWS`, `ALL_OBJECTS`, `ALL_TRIGGERS`, `ALL_SEQUENCES`, and `DBMS_METADATA`. See the [Oracle Database Guide](../../../databases/oracledb.md) and [Data Dictionary Guide](../../../learn/data-dictionary.md) for usage examples.

## Dialect Configuration

| Setting | Value |
| --- | --- |
| **Dialect Name** | `oracle` |
| **Default Schema** | `None` |
| **Version Pattern** | `(\d+)` |
| **DDL Strategy** | Native `DBMS_METADATA.GET_DDL` + AST fallback |
| **Catalog `.sql` Files** | `22` files (`36` named queries) |

### Capabilities & Type Mappings

| Capability | Status / Minimum Version | Logical Type | Target Physical Type |
| --- | --- | --- | --- |
| `supports_for_update` | `True` | `bigint` | `NUMBER(19)` |
| `supports_in_memory` | `True` | `blob` | `BLOB` |
| `supports_on_conflict` | `False` | `boolean` | `NUMBER(1)` |
| `supports_prepared_statements` | `True` | `float` | `BINARY_DOUBLE` |
| `supports_schemas` | `True` | `integer` | `NUMBER(10)` |
| `supports_skip_locked` | `True` | `json` | `JSON` |
| `supports_transactions` | `True` | `text` | `CLOB` |
| `supports_update_from` | `False` | `timestamp` | `TIMESTAMP` |
| - | - | `uuid` | `RAW(16)` |
| - | - | `varchar` | `VARCHAR2({length})` |

## Adapter Data Dictionary Classes

::: sqlspec.adapters.oracledb.data_dictionary.OracledbSyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.oracledb.data_dictionary.OracledbAsyncDataDictionary
    options:
      heading_level: 3

::: sqlspec.adapters.oracledb.data_dictionary.OracleVersionInfo
    options:
      heading_level: 3

::: sqlspec.adapters.oracledb.data_dictionary.JSONStorageType
    options:
      heading_level: 3

## Catalog SQL Queries

The `oracle` dialect pack defines `36` named queries across `22` domain `.sql` files loaded via `DataDictionaryLoader` (`sqlspec/data_dictionary/dialects/oracle/`):

| Domain Pack | Named Queries | Bind Parameters |
| --- | --- | --- |
| `arguments.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `columns.sql` | `by_schema`, `by_table` | `:schema_name`, `:table_name` |
| `comments.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `constraints.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `ddl.sql` | `by_object` | `:object_name`, `:object_type`, `:owner` |
| `dependencies.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `foreign_keys.sql` | `by_table`, `by_schema` | `:schema_name`, `:table_name` |
| `grants.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `indexes.sql` | `by_schema`, `by_table`, `expressions` | `:schema_name`, `:table_name` |
| `lob_storage.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `materialized_views.sql` | `by_schema` | `:mview_name`, `:schema_name` |
| `objects.sql` | `by_schema`, `cloud_ai_available` | `:object_name`, `:schema_name` |
| `partitions.sql` | `by_schema` | `:schema_name`, `:table_name` |
| `routines.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `schemas.sql` | `by_schema` | `:schema_name` |
| `sequences.sql` | `by_schema` | `:schema_name`, `:sequence_name` |
| `source.sql` | `by_schema` | `:object_name`, `:schema_name` |
| `system.sql` | `awr`, `no_diagnostics`, `statspack`, `vector_memory_pool` | `:acknowledge_license`, `:include_diagnostics` |
| `tables.sql` | `by_schema`, `names_by_schema` | `:schema_name`, `:table_name` |
| `triggers.sql` | `by_schema` | `:schema_name`, `:trigger_name` |
| `version.sql` | `current`, `autonomous_service`, `compatible`, `apex_release` | *none* |
| `views.sql` | `by_schema`, `json_duality_views`, `json_duality_views_by_schema` | `:schema_name`, `:view_name` |

??? code "`arguments.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        object_name,
        package_name,
        procedure_name,
        argument_name,
        position,
        sequence,
        data_level,
        data_type,
        defaulted,
        default_value,
        in_out,
        data_length,
        data_precision,
        data_scale,
        type_owner,
        type_name,
        type_subname,
        pls_type,
        char_length,
        char_used,
        origin_con_id
    FROM all_arguments
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR object_name = :object_name)
    ORDER BY owner, object_name, package_name, subprogram_id, sequence;
    ```

??? code "`columns.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    WITH pk_columns AS (
        SELECT DISTINCT
            acc.owner,
            acc.table_name,
            acc.column_name
        FROM all_constraints ac
        JOIN all_cons_columns acc
          ON acc.owner = ac.owner
         AND acc.constraint_name = ac.constraint_name
         AND acc.table_name = ac.table_name
        WHERE ac.owner = COALESCE(:schema_name, USER)
          AND (:table_name IS NULL OR ac.table_name = :table_name)
          AND ac.constraint_type = 'P'
    )
    SELECT
        c.owner AS schema_name,
        c.table_name,
        c.column_name,
        c.data_type,
        c.data_type_owner,
        c.data_length AS max_length,
        c.data_precision AS numeric_precision,
        c.data_scale AS numeric_scale,
        c.nullable AS is_nullable,
        c.column_id AS ordinal_position,
        c.data_default AS column_default,
        CASE WHEN c.hidden_column = 'YES' THEN 1 ELSE 0 END AS is_hidden,
        CASE WHEN c.virtual_column = 'YES' THEN 1 ELSE 0 END AS is_generated,
        c.segment_column_id,
        c.internal_column_id,
        CASE WHEN c.identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity,
        CASE
            WHEN ic.generation_type = 'BY DEFAULT' AND c.default_on_null = 'YES' THEN 'BY DEFAULT ON NULL'
            ELSE ic.generation_type
        END AS identity_generation,
        ic.sequence_name AS sequence_name,
        CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary,
        c.collation,
        cc.comments AS column_comment
    FROM all_tab_cols c
    LEFT JOIN pk_columns pk
      ON pk.owner = c.owner
     AND pk.table_name = c.table_name
     AND pk.column_name = c.column_name
    LEFT JOIN all_tab_identity_cols ic
      ON ic.owner = c.owner
     AND ic.table_name = c.table_name
     AND ic.column_name = c.column_name
    LEFT JOIN all_col_comments cc
      ON cc.owner = c.owner
     AND cc.table_name = c.table_name
     AND cc.column_name = c.column_name
    WHERE c.owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR c.table_name = :table_name)
    ORDER BY c.owner, c.table_name, c.internal_column_id;

    -- name: by_table
    -- dialect: oracle
    WITH pk_columns AS (
        SELECT DISTINCT
            acc.owner,
            acc.table_name,
            acc.column_name
        FROM all_constraints ac
        JOIN all_cons_columns acc
          ON acc.owner = ac.owner
         AND acc.constraint_name = ac.constraint_name
         AND acc.table_name = ac.table_name
        WHERE ac.owner = COALESCE(:schema_name, USER)
          AND ac.table_name = :table_name
          AND ac.constraint_type = 'P'
    )
    SELECT
        c.owner AS schema_name,
        c.table_name AS table_name,
        c.column_name AS column_name,
        c.data_type AS data_type,
        c.nullable AS is_nullable,
        c.data_default AS column_default,
        c.column_id AS ordinal_position,
        CASE WHEN c.hidden_column = 'YES' THEN 1 ELSE 0 END AS is_hidden,
        CASE WHEN c.virtual_column = 'YES' THEN 1 ELSE 0 END AS is_generated,
        CASE WHEN c.identity_column = 'YES' THEN 1 ELSE 0 END AS is_identity,
        CASE
            WHEN ic.generation_type = 'BY DEFAULT' AND c.default_on_null = 'YES' THEN 'BY DEFAULT ON NULL'
            ELSE ic.generation_type
        END AS identity_generation,
        ic.sequence_name AS sequence_name,
        CASE WHEN pk.column_name IS NOT NULL THEN 1 ELSE 0 END AS is_primary
    FROM all_tab_cols c
    LEFT JOIN pk_columns pk
      ON pk.owner = c.owner
     AND pk.table_name = c.table_name
     AND pk.column_name = c.column_name
    LEFT JOIN all_tab_identity_cols ic
      ON ic.owner = c.owner
     AND ic.table_name = c.table_name
     AND ic.column_name = c.column_name
    WHERE c.owner = COALESCE(:schema_name, USER)
      AND c.table_name = :table_name
    ORDER BY c.column_id;
    ```

??? code "`comments.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        table_name,
        NULL AS column_name,
        table_type AS object_type,
        comments
    FROM all_tab_comments
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR table_name = :object_name)
    UNION ALL
    SELECT
        owner AS schema_name,
        table_name,
        column_name,
        'COLUMN' AS object_type,
        comments
    FROM all_col_comments
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR table_name = :object_name)
    ORDER BY schema_name, table_name, column_name;
    ```

??? code "`constraints.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        c.owner AS schema_name,
        c.table_name,
        c.constraint_name,
        c.constraint_type,
        c.search_condition_vc AS search_condition,
        c.r_owner AS referenced_schema,
        r.table_name AS referenced_table,
        c.r_constraint_name AS referenced_constraint,
        c.delete_rule,
        c.status,
        c.deferrable,
        c.deferred,
        c.validated,
        c.generated,
        c.bad,
        c.rely,
        c.last_change,
        LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position) AS columns
    FROM all_constraints c
    LEFT JOIN all_cons_columns cc
      ON cc.owner = c.owner
      AND cc.constraint_name = c.constraint_name
    LEFT JOIN all_constraints r
      ON r.owner = c.r_owner
      AND r.constraint_name = c.r_constraint_name
    WHERE c.owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR c.table_name = :table_name)
    GROUP BY
        c.owner,
        c.table_name,
        c.constraint_name,
        c.constraint_type,
        c.search_condition_vc,
        c.r_owner,
        r.table_name,
        c.r_constraint_name,
        c.delete_rule,
        c.status,
        c.deferrable,
        c.deferred,
        c.validated,
        c.generated,
        c.bad,
        c.rely,
        c.last_change
    ORDER BY c.owner, c.table_name, c.constraint_name;
    ```

??? code "`ddl.sql` (1 named queries)"

    ```sql
    -- name: by_object
    -- dialect: oracle
    SELECT DBMS_METADATA.GET_DDL(:object_type, :object_name, :owner) AS ddl
    FROM dual;
    ```

??? code "`dependencies.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        name AS object_name,
        type AS object_type,
        referenced_owner,
        referenced_name,
        referenced_type,
        referenced_link_name,
        dependency_type,
        origin_con_id
    FROM all_dependencies
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR name = :object_name)
    ORDER BY owner, name, referenced_owner, referenced_name, referenced_type;
    ```

??? code "`foreign_keys.sql` (2 named queries)"

    ```sql
    -- name: by_table
    -- dialect: oracle
    SELECT
        c.table_name,
        cc.column_name,
        r.table_name AS referenced_table_name,
        rcc.column_name AS referenced_column_name,
        c.constraint_name,
        c.owner AS table_schema,
        r.owner AS referenced_table_schema
    FROM all_constraints c
    JOIN all_cons_columns cc
      ON c.owner = cc.owner
      AND c.constraint_name = cc.constraint_name
    JOIN all_constraints r
      ON c.r_owner = r.owner
      AND c.r_constraint_name = r.constraint_name
    JOIN all_cons_columns rcc
      ON r.owner = rcc.owner
      AND r.constraint_name = rcc.constraint_name
      AND cc.position = rcc.position
    WHERE c.constraint_type = 'R'
      AND c.owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR c.table_name = :table_name);

    -- name: by_schema
    -- dialect: oracle
    SELECT
        c.table_name,
        cc.column_name,
        r.table_name AS referenced_table_name,
        rcc.column_name AS referenced_column_name,
        c.constraint_name,
        c.owner AS table_schema,
        r.owner AS referenced_table_schema
    FROM all_constraints c
    JOIN all_cons_columns cc
      ON c.owner = cc.owner
      AND c.constraint_name = cc.constraint_name
    JOIN all_constraints r
      ON c.r_owner = r.owner
      AND c.r_constraint_name = r.constraint_name
    JOIN all_cons_columns rcc
      ON r.owner = rcc.owner
      AND r.constraint_name = rcc.constraint_name
      AND cc.position = rcc.position
    WHERE c.constraint_type = 'R'
      AND c.owner = COALESCE(:schema_name, USER);
    ```

??? code "`grants.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        table_name AS object_name,
        NULL AS column_name,
        grantor,
        grantee,
        privilege,
        grantable,
        hierarchy,
        type,
        common,
        inherited
    FROM all_tab_privs
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR table_name = :object_name)
    UNION ALL
    SELECT
        owner AS schema_name,
        table_name AS object_name,
        column_name,
        grantor,
        grantee,
        privilege,
        grantable,
        NULL AS hierarchy,
        'COLUMN' AS type,
        common,
        inherited
    FROM all_col_privs
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR table_name = :object_name)
    ORDER BY schema_name, object_name, column_name, grantee, privilege;
    ```

??? code "`indexes.sql` (3 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        i.owner AS schema_name,
        i.table_name,
        i.index_name,
        i.index_type,
        i.uniqueness,
        i.compression,
        i.prefix_length,
        i.tablespace_name,
        i.status,
        i.partitioned,
        i.temporary,
        i.generated,
        i.visibility,
        i.blevel,
        i.leaf_blocks,
        i.distinct_keys,
        i.avg_leaf_blocks_per_key,
        i.avg_data_blocks_per_key,
        i.clustering_factor,
        LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns
    FROM all_indexes i
    LEFT JOIN all_ind_columns ic
      ON ic.index_owner = i.owner
      AND ic.index_name = i.index_name
    WHERE i.owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR i.table_name = :table_name)
    GROUP BY
        i.owner,
        i.table_name,
        i.index_name,
        i.index_type,
        i.uniqueness,
        i.compression,
        i.prefix_length,
        i.tablespace_name,
        i.status,
        i.partitioned,
        i.temporary,
        i.generated,
        i.visibility,
        i.blevel,
        i.leaf_blocks,
        i.distinct_keys,
        i.avg_leaf_blocks_per_key,
        i.avg_data_blocks_per_key,
        i.clustering_factor
    ORDER BY i.owner, i.table_name, i.index_name;

    -- name: by_table
    -- dialect: oracle
    SELECT
        i.index_name,
        i.table_name AS table_name,
        CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique,
        CASE
            WHEN MAX(CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END) = 1 THEN 1
            ELSE 0
        END AS is_primary,
        LISTAGG(ic.column_name, ',') WITHIN GROUP (ORDER BY ic.column_position) AS columns
    FROM all_indexes i
    JOIN all_ind_columns ic
      ON i.owner = ic.index_owner
      AND i.index_name = ic.index_name
    LEFT JOIN all_constraints c
      ON c.owner = i.owner
      AND c.index_name = i.index_name
      AND c.constraint_type = 'P'
    WHERE i.table_name = :table_name
      AND i.owner = COALESCE(:schema_name, USER)
    GROUP BY i.index_name, i.table_name, i.uniqueness;

    -- name: expressions
    -- dialect: oracle
    SELECT
        ie.index_owner AS schema_name,
        ie.table_name,
        ie.index_name,
        ie.column_position,
        ie.column_expression
    FROM all_ind_expressions ie
    WHERE ie.index_owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR ie.table_name = :table_name)
    ORDER BY ie.index_owner, ie.table_name, ie.index_name, ie.column_position;
    ```

??? code "`lob_storage.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        table_name,
        column_name,
        segment_name,
        tablespace_name,
        index_name,
        chunk,
        pctversion,
        retention,
        freepools,
        cache,
        logging,
        encrypt,
        compression,
        deduplication,
        in_row,
        format,
        partitioned,
        securefile,
        retention_type,
        retention_value
    FROM all_lobs
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY owner, table_name, column_name;
    ```

??? code "`materialized_views.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        mview_name,
        container_name,
        query,
        updatable,
        update_log,
        master_rollback_seg,
        master_link,
        rewrite_enabled,
        rewrite_capability,
        refresh_mode,
        refresh_method,
        build_mode,
        fast_refreshable,
        last_refresh_type,
        last_refresh_date,
        staleness,
        compile_state,
        use_no_index,
        stale_since
    FROM all_mviews
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:mview_name IS NULL OR mview_name = :mview_name)
    ORDER BY owner, mview_name;
    ```

??? code "`objects.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        object_name,
        object_type,
        status,
        created,
        last_ddl_time,
        temporary,
        generated,
        secondary,
        editionable,
        oracle_maintained
    FROM all_objects
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR object_name = :object_name)
    ORDER BY owner, object_type, object_name;

    -- name: cloud_ai_available
    -- dialect: oracle
    SELECT
        COUNT(*) AS "cnt"
    FROM all_objects
    WHERE object_name = 'DBMS_CLOUD_AI'
      AND object_type = 'PACKAGE';
    ```

??? code "`partitions.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        table_owner AS schema_name,
        table_name,
        partition_name,
        composite,
        subpartition_count,
        high_value,
        high_value_length,
        partition_position,
        tablespace_name,
        pct_free,
        pct_used,
        ini_trans,
        max_trans,
        initial_extent,
        next_extent,
        min_extent,
        max_extent,
        max_size,
        pct_increase,
        freelists,
        freelist_groups,
        logging,
        compression,
        compress_for,
        num_rows,
        blocks,
        empty_blocks,
        avg_space,
        chain_cnt,
        avg_row_len,
        sample_size,
        last_analyzed,
        interval,
        segment_created,
        indexing,
        read_only,
        inmemory
    FROM all_tab_partitions
    WHERE table_owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR table_name = :table_name)
    ORDER BY table_owner, table_name, partition_position;
    ```

??? code "`routines.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        object_name,
        procedure_name,
        object_id,
        subprogram_id,
        object_type,
        aggregate,
        pipelined,
        impltypeowner,
        impltypename,
        parallel,
        interface,
        deterministic,
        authid
    FROM all_procedures
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR object_name = :object_name)
    ORDER BY owner, object_name, subprogram_id, procedure_name;
    ```

??? code "`schemas.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        username AS schema_name,
        user_id,
        created,
        common,
        oracle_maintained,
        inherited,
        default_collation
    FROM all_users
    WHERE username = COALESCE(:schema_name, username)
    ORDER BY username;
    ```

??? code "`sequences.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        sequence_owner AS schema_name,
        sequence_name,
        min_value,
        max_value,
        increment_by,
        cycle_flag,
        order_flag,
        cache_size,
        last_number,
        scale_flag,
        extend_flag,
        sharded_flag,
        session_flag,
        keep_value
    FROM all_sequences
    WHERE sequence_owner = COALESCE(:schema_name, USER)
      AND (:sequence_name IS NULL OR sequence_name = :sequence_name)
    ORDER BY sequence_owner, sequence_name;
    ```

??? code "`source.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        name,
        type,
        line,
        text,
        origin_con_id
    FROM all_source
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:object_name IS NULL OR name = :object_name)
    ORDER BY owner, name, type, line;
    ```

??? code "`system.sql` (4 named queries)"

    ```sql
    -- name: awr
    -- dialect: oracle
    SELECT
        :acknowledge_license AS license_acknowledged,
        :include_diagnostics AS diagnostics_enabled,
        'DBA_HIST_SNAPSHOT' AS required_view,
        'AWR/ASH/ADDM and DBA_HIST access requires explicit opt-in, privileges, and license acknowledgement.' AS warning
    FROM dual
    WHERE :include_diagnostics = 1
      AND :acknowledge_license = 1;

    -- name: no_diagnostics
    -- dialect: oracle
    SELECT
        0 AS diagnostics_enabled,
        'disabled' AS diagnostics_mode,
        'Oracle diagnostics, AWR/ASH/ADDM, Statspack, and DBA_HIST queries are disabled by default.' AS warning
    FROM dual;

    -- name: statspack
    -- dialect: oracle
    SELECT
        :acknowledge_license AS license_acknowledged,
        :include_diagnostics AS diagnostics_enabled,
        'STATS$SNAPSHOT' AS required_view,
        'Statspack access requires explicit opt-in and privileges.' AS warning
    FROM dual
    WHERE :include_diagnostics = 1
      AND :acknowledge_license = 1;

    -- name: vector_memory_pool
    -- dialect: oracle
    SELECT
        pool AS "pool",
        alloc_bytes AS "alloc_bytes",
        used_bytes AS "used_bytes"
    FROM v$vector_memory_pool;
    ```

??? code "`tables.sql` (2 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        t.owner AS schema_name,
        t.table_name,
        t.tablespace_name,
        t.cluster_name,
        t.iot_name,
        t.status,
        t.pct_free,
        t.pct_used,
        t.ini_trans,
        t.max_trans,
        t.logging,
        t.num_rows,
        t.blocks,
        t.empty_blocks,
        t.avg_space,
        t.chain_cnt,
        t.avg_row_len,
        t.degree,
        t.instances,
        t.cache,
        t.table_lock,
        t.partitioned,
        t.temporary,
        t.secondary,
        t.nested,
        t.dropped,
        t.read_only,
        t.segment_created,
        t.result_cache,
        c.comments AS table_comment
    FROM all_tables t
    LEFT JOIN all_tab_comments c
      ON c.owner = t.owner
      AND c.table_name = t.table_name
    WHERE t.owner = COALESCE(:schema_name, USER)
      AND (:table_name IS NULL OR t.table_name = :table_name)
    ORDER BY t.owner, t.table_name;

    -- name: names_by_schema
    -- dialect: oracle
    SELECT
        table_name
    FROM all_tables
    WHERE owner = COALESCE(:schema_name, USER)
    ORDER BY table_name;
    ```

??? code "`triggers.sql` (1 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        trigger_name,
        trigger_type,
        triggering_event,
        table_owner,
        base_object_type,
        table_name,
        column_name,
        referencing_names,
        when_clause,
        status,
        description,
        action_type,
        trigger_body,
        crossedition,
        before_statement,
        before_row,
        after_row,
        after_statement,
        instead_of_row,
        fire_once,
        apply_server_only
    FROM all_triggers
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:trigger_name IS NULL OR trigger_name = :trigger_name)
    ORDER BY owner, trigger_name;
    ```

??? code "`version.sql` (4 named queries)"

    ```sql
    -- name: current
    -- dialect: oracle
    SELECT
        product AS "product",
        version AS "version",
        status AS "status"
    FROM product_component_version
    WHERE product LIKE 'Oracle%'
    ORDER BY TO_NUMBER(REGEXP_SUBSTR(version, '^[0-9]+')) DESC, version DESC
    FETCH FIRST 1 ROWS ONLY;

    -- name: autonomous_service
    -- dialect: oracle
    SELECT sys_context('USERENV', 'CLOUD_SERVICE') AS "service" FROM dual;

    -- name: compatible
    -- dialect: oracle
    SELECT value AS compatible
    FROM v$parameter
    WHERE name = 'compatible';

    -- name: apex_release
    -- dialect: oracle
    SELECT
        version_no AS "version_no",
        api_compatibility AS "api_compatibility"
    FROM apex_release;
    ```

??? code "`views.sql` (3 named queries)"

    ```sql
    -- name: by_schema
    -- dialect: oracle
    SELECT
        owner AS schema_name,
        view_name,
        text_length,
        text,
        type_text_length,
        type_text,
        oid_text_length,
        oid_text,
        view_type_owner,
        view_type,
        superview_name,
        editioning_view,
        read_only,
        container_data,
        bequeath,
        origin_con_id
    FROM all_views
    WHERE owner = COALESCE(:schema_name, USER)
      AND (:view_name IS NULL OR view_name = :view_name)
    ORDER BY owner, view_name;

    -- name: json_duality_views
    -- dialect: oracle
    SELECT
        view_name AS "view_name",
        root_table_name AS "root_table_name",
        root_table_owner AS "root_table_owner",
        allow_insert AS "allow_insert",
        allow_update AS "allow_update",
        allow_delete AS "allow_delete"
    FROM user_json_duality_views
    ORDER BY view_name;

    -- name: json_duality_views_by_schema
    -- dialect: oracle
    SELECT
        view_name AS "view_name",
        root_table_name AS "root_table_name",
        root_table_owner AS "root_table_owner",
        allow_insert AS "allow_insert",
        allow_update AS "allow_update",
        allow_delete AS "allow_delete"
    FROM all_json_duality_views
    WHERE (:schema_name IS NULL AND view_owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))
       OR (:schema_name IS NOT NULL AND view_owner = :schema_name)
    ORDER BY view_name;
    ```
