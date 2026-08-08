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
