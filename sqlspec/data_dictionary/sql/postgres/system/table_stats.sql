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
