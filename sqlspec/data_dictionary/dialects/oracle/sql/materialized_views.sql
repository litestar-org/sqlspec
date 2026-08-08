-- name: by_owner
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
