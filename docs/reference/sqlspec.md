# SQLSpec Registry

```python
from sqlspec import SQLSpec
```

Central registry for database configurations, connection pools, driver sessions, named SQL file loading, and global statement cache controls. See [Quickstart](../learn/quickstart.md) and [Configuration](../learn/configuration.md) for usage guides.

## Registry Class

::: sqlspec.base.SQLSpec
    options:
      heading_level: 3
      members:
        - __init__
        - add_config
        - get_connection
        - get_session
        - provide_connection
        - provide_session
        - get_pool
        - close_pool
        - close_all_pools
        - event_channel
        - telemetry_snapshot
        - load_sql_files
        - add_named_sql
        - get_sql
        - list_sql_queries
        - has_sql_query
        - clear_sql_cache
        - reload_sql_files
        - configure_cache
        - get_cache_config
        - update_cache_config
        - get_cache_stats
        - reset_stats_only
        - log_cache_stats
