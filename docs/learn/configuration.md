# Configuration

SQLSpec separates **adapter configuration** (connection credentials, pool bounds, dialect defaults, and extension settings) from **active sessions** (checked-out connections executing SQL). Every adapter inherits from one of four base configuration classes in `sqlspec.config`.

## Pooled vs Non-Pooled Configs

| Base Class | Mode | Connection Lifecycle | Typical Adapters |
| --- | --- | --- | --- |
| `SyncDatabaseConfig` | Sync | Managed thread-safe connection pool (`create_pool`, `close_pool`, `provide_session`) | `SqliteConfig`, `PsycopgSyncConfig`, `OracleSyncConfig`, `DuckDBConfig`, `Db2SyncConfig`, `PymssqlConfig` |
| `AsyncDatabaseConfig` | Async | Managed `asyncio` connection pool (`await create_pool`, `await close_pool`, `async with provide_session`) | `AsyncpgConfig`, `PsycopgAsyncConfig`, `PsqlpyConfig`, `AiosqliteConfig`, `OracleAsyncConfig`, `AsyncmyConfig` |
| `NoPoolSyncConfig` | Sync | Stateless client or per-session direct connection | `BigQueryConfig`, `ArrowOdbcConfig`, `AdbcConfig` |
| `NoPoolAsyncConfig` | Async | Async client managing its own multiplexed transport | Custom stateless async adapters |

You can open sessions either through the `SQLSpec` registry (`spec.provide_session(config)`) or directly on any config object (`config.provide_session()`).

## Multi-Database Registration and Bind Keys

A single `SQLSpec` instance can coordinate multiple databases—such as an OLTP primary database alongside an analytical or audit store—and share loaded `.sql` files across them:

=== "SQL (`sql/queries.sql`)"

    ```sql
    -- name: list_users
    SELECT id, name
    FROM users
    ORDER BY id;
    ```

=== "Python"

    ```python
    --8<-- "docs/examples/configuration/multi_database.py:example"
    ```

When integrating with the CLI or web frameworks, assign `bind_key="primary"` and `bind_key="analytics"` on each config so migrations (`sqlspec --bind-key analytics upgrade`) and framework dependency keys (`db_session`, `analytics_session`) address each database unambiguously. For multi-pool web and background worker composition roots (`app/config.py`), see [Service Layer](recipes/service-layer.md#production-composition-root).

## Customizing StatementConfig

Every adapter supplies a dialect-tuned `StatementConfig` automatically. Pass `statement_config=` on the adapter config when you want to override caching, parameter styles, or SQLGlot optimization defaults across all sessions created from that config:

```python
from sqlspec import StatementConfig
from sqlspec.adapters.sqlite import SqliteConfig

config = SqliteConfig(
    connection_config={"database": ":memory:"},
    statement_config=StatementConfig(
        dialect="sqlite",
        enable_caching=True,
        enable_validation=True,
        enable_sqlcommenter=False,
    ),
)
```

Key `StatementConfig` options:

- `enable_caching`: Caches parsed SQLGlot ASTs and compiled driver parameter plans in a thread-safe `StatementCacheHolder` shared across sessions from the same config.
- `enable_optimization`: Runs SQLGlot AST optimization on fluent `QueryBuilder` queries (`True` by default; pass `False` when verbatim identifier casing is required, such as on BigQuery).
- `enable_sqlcommenter`: Appends OpenTelemetry/correlation comments at wire dispatch time without invalidating compiled statement cache keys.

## Connection and Query Lifecycle Hooks

Attach callbacks for pool lifecycle, connection initialization, session boundaries, or query execution via `ObservabilityConfig(lifecycle=...)` or adapter `driver_features`. Every hook receives a single `LifecycleContext` (`dict[str, Any]`) mapping:

```python
from typing import Any

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.observability import ObservabilityConfig


def on_connect(context: dict[str, Any]) -> None:
    conn = context["connection"]
    conn.execute("PRAGMA busy_timeout = 5000")


spec = SQLSpec(
    observability_config=ObservabilityConfig(
        lifecycle={"on_connection_create": [on_connect]}
    )
)
config = spec.add_config(
    SqliteConfig(connection_config={"database": ":memory:"})
)
```

Every `LifecycleContext` dictionary includes `"config"` (`str`), plus `"bind_key"` (`str`, when configured) and `"correlation_id"` (`str`, when active), alongside event-specific keys:

| Hook Event(s) | Trigger Point | Event-Specific `LifecycleContext` Keys |
| --- | --- | --- |
| `on_pool_create`, `on_pool_destroying`, `on_pool_destroy` | Pool initialization, pre-shutdown, and post-close | `pool` |
| `on_connection_create`, `on_connection_destroy` | Raw driver connection creation and teardown | `connection` |
| `on_session_start`, `on_session_end` | Driver session context entry and exit | `session` |
| `on_query_start` | Immediately before wire statement dispatch | `sql`, `parameters`, `driver`, `operation`, `is_many`, `is_script` |
| `on_query_complete` | Immediately after wire statement execution succeeds | `sql`, `parameters`, `driver`, `operation`, `is_many`, `is_script`, `rows_affected` |
| `on_error` | When statement execution raises an exception | `exception`, `sql`, `parameters`, `driver`, `operation`, `is_many`, `is_script` |

## Startup Warmup and Shutdown Cleanup

In long-running web workers or CLI tools, verify connectivity and pre-initialize migration commands during startup, and always close the pool during shutdown:

```python
--8<-- "docs/examples/configuration/migration_warmup.py:example"
```

When you integrate SQLSpec through `SQLSpecPlugin` (Litestar), an ASGI lifespan context manager (FastAPI / Starlette), or Dishka `Scope.APP` providers, pool creation and `close_pool()` are handled for you automatically.

## Recap

- Prefer `config.provide_session()` or `spec.provide_session(config)` over constructing driver instances directly so per-config statement caches and lifecycle hooks remain attached.
- Assign `bind_key` whenever an application registers more than one database config.
- Keep pool sizing and driver-specific options inside each adapter's typed `connection_config` and `driver_features` dictionaries (documented per engine in [Databases](/databases/)).
