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

## Multi-Database Registration and `bind_key`

A single `SQLSpec` instance can coordinate multiple databases—such as an OLTP primary database alongside an analytical or audit store—and share loaded `.sql` files across them:

```python
--8<-- "docs/examples/configuration/multi_database.py:example"
```

When integrating with the CLI or web frameworks, assign `bind_key="primary"` and `bind_key="analytics"` on each config so migrations (`sqlspec --bind-key analytics upgrade`) and framework dependency keys (`db_session`, `analytics_session`) address each database unambiguously.

## Production Composition Root (`app/config.py`) and Settings

In production web applications and background worker services, isolate connection settings and registry assembly into a dedicated composition root module (`app/config.py`). This pattern keeps configuration evaluation lazy (using PEP 562 `__getattr__`), separates short-lived web requests from long-running background tasks, and registers CLI migrations directly in `pyproject.toml`.

```python
"""Application database configuration and composition root."""

from pathlib import Path
from typing import Any

from pydantic_settings import BaseSettings

from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig

BASE_DIR = Path(__file__).resolve().parent.parent


class DatabaseSettings(BaseSettings):
    """Database connection and pool settings."""

    url: str = "postgresql://user:pass@localhost:5432/app"
    pool_min_size: int = 5
    pool_max_size: int = 20
    pool_timeout: float = 30.0

    def get_web_config(self) -> AsyncpgConfig:
        """Configure request-scoped database pool with framework hooks."""
        return AsyncpgConfig(
            bind_key="app-web",
            connection_config={
                "dsn": self.url,
                "min_size": self.pool_min_size,
                "max_size": self.pool_max_size,
                "command_timeout": 30.0,
            },
            migration_config={
                "script_location": str(BASE_DIR / "db" / "migrations"),
                "include_extensions": ["litestar", "adk"],
            },
            extension_config={
                "litestar": {
                    "disable_di": True,
                    "manage_lifespan": True,
                    "session_table": "app_session",
                }
            },
        )

    def get_worker_config(self) -> AsyncpgConfig:
        """Configure worker database pool without request timeout bounds."""
        return AsyncpgConfig(
            bind_key="app-worker",
            connection_config={
                "dsn": self.url,
                "min_size": 2,
                "max_size": 10,
                "command_timeout": None,
            },
            extension_config={"litestar": {"session_key": "worker_session"}},
        )


def _init_db_manager() -> tuple[SQLSpec, AsyncpgConfig]:
    settings = DatabaseSettings()
    db_manager = SQLSpec()
    db = db_manager.add_config(settings.get_web_config())
    db_manager.add_config(settings.get_worker_config())

    sql_dir = BASE_DIR / "db" / "sql"
    if sql_dir.exists():
        db_manager.load_sql_files(sql_dir)

    return db_manager, db


_db_manager: SQLSpec | None = None
_db: AsyncpgConfig | None = None


def __getattr__(name: str) -> Any:
    """Lazily initialize db_manager and primary db handle on first access."""
    global _db, _db_manager
    if name in {"db_manager", "db"}:
        if _db_manager is None:
            _db_manager, _db = _init_db_manager()
        return _db_manager if name == "db_manager" else _db
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
```

Point the CLI migration runner directly to `app.config.db` in `pyproject.toml`:

```toml
[tool.sqlspec]
config = "app.config.db"
```

## Customizing `StatementConfig`

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

Attach observability callbacks for pool creation, connection initialization, session boundaries, or query execution via `ObservabilityConfig(lifecycle=...)` or adapter `driver_features`:

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

Supported `LifecycleConfig` hook keys include `on_pool_create`, `on_pool_destroy`, `on_connection_create`, `on_connection_destroy`, `on_session_start`, `on_session_end`, `on_query_start`, `on_query_complete`, and `on_error`.

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
