# Litestar Extension Guide

Explains how to wire SQLSpec into Litestar using the official plugin, covering dependency injection, transaction strategies, session storage, and CLI integrations.

## Quick Facts

- Install with `pip install "sqlspec[litestar]"` or `uv pip install "sqlspec[litestar]"`.
- `SQLSpecPlugin` implements Litestar’s plugin protocol and exposes connection, pool, and session dependencies.
- Commit strategies: `manual`, `autocommit`, and `autocommit_include_redirect`, configured via `extension_config["litestar"]["commit_mode"]`.
- Session storage uses adapter-specific stores built on `BaseSQLSpecStore` (e.g., `AsyncpgStore`, `AiosqliteStore`).
- CLI support registers `litestar db ...` commands by including `database_group` in the Litestar CLI app.
- Correlation middleware emits request IDs in query logs (`enable_correlation_middleware=True` by default).

## Installation

```bash
pip install "sqlspec[litestar]"
# or
uv pip install "sqlspec[litestar]"
```

Use the extra that matches your database adapter. The plugin works with async and sync drivers; pick the adapter that aligns with your application.

## Core Plugin Components

- `SQLSpecPlugin`: Registers dependency providers, lifecycle hooks, and optional middleware.
- `LitestarConfig`: Injected through `extension_config` to customize keys, commit modes, and middleware.
- `BaseSQLSpecStore`: Base class for database-backed session stores; adapters provide concrete subclasses (e.g., AsyncpgSQLSpecStore).
- `database_group`: Adds `litestar db` CLI commands for running migrations and inspecting connections.

Create the plugin once per `SQLSpec` instance and attach it to the Litestar app:

```python
from litestar import Litestar
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.litestar import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/app"},
        extension_config={
            "litestar": {
                "commit_mode": "autocommit",
                "enable_correlation_middleware": True,
            }
        },
    )
)

app = Litestar(
    route_handlers=[...],
    plugins=[SQLSpecPlugin(sqlspec=spec)],
)
```

## Dependency Injection Patterns

The plugin registers three dependency types per configuration. Handlers accept them by type annotation or by custom keys:

```python
from litestar import get
from sqlspec.adapters.asyncpg import AsyncpgDriver, AsyncpgPool, AsyncpgConnection

@get("/users")
async def list_users(db_session: AsyncpgDriver) -> dict[str, object]:
    result = await db_session.execute("SELECT * FROM users ORDER BY id LIMIT 100")
    return {"users": result.all()}

@get("/pool-status")
async def pool_status(db_pool: AsyncpgPool) -> dict[str, int]:
    return {"size": db_pool.get_size(), "idle": db_pool.get_idle_size()}

@get("/raw")
async def raw_connection(db_connection: AsyncpgConnection) -> dict[str, int]:
    value = await db_connection.fetchval("SELECT COUNT(*) FROM orders")
    return {"orders": value}
```

Override dependency keys for multi-tenant setups:

```python
config = AsyncpgConfig(
    pool_config={"dsn": "postgresql://localhost/primary"},
    extension_config={
        "litestar": {
            "session_key": "primary_db",
            "connection_key": "primary_connection",
            "pool_key": "primary_pool",
        }
    },
)
```

## Transaction Management

Choose a commit strategy per configuration:

- **Manual** (`manual`): Explicitly call `await db_session.begin()` / `await db_session.commit()` in handlers. Recommended for complex flows.
- **Autocommit** (`autocommit`): Plugin commits automatically on 2xx responses and rolls back on errors.
- **Autocommit with Redirects** (`autocommit_include_redirect`): Same as autocommit but supports 3xx responses.

Switch strategies by setting `extension_config["litestar"]["commit_mode"]`. Manual sessions still support nested transactions by pairing `await session.begin()` with `await session.commit()` or `await session.rollback()`.

## Session Storage

Enable server-side sessions backed by SQLSpec stores:

```python
from datetime import timedelta
from litestar import Litestar
from litestar.middleware.session.server_side import ServerSideSessionConfig
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.adapters.asyncpg.litestar import AsyncpgStore
from sqlspec.extensions.litestar import SQLSpecPlugin

spec = SQLSpec()
db = spec.add_config(
    AsyncpgConfig(
        pool_config={"dsn": "postgresql://localhost/app"},
        extension_config={"litestar": {"session_table": "litestar_sessions"}},
    )
)
store = AsyncpgStore(db)

async def on_startup(app: Litestar) -> None:
    await store.create_table()

app = Litestar(
    plugins=[SQLSpecPlugin(sqlspec=spec)],
    middleware=[
        ServerSideSessionConfig(
            store=store,
            max_age=timedelta(days=1),
        ).middleware
    ],
    on_startup=[on_startup],
)
```

Adapter packages expose store classes (AsyncpgStore, AiosqliteStore, OracledbStore). Each subclass inherits `BaseSQLSpecStore`, enforces consistent schema DDL, and provides utilities such as `delete_expired()`. Run `delete_expired()` periodically or use `litestar sessions delete-expired` from the CLI.

## CLI Integration

Litestar’s CLI picks up SQLSpec commands when you register `database_group`:

```python
from litestar.cli import LitestarCLI
from sqlspec.extensions.litestar import database_group

cli = LitestarCLI(app="app:app")
cli.add_command(database_group, name="db")
```

Commands include `db migrate`, `db upgrade`, `db downgrade`, and `db status`. The CLI uses the same `SQLSpec` instance that the plugin references.

## Middleware and Observability

- Correlation middleware annotates query logs with request-scoped IDs. Disable by setting `enable_correlation_middleware=False`.
- The plugin enforces graceful shutdown by closing pools during Litestar’s lifespan events.
- Combine with Litestar’s `TelemetryConfig` to emit tracing spans around database calls.

## Best Practices

- Register one `SQLSpec` instance per application and call `SQLSpec.close_all_pools()` in shutdown hooks for non-Litestar consumers.
- Prefer injecting driver sessions over raw connections; sessions handle parameter style conversion and result typing.
- Use manual transactions for composite operations (audit logging, aggregate writes).
- Configure session stores with unique table names in multi-tenant environments to keep data separated.
- Keep CLI usage behind role-based access when running migrations in production.
