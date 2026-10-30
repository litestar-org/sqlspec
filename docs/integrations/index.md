# Framework and Ecosystem Integrations

SQLSpec ships first-party framework plugins and ecosystem extensions under `sqlspec.extensions.*` plus native Dishka dependency-injection patterns. Every integration manages connection pools at application startup/shutdown and provides request- or task-scoped driver sessions with configurable transaction commit behavior.

## Integration Matrix

| Integration | Module / Package | Install Extra | Request DI | Commit Modes | Server-Side Stores | Filter Providers | CLI Integration |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **[Litestar](litestar.md)** | `sqlspec.extensions.litestar` | `sqlspec[litestar]` | `Provide` (`db_session`, `db_connection`, `db_pool`) | `manual`, `autocommit`, `autocommit_include_redirect` | `Store` (`13` adapters) | `FilterConfig` (`LimitOffset`, `BeforeAfter`, `Search`, `OrderBy`, `Cursor`, `InCollection`) | `litestar database ...` |
| **[Google ADK](adk.md)** | `sqlspec.extensions.adk` | `sqlspec[adk]` | Runner / Agent services | Atomic store operations | `SQLSpecSessionService`, `SQLSpecMemoryService`, `SQLSpecArtifactService` (`19` adapters) | Vector, FTS, and Hybrid RRF memory search | `sqlspec adk memory ...` |
| **[Events & Queues](events.md)** | `sqlspec.extensions.events` | Built-in (`sqlspec`) | `SyncEventChannel` / `AsyncEventChannel` | Transactional publish / ack | `notify`, `notify_queue`, `poll_queue`, Oracle `aq` / `txeventq` (`19` adapters) | Channel + metadata envelope filtering | Schema provisioning via `MigrationCommands` |
| **[FastAPI](fastapi.md)** | `sqlspec.extensions.fastapi` | `sqlspec[fastapi]` | `Depends(db_ext.provide_session())` | `manual`, `autocommit`, `autocommit_include_redirect` | — | `FilterConfig` query parameter dependencies | `sqlspec` CLI |
| **[Starlette](starlette.md)** | `sqlspec.extensions.starlette` | `sqlspec[starlette]` | `db_plugin.get_session(request)` | `manual`, `autocommit`, `autocommit_include_redirect` | — | — | `sqlspec` CLI |
| **[Flask](flask.md)** | `sqlspec.extensions.flask` | `sqlspec[flask]` | `plugin.get_session()` (`g`-scoped) | `manual`, `autocommit`, `autocommit_include_redirect` | — | — | `sqlspec` CLI |
| **[Sanic](sanic.md)** | `sqlspec.extensions.sanic` | `sqlspec[sanic]` | `db_plugin.get_session(request)` / `ext.dependency` | `manual`, `autocommit`, `autocommit_include_redirect` | — | — | `sqlspec` CLI |
| **[Dishka](dishka.md)** | `dishka` | `uv add dishka` | `FromDishka[DriverT]` (`Scope.REQUEST`) | Explicit session or service scope | Combines with any adapter store | Combines with `SQLSpecAsyncService` / `SQLSpecSyncService` | Shared `Scope.APP` container across web, CLI, and workers |

## Shared `extension_config` Pattern

Framework and extension settings live on each adapter configuration inside `extension_config`, keeping pool settings, statement compilation, and framework integration in one typed declaration:

```python
from sqlspec.adapters.asyncpg import AsyncpgConfig

config = AsyncpgConfig(
    connection_config={"dsn": "postgresql://localhost/app", "min_size": 2, "max_size": 20},
    extension_config={
        "litestar": {"session_key": "db_session", "commit_mode": "autocommit"},
        "events": {"backend": "notify_queue", "event_poll_interval": 1.0},
        "adk": {
            "session_table": "adk_sessions",
            "events_table": "adk_events",
            "memory_table": "adk_memories",
            "memory_use_fts": True,
        },
    },
)
```

## Choosing an Integration

- **ASGI & WSGI Web Frameworks**: Use [Litestar](litestar.md), [FastAPI](fastapi.md), [Starlette](starlette.md), [Flask](flask.md), or [Sanic](sanic.md) when you want automatic pool lifecycle hooks (`lifespan` / `on_startup` / `teardown_appcontext`), per-request session cleanup, correlation ID middleware, and HTTP status-aware transaction commit modes.
- **Multi-Entrypoint IoC Containers**: Use [Dishka](dishka.md) when HTTP handlers, background workers, and CLI commands share a single `Provider` graph with `Scope.APP` pools and `Scope.REQUEST` driver sessions.
- **AI Agents & Long-Term Memory**: Use [Google ADK](adk.md) to persist agent conversation sessions, events, versioned artifacts, and full-text or vector/hybrid RRF memories across SQLite, PostgreSQL (`pgvector` / `ParadeDB`), Oracle 26ai, Cloud Spanner, DuckDB, MySQL, SQL Server, and IBM Db2.
- **Transactional Pub/Sub & Competing Consumers**: Use [Events & Queues](events.md) for PostgreSQL `LISTEN`/`NOTIFY`, durable `notify_queue` wakeups, Oracle Advanced Queuing (`aq` / `txeventq`), or portable table-backed queues (`poll_queue`).
