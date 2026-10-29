# Litestar Integration

`sqlspec.extensions.litestar` provides `SQLSpecPlugin`, wiring SQLSpec connection pools into Litestar's application lifespan, registering request-scoped session and connection dependencies, mounting `litestar database` CLI migration commands, providing declarative query filter dependencies, and supplying server-side Litestar `Store` implementations across 13 adapters.

## Installation

```bash
uv add "sqlspec[litestar]"
```

## Plugin Setup and Session Injection

Register one or more database configurations on a `SQLSpec` instance, configure `extension_config["litestar"]`, and pass `SQLSpecPlugin(sqlspec=spec)` to `Litestar(plugins=[...])`:

```{ .python title="docs/examples/extensions/litestar/plugin_setup.py" hl_lines="15-18 23-29" }
--8<-- "docs/examples/extensions/litestar/plugin_setup.py"
```

By default, a single configuration registers `db_session`, `db_connection`, and `db_pool` in Litestar's dependency graph. Override `session_key`, `connection_key`, or `pool_key` inside `extension_config["litestar"]` to customize parameter names.

## Multi-Database Dependency Keys

When an application connects to multiple databases (for example, a transactional primary database and a read-heavy analytics database), assign a distinct `bind_key` and `session_key` to each configuration:

```{ .python title="docs/examples/extensions/litestar/dependency_keys.py" hl_lines="13-27" }
--8<-- "docs/examples/extensions/litestar/dependency_keys.py"
```

## Transaction Commit Modes

Configure `commit_mode` inside `extension_config["litestar"]` to control how request-scoped sessions finalize when a route handler returns:

| Commit Mode | Commits On | Rolls Back On | Best For |
| --- | --- | --- | --- |
| `"manual"` (default) | Explicit `session.commit()` calls only | Unhandled exceptions or uncommitted changes at request exit | Explicit service-layer transaction boundaries |
| `"autocommit"` | HTTP `200`–`299` responses | HTTP `>= 300` responses (`3xx`, `4xx`, `5xx`) or unhandled exceptions | JSON REST APIs where every `2xx` handler mutates state |
| `"autocommit_include_redirect"` | HTTP `200`–`399` responses | HTTP `>= 400` responses or unhandled exceptions | HTML / HTMX form handlers that commit and return `302` / `303` redirects |

```{ .python title="docs/examples/frameworks/litestar/commit_modes.py" hl_lines="13-24" }
--8<-- "docs/examples/frameworks/litestar/commit_modes.py"
```

Add `extra_commit_statuses` or `extra_rollback_statuses` in `LitestarConfig` when specific status codes require custom commit or rollback treatment.

## Declarative Query Filter Providers

Use `create_filter_dependencies` with `FilterConfig` to generate OpenAPI-documented Litestar dependencies that parse query parameters directly into `StatementFilter` lists (`LimitOffsetFilter`, `OrderByFilter`, `SearchFilter`, `BeforeAfterFilter`, `CursorFilter`, and `InCollectionFilter`):

```python
from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from litestar import Litestar, get
from litestar.params import Dependency

from sqlspec.adapters.asyncpg import AsyncpgDriver
from sqlspec.core import FilterTypes
from sqlspec.extensions.litestar.providers import FilterConfig, create_filter_dependencies

user_filters = create_filter_dependencies(
    FilterConfig(
        id_filter=UUID,
        id_field="id",
        sort_field="created_at",
        sort_order="desc",
        pagination_type="limit_offset",
        pagination_size=25,
        pagination_max_size=100,
        search="name,email",
        search_ignore_case=True,
        created_at=True,
        updated_at=True,
    )
)


@get("/users", dependencies=user_filters)
async def list_users(
    db_session: AsyncpgDriver, filters: Annotated[list[FilterTypes], Dependency(skip_validation=True)]
) -> list[dict[str, Any]]:
    return await db_session.select("SELECT id, name, email, created_at FROM users", *filters)
```

For bidirectional keyset pagination (`CursorPagination`), set `cursor_key="id"` (or pass `CursorKey` specs) on `FilterConfig`; see [Filtering and Pagination](../learn/filtering-and-pagination.md).

## Server-Side Session Stores

Thirteen adapters expose a Litestar `Store` implementation under `sqlspec.adapters.<adapter>.litestar` (`AsyncpgStore`, `PsycopgAsyncStore`, `PsqlpyStore`, `AiosqliteStore`, `SqliteStore`, `AsyncmyStore`, `AiomysqlStore`, `PymysqlStore`, `MysqlConnectorAsyncStore`, `OracleAsyncStore`, `SpannerSyncStore`, `SpannerAsyncStore`, `DuckDBStore`, `AdbcStore`, `ArrowOdbcStore`, and `Db2SyncStore`):

```{ .python title="docs/examples/frameworks/litestar/session_stores.py" hl_lines="9-12" }
--8<-- "docs/examples/frameworks/litestar/session_stores.py"
```

Call `await store.create_table()` during application startup (or include the extension migration via `enable_extensions=["litestar"]` in `migration_config`) and pass the store to Litestar's `ServerSideSessionConfig` or `stores={"sessions": store}` registry.

## Recap

- `SQLSpecPlugin` manages pool lifecycles, injects `db_session` / `db_connection` / `db_pool`, and registers `litestar database` CLI commands automatically.
- Set `commit_mode`, `session_key`, `enable_correlation_middleware`, and `enable_sqlcommenter_middleware` in `extension_config["litestar"]`.
- See the [Litestar Extension Reference](/reference/extensions/litestar/) for `SQLSpecPlugin`, `LitestarConfig`, and `FilterConfig` signatures.
