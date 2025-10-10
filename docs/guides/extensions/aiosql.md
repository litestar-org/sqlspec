# aiosql Extension Guide

Focuses on loading aiosql-style SQL files inside SQLSpec, when to use the native loader versus the compatibility adapters, and how to keep query execution fast and type-safe.

## Quick Facts

- Optional dependency: install with `pip install "sqlspec[aiosql]"` or `uv pip install "sqlspec[aiosql]"` to enable the adapter.
- Two paths:
  - **SQLFileLoader** for SQLSpec-native projects (no aiosql operators).
  - **Aiosql adapters** for existing aiosql repositories that rely on operators like `^`, `$`, `!`, `*!`, and `#`.
- Adapters wrap SQLSpec drivers, so you keep connection pooling, parameter style conversion, and schema typing.
- Comments between `-- name:` and the SQL body become documentation strings for generated methods—use them for quick API recall.

## Installation

```bash
pip install "sqlspec[aiosql]"
# or
uv pip install "sqlspec[aiosql]"
```

Verify the optional package before wiring the adapter; `AiosqlAsyncAdapter` and `AiosqlSyncAdapter` raise `MissingDependencyError` when `aiosql` is absent.

## Choosing an Integration Strategy

- **Prefer SQLFileLoader** when you control the SQL files and do not depend on aiosql operators. You gain dialect-aware caching, schema typing, and remote storage support.
- **Use the aiosql adapters** when you already have `.sql` files organized with aiosql, need operator semantics (`^` for select-one, `$` for scalar, `!`/`<!` for DML), or want to share query packs with code still on vanilla aiosql.
- You can mix both approaches in the same project: load new statements through the loader while keeping legacy files on the adapter until they are migrated.

## Async Workflow (AiosqlAsyncAdapter)

```python
import asyncio
import aiosql
from sqlspec import SQLSpec
from sqlspec.adapters.asyncpg import AsyncpgConfig
from sqlspec.extensions.aiosql import AiosqlAsyncAdapter


async def main() -> None:
    spec = SQLSpec()
    config = spec.add_config(AsyncpgConfig(pool_config={"dsn": "postgresql://localhost/app"}))

    async with spec.provide_driver(config) as driver:
        adapter = AiosqlAsyncAdapter(driver)
        queries = aiosql.from_path("queries/users.sql", adapter)

        async with spec.provide_connection(config) as conn:
            users = await queries.get_all_users(conn)
            count = await queries.get_user_count(conn)
            await queries.create_user(conn, username="alice", email="alice@example.com")

            # Map results with SQLSpec typing
            await queries.get_user_by_id(
                conn,
                user_id=1,
                _sqlspec_schema_type="sqlspec.schemas.User",
            )


asyncio.run(main())
```

Key points:

- `spec.provide_driver` supplies the SQLSpec driver instance expected by the adapter.
- `spec.provide_connection` yields the raw connection passed as the first argument to generated query methods.
- Pass `_sqlspec_schema_type` or `schema_type` to the adapter methods when you want result typing; `record_class` is ignored for compatibility.

## Sync Workflow (AiosqlSyncAdapter)

```python
import aiosql
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig
from sqlspec.extensions.aiosql import AiosqlSyncAdapter


spec = SQLSpec()
config = spec.add_config(SqliteConfig(database="app.db"))

with spec.provide_driver(config) as driver:
    adapter = AiosqlSyncAdapter(driver)
    queries = aiosql.from_path("queries/reports.sql", adapter)

    with spec.provide_connection(config) as conn:
        rows = list(queries.get_monthly_totals(conn, month="2025-10"))
        total = queries.get_total_revenue(conn)
        queries.insert_audit_entry(conn, actor="system", action="refresh")
```

## Best Practices

- Keep SQL files in namespaced directories (`analytics/users.sql`, `inventory/orders.sql`)—`aiosql.from_path` exposes subdirectories as attributes (`queries.analytics.list_reports`).
- Include meaningful comments under each `-- name:` tag; aiosql surfaces them as docstrings in `help()` output.
- Normalize dialects in adapter usage by choosing SQLSpec drivers whose `.dialect` attribute matches your SQL (e.g., `AsyncpgDriver` sets `postgres`).
- For schema-aware results, prefer `_sqlspec_schema_type` parameters or run queries through SQLSpec sessions where the driver handles mapping.
- Cache adapters or `queries` modules at module scope. Loading and parsing `.sql` files on every call defeats the statement cache advantages.

## Common Pitfalls

- **Missing dependency**: Forgetting to install the `aiosql` extra raises `MissingDependencyError`. Install before constructing adapters.
- **Wrong execution object**: Passing the SQLSpec driver instead of the underlying connection to generated methods will fail. Always call `queries.method(conn, ...)` with a connection from `spec.provide_connection()`.
- **Mixing operators with SQLFileLoader**: The native loader ignores aiosql operators (`^`, `$`, `!`). Use the adapter until you translate those statements.
- **Deprecated `record_class`**: aiosql’s `record_class` argument is ignored. Use schema typing or data models handled by SQLSpec instead.
- **Dialect mismatches**: Ensure your SQLSpec driver’s `dialect` matches the SQL in your aiosql files; otherwise SQLGlot transformations (validation, conversions) may not align.

## Resources

- aiosql documentation: https://nackjicholson.github.io/aiosql/
- Getting started with aiosql named queries: https://nackjicholson.github.io/aiosql/getting-started/
- SQLSpec aiosql extension docs: `docs/extensions/aiosql/`
- SQLSpec SQL file loader reference: `docs/extensions/aiosql/usage`
