# Multi-Tenancy

Multi-tenant SaaS applications typically isolate customer data using one of three database models: **shared table with `tenant_id`**, **schema-per-tenant**, or **database-per-tenant**. SQLSpec supports all three cleanly without global mutable connection state.

## Choosing an Isolation Model

| Model | Isolation Boundary | Operational Trade-Off | SQLSpec Mechanism |
| --- | --- | --- | --- |
| **Row-Level (`tenant_id` + RLS)** | Shared tables, shared pool | Highest connection utilization; requires consistent `tenant_id` binding | Named `:tenant_id` parameters + PostgreSQL `SET LOCAL app.tenant_id` |
| **Schema-per-Tenant** | Dedicated schema in one database | Clean logical separation and per-tenant migrations | `SET LOCAL search_path` (Postgres) / `ALTER SESSION SET CURRENT_SCHEMA` (Oracle) |
| **Database-per-Tenant** | Dedicated database or cluster | Strongest blast-radius and compliance isolation | Multiple configs registered on `SQLSpec` keyed by `bind_key` |

## Row-Level Isolation with Transaction-Scoped Context

Combine explicit `:tenant_id` predicates with transaction-local session settings (`SET LOCAL` in PostgreSQL) so connection pool checkouts—including transaction-mode PgBouncer—never leak tenant state to subsequent requests:

```python
from collections.abc import Iterator
from contextlib import contextmanager

from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig, SqliteDriver

spec = SQLSpec()
config = spec.add_config(SqliteConfig(connection_config={"database": ":memory:"}))


@contextmanager
def tenant_session(tenant_id: str) -> Iterator[tuple[SqliteDriver, str]]:
    with spec.provide_session(config) as session:
        yield session, tenant_id


with spec.provide_session(config) as setup:
    setup.execute(
        """
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            name TEXT NOT NULL
        )
        """
    )
    setup.execute(
        """
        INSERT INTO projects (tenant_id, name)
        VALUES (:tenant_id, :name)
        """,
        tenant_id="acme",
        name="Billing",
    )
    setup.execute(
        """
        INSERT INTO projects (tenant_id, name)
        VALUES (:tenant_id, :name)
        """,
        tenant_id="globex",
        name="Search",
    )

with tenant_session("acme") as (session, tenant_id):
    rows = session.select(
        """
        SELECT id, name
        FROM projects
        WHERE tenant_id = :tenant_id
        """,
        tenant_id=tenant_id,
    )
    assert rows == [{"id": 1, "name": "Billing"}]
```

On PostgreSQL (`AsyncpgConfig`, `PsycopgAsyncConfig`, `PsqlpyConfig`), enforce defense-in-depth Row-Level Security inside a transaction:

```python
async with config.provide_session() as session:
    await session.begin()
    await session.execute("SELECT set_config('app.tenant_id', :tenant_id, true)", tenant_id=tenant_id)
    rows = await session.select(
        """
        SELECT id, name
        FROM projects
        ORDER BY id
        """
    )
    await session.commit()
```

Passing `true` as the third argument to `set_config` scopes the setting strictly to the current transaction (`SET LOCAL`), ensuring pooled connections reset automatically on `commit()` or `rollback()`.

## Schema-per-Tenant Routing

Validate the resolved schema slug against a strict identifier allowlist (`^[a-z0-9_]+$`) before switching schemas inside a transaction:

- **PostgreSQL**: `SELECT set_config('search_path', :schema_name, true)` inside `session.begin()`.
- **Oracle**: `OracleConnectionParams(current_schema=...)` for static pools or `ALTER SESSION SET CURRENT_SCHEMA = ...` on dedicated tenant pools.
- **IBM Db2**: `Db2ConnectionParams(current_schema=...)` or `SET CURRENT SCHEMA`.
- **Migrations**: Use `migration_config={"default_schema": tenant_schema, "version_table_schema": "admin"}` to upgrade each tenant schema deterministically.

## Database-per-Tenant with `bind_key`

For dedicated enterprise tenants, register one adapter configuration per tenant `bind_key` on `SQLSpec`:

```python
from sqlspec import SQLSpec
from sqlspec.adapters.sqlite import SqliteConfig

spec = SQLSpec()
tenant_configs = {
    "acme": spec.add_config(SqliteConfig(bind_key="acme", connection_config={"database": ":memory:"})),
    "globex": spec.add_config(SqliteConfig(bind_key="globex", connection_config={"database": ":memory:"})),
}

with spec.provide_session(tenant_configs["acme"]) as session:
    value = session.select_value("SELECT 1", value_type=int)
```

## Recap

- Never store active tenant IDs on global config singletons; bind tenant context per request or transaction.
- Prefer `set_config(..., true)` (`SET LOCAL`) over session-level `SET` whenever sharing connection pools across tenants.
- Validate schema or table identifiers against a strict regex allowlist before any DDL or schema-switch statement.
