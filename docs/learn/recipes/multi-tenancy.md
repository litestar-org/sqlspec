# Multi-Tenancy

Keep each customer's rows in one `projects` table with PostgreSQL row-level security (RLS). SQLSpec sets the tenant when a session opens. It restores the prior settings before the pool reuses that connection. The policy decides which rows the app can read or write.

## Choose an Isolation Model

| Model | Boundary | SQLSpec configuration |
| --- | --- | --- |
| Shared tables with RLS | A database policy checks each row | `tenant_strategy="rls"` on a shared session pool |
| Schema per tenant | Separate namespaces in one database | A trusted schema resolver or dedicated schema configs |
| Database per tenant | Separate databases and pools | One configuration per `bind_key` |

This guide uses shared tables. For request auth, see [Tenant Framework Integration](multi-tenancy-frameworks.md). For worker tasks and server setup, see [Tenant Operations](multi-tenancy-operations.md).

## Create the Policy and Application Role

Install the driver with `uv add 'sqlspec[asyncpg]'`. Use an admin connection to create the role, table, and policy. Set the role's login password through your secret store.

```sql
CREATE ROLE tenant_app LOGIN NOSUPERUSER NOBYPASSRLS;
CREATE TABLE public.projects (
    id integer PRIMARY KEY,
    tenant_id text NOT NULL,
    name text NOT NULL
);
INSERT INTO public.projects VALUES
    (1, 'acme', 'Billing'),
    (2, 'globex', 'Search');
ALTER TABLE public.projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.projects FORCE ROW LEVEL SECURITY;
CREATE POLICY project_tenant ON public.projects
    USING (tenant_id = current_setting('app.current_tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.current_tenant_id', true));
GRANT USAGE ON SCHEMA public TO tenant_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.projects TO tenant_app;
```

Connect as `tenant_app`. This role must not own the table or have superuser or `BYPASSRLS` rights. `USING` filters reads; `WITH CHECK` rejects writes for the wrong tenant. A session setting alone does not hide rows.

## Open a Tenant Session

Set `DATABASE_URL` to the app role's DSN, then run:

```python
import asyncio
import os

from sqlspec import async_tenant_scope
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def main() -> None:
    config = AsyncpgConfig(
        connection_config={"dsn": os.environ["DATABASE_URL"]},
        driver_features={"tenant_strategy": "rls"},
    )
    try:
        async with async_tenant_scope("acme"):
            async with config.provide_session() as session:
                rows = await session.select(
                    "SELECT id, name FROM public.projects ORDER BY id"
                )
                assert rows == [{"id": 1, "name": "Billing"}]
    finally:
        await config.close_pool()


asyncio.run(main())
```

Choose a tenant only after the app has checked the user's right to access it. Do not trust a raw header. Without a tenant scope, SQLSpec raises `TenantContextError` before it takes a pool slot. An insert for `globex` in the `acme` scope fails the write check.

You can also use `TenantContext.scope("acme")` in sync or async code. The lease takes one snapshot of the tenant. A later scope change does not change an open session. Call `session.commit()` to save writes. The lease rolls back any open transaction before cleanup.

## Preserve Pool Settings

Tenant mode sets `pooler_mode` to `"session"` when you omit it or pass `None`. It rejects `"transaction"`, `"statement"`, and `"auto"`. The lease needs the same server connection for its whole lifetime.

At checkout, one PostgreSQL query saves the old settings and sets the tenant. At checkin, one query restores those settings and checks the result. The strategy changes only keys it owns. Ending a transaction, resetting the native pool, and replaying a baseline each take their own steps.

Asyncpg's pool resets settings on release. List the values to restore before each new tenant checkout:

```python
config = AsyncpgConfig(
    connection_config={"dsn": os.environ["DATABASE_URL"]},
    driver_features={
        "tenant_strategy": "rls",
        "tenant_session_baseline": {
            "TimeZone": "UTC",
            "statement_timeout": "5000",
            "search_path": "public",
        },
    },
)
```

If you use `init`, `setup`, or `on_connection_create` hooks, pass `tenant_session_baseline` too. Use `{}` if the hook sets no lasting values. A baseline value must agree with the same key in `server_settings`. See the [asyncpg reference](../../reference/adapters/asyncpg.md) for details.

## Transaction-Mode Pooler Alternative

For a transaction-mode pooler, omit `tenant_strategy`. Set the tenant inside each transaction:

```python
async with config.provide_session() as session:
    await session.begin()
    try:
        await session.execute(
            "SELECT set_config('app.current_tenant_id', :tenant_id, true)",
            tenant_id="acme",
        )
        rows = await session.select("SELECT id, name FROM public.projects")
        await session.commit()
    except BaseException:
        await session.rollback()
        raise
```

The final `true` limits the setting to that transaction. A commit or rollback clears it. Set it again in the next transaction. Keep the same app role and RLS policy.

## Schema and Database Alternatives

For PostgreSQL schemas, pass `AsyncpgRLSStrategy(schema_resolver=...)`. This trusted function maps the tenant payload to a schema. Names must match `[a-z_][a-z0-9_]*`. SQLSpec quotes the name and restores the old `search_path`. Never use raw request input as a schema name. For Oracle or Db2, use a dedicated pool with a fixed schema.

For separate databases, give each tenant a config with its own `bind_key` on `SQLSpec`. Check access before choosing the config. Each config owns its pool, so it can use its own login and maintenance schedule.

Run [migrations](../migrations.md) with a separate admin config and pool. Omit `tenant_strategy` on that config. See [Tenant Operations](multi-tenancy-operations.md) for bypass scopes, cleanup errors, Oracle VPD, and SQL Server setup.
