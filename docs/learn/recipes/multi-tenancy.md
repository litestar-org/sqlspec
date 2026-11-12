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

=== "uv"

    ```bash
    uv add "sqlspec[asyncpg]"
    ```

=== "pip"

    ```bash
    pip install "sqlspec[asyncpg]"
    ```

=== "pdm"

    ```bash
    pdm add "sqlspec[asyncpg]"
    ```

=== "poetry"

    ```bash
    poetry add "sqlspec[asyncpg]"
    ```

Use an admin connection to create the role, table, and policy. Set the role's login password through your secret store.

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

from sqlspec.adapters.asyncpg import AsyncpgConfig


async def main() -> None:
    config = AsyncpgConfig(
        connection_config={"dsn": os.environ["DATABASE_URL"]},
        driver_features={"tenant_strategy": "rls"},
    )
    try:
        async with config.provide_session(tenant="acme") as session:
            rows = await session.select(
                "SELECT id, name FROM public.projects ORDER BY id"
            )
            assert rows == [{"id": 1, "name": "Billing"}]
    finally:
        await config.close_pool()


asyncio.run(main())
```

Choose a tenant only after the app has checked the user's right to access it. Do not trust a raw header. Without an explicit tenant or an active tenant scope, SQLSpec raises `TenantContextError` before it takes a pool slot. An insert for `globex` in the `acme` session fails the write check.

Sync configs use `with config.provide_session(tenant="acme") as session`. The `tenant` argument accepts a string, UUID, or `TenantContextPayload` with session variables. It binds the tenant through acquisition, application work, and cleanup, then restores the caller's context, including on failure or cancellation. It requires a configured `tenant_strategy`.

Use `TenantContext.scope("acme")` (supports both `with` and `async with`) around several sessions when they share one tenant, or pass `bypass_tenant=True` to `provide_session(bypass_tenant=True)` for a pre-tenant login lookup or health probe. Calling `provide_session()` or passing `tenant=None` inherits the ambient scope. The lease takes one snapshot of the tenant; a later scope change does not alter an open session. Enabling a tenant strategy does not change commit behavior: each adapter's sessions and connections commit or discard unfinished work exactly as they do without one. Call `session.commit()` to save writes portably.

## Preserve Pool Settings

Tenant mode sets `pooler_mode` to `"session"` when you omit it or pass `None`. It rejects `"transaction"`, `"statement"`, and `"auto"`. The lease needs the same server connection for its whole lifetime.

At checkout, one PostgreSQL query saves the old settings and sets the tenant. At checkin, one query restores those settings and checks the result. The strategy changes only keys it owns.

Prefer startup `server_settings` inside `connection_config` for connection-wide PostgreSQL defaults; PostgreSQL applies them at connection startup and retains them across `asyncpg` pool resets with zero per-checkout SQL:

```python
config = AsyncpgConfig(
    connection_config={
        "dsn": os.environ["DATABASE_URL"],
        "server_settings": {
            "TimeZone": "UTC",
            "statement_timeout": "5000",
            "search_path": "public",
        },
    },
    driver_features={"tenant_strategy": "rls"},
)
```

Reserve `driver_features["tenant_session_baseline"]` for settings introduced dynamically inside `init` or `setup` pool hooks that `asyncpg`'s `RESET ALL` clears on release. Pure codec hooks registered via `on_connection_create` default `tenant_session_baseline` to `{}` automatically, whereas `init` and `setup` require an explicit `tenant_session_baseline` (`{}` when they set no lasting GUCs). A baseline value must agree with the same key in `server_settings`. See the [asyncpg reference](../../reference/adapters/asyncpg.md) for details.

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
