# Tenant Operations

Run tenant work inside an explicit scope. Use a separate admin config and pool for migrations, stores, and queues. The app role still needs a database policy that checks tenant access; context and session settings alone do not hide rows.

Start with the [PostgreSQL setup recipe](multi-tenancy.md) and use [Tenant Framework Integration](multi-tenancy-frameworks.md) to bind request identity.

## Worker and Thread Scopes

A worker must check access to the chosen tenant before opening a session:

```python
from sqlspec import async_tenant_scope
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def count_projects(config: AsyncpgConfig, tenant_id: str) -> int:
    async with async_tenant_scope(tenant_id, variables={"region": "west"}):
        async with config.provide_session() as session:
            return await session.select_value(
                "SELECT count(*) FROM public.projects", value_type=int
            )
```

If you pass variables, configure a strategy with a matching `variable_keys` map. Unknown variables fail checkout. Each lease holds its original payload even if the ambient scope changes.

Tasks copy the context when they are created. A child task may outlive a request, so request teardown does not revoke the child's copied identity. Give detached jobs their own scope and session. Never share one active driver across concurrent tasks.

Plain threads do not copy context. `asyncio.to_thread()` copies it for the submitted call:

```python
import asyncio

from sqlspec import TenantContext


async def read_identity_in_thread() -> str | None:
    with TenantContext.scope("acme"):
        return await asyncio.to_thread(TenantContext.get_tenant_id)
```

For a direct executor submission, wrap the call with `contextvars.copy_context().run`. Create a fresh copy for each concurrent call. All scope managers restore the caller's token on success or failure.

## Explicit Bypass

Use bypass for a pre-tenant user lookup or a health probe that cannot yet select a tenant:

```python
from sqlspec import TenantContext, async_tenant_bypass
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def probe(config: AsyncpgConfig) -> int:
    async with async_tenant_bypass():
        async with config.provide_session() as session:
            return await session.select_value("SELECT 1", value_type=int)


with TenantContext.bypass() as payload:
    assert payload.is_bypass
```

`TenantContextPayload.bypass()` gives an extractor the same explicit state. Bypass skips strategy checkout and checkin. It still finishes transactions and releases or discards the connection. It does **not** disable RLS, grant admin rights, or clear an existing database policy. A lookup must use a table the app role can read under that policy.

## Administrative Pools

Use a config without `tenant_strategy`, its own pool, and a role with the rights needed for each admin task. This applies to migrations, Litestar session stores, ADK stores, and event queues. Do not reuse the app's tenant pool for these tasks.

Migration commands reject a tenant config even inside a bypass scope. PostgreSQL migration cleanup can reset `search_path`; Oracle schema switching also affects a shared session. A separate pool avoids changing a tenant lease's state. See [Migrations](../migrations.md) for setup.

## Failure and Cancellation

Without a scope or explicit bypass, acquisition raises `TenantContextError` before taking a connection. If checkout fails, the lease discards the connection. At exit it first finishes the transaction, then restores and checks tenant state, then returns the connection. A failed transaction finish skips tenant reset SQL and goes straight to disposal.

Failed reset or release causes disposal too. With no body error, cleanup raises `TenantCleanupError` chained from the cause. With a body error, the body error keeps its identity and gets a cleanup note. Inspect that cause or note when diagnosing a failure.

Async cleanup runs in an owned, shielded task with a five-second default deadline. Outer cancellation waits for cleanup or the deadline, then propagates. On timeout the lease cancels cleanup, waits for it to stop, and discards the connection. Disposal is shielded; adapters provide their own physical close or terminate operation. No cleanup task may return a connection after release has reported completion.

## Database Deployment Limits

| Adapter | Session strategy | Deployment constraint |
| --- | --- | --- |
| asyncpg, psycopg | PostgreSQL session GUCs | Session-affine pooler; reject transaction, statement, and auto modes |
| psqlpy | PostgreSQL session GUCs | Dirty sessions require confirmed backend termination or pool disposal |
| CockroachDB adapters | Session GUCs | Restore an unset owned key as an empty string |
| oracledb | Client identifier or trusted VPD context | Identifier is metadata; the DBA must install the policy and trusted package |
| mssql-python | Mutable `SESSION_CONTEXT` keys | Every config in the process uses `pool_enabled=False` |
| pymssql | Mutable `SESSION_CONTEXT` keys | One standalone connection per tenant lease |

PostgreSQL and SQL Server batch capture/apply and restore/verify into one strategy query or batch per boundary. Finishing a transaction and native pool reset are separate operations. Oracle composes existing Python hooks and verifies state through server reads; those steps can require multiple round trips.

For Oracle, `"default"`, `"identifier"`, and `True` select the client-identifier strategy. A client identifier alone does not enforce row access. VPD uses explicit `OracleSyncVPDStrategy` or `OracleAsyncVPDStrategy` instances and an admin-provisioned trusted package that sets and clears the context. Have your DBA confirm policy design, edition, and licensing for your deployment. See the [Oracle reference](../../reference/adapters/oracledb.md).

For mssql-python, select `"session_context"` and set `connection_config={"pool_enabled": False, ...}`. Tenant mode disables SDK pooling before the first connection. Admin and tenant configs may be constructed in either order only if both are unpooled. An established pooled mode is rejected with an error naming `pool_enabled=False`. Owned keys use mutable values (`@read_only=0`) so checkin can restore them. See the [mssql-python reference](../../reference/adapters/mssql-python.md) and [pymssql reference](../../reference/adapters/pymssql.md).

## Verify a Deployment

Test with a non-owner app role and a real policy. Check reads and writes for two tenants on a size-one pool. Then inject body errors, cancellation, and reset failure; the next tenant must see only its own rows. Confirm that failed cleanup replaces the physical connection.

Local integration tests use pytest-databases containers for PostgreSQL, CockroachDB, Oracle, and SQL Server. Keep fixture namespaces distinct when running lanes in parallel. A service or platform check that could not run supplies no acceptance evidence. Validate your production role, policy, SDK mode, and any Oracle licensing separately from local test results.
