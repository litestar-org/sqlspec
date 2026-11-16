# Tenant Operations

Run tenant work inside an explicit scope. Use a separate admin config and pool for migrations, stores, and queues. The app role still needs a database policy that checks tenant access; context and session settings alone do not hide rows.

Start with the [PostgreSQL setup recipe](multi-tenancy.md) and use the [Tenant Request Contract](multi-tenancy-frameworks.md) to bind request identity.

## Worker and Thread Scopes

A worker must check access to the chosen tenant before opening a session:

```python
from sqlspec import TenantContext, TenantContextPayload
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def count_projects(config: AsyncpgConfig, tenant_id: str) -> int:
    payload = TenantContextPayload(tenant_id, variables={"region": "west"})
    async with config.provide_session(tenant=payload) as session:
        return await session.select_value(
            "SELECT count(*) FROM public.projects", value_type=int
        )


async def run_batch(config: AsyncpgConfig, tenant_id: str) -> int:
    with TenantContext.scope(tenant_id, variables={"region": "west"}):
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
from sqlspec import TenantContext
from sqlspec.adapters.asyncpg import AsyncpgConfig


async def probe(config: AsyncpgConfig) -> int:
    async with config.provide_session(bypass_tenant=True) as session:
        return await session.select_value("SELECT 1", value_type=int)


with TenantContext.bypass() as payload:
    assert payload.is_bypass
```

`provide_session(bypass_tenant=True)` scopes bypass to that single session without mutating outer tasks, while `TenantContext.bypass()` (for `with` or `async with`) wraps multiple sessions and `TenantContextPayload.bypass()` gives an extractor the same explicit state. Bypass skips strategy checkout and checkin. It still finishes transactions and releases or discards the connection. It does **not** disable RLS, grant admin rights, or clear an existing database policy. A lookup must use a table the app role can read under that policy.

## Administrative Pools

Use a config without `tenant_strategy`, its own pool, and a role with the rights needed for each admin task. This applies to migrations, Litestar session stores, ADK stores, and event queues. Do not reuse the app's tenant pool for these tasks.

Migration commands reject a tenant config even inside a bypass scope. PostgreSQL migration cleanup can reset `search_path`; Oracle schema switching also affects a shared session. A separate pool avoids changing a tenant lease's state. See [Migrations](../migrations.md) for setup.

## Failure and Cancellation

Without a scope or explicit bypass, acquisition raises `TenantContextError` before taking a connection. If checkout fails, the lease discards the connection. At exit it first finishes the transaction, then restores and checks tenant state, then returns the connection. A failed transaction finish skips tenant reset SQL and goes straight to disposal.

Failed reset or release causes disposal too. With no body error, cleanup raises `TenantCleanupError` chained from the cause. With a body error, the body error keeps its identity and gets a cleanup note. Inspect that cause or note when diagnosing a failure. If physical close fails, psycopg closes the owning pool before its native context can return a dirty connection. Recreate that pool after diagnosing the error.

Async cleanup owns a shielded task with a five-second default deadline. At the deadline, the lease cancels cleanup. It waits for that work to stop before it discards the connection. A cancelled caller also waits for owned work to stop. A native reset hook must respond to cancellation. If a hook ignores it, cleanup waits longer to keep the pool safe. No old release task may act on a connection that another lease has acquired.

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

For Oracle, `"default"`, `"identifier"`, and `True` select the client-identifier strategy, and any `driver_features["session_context_hook"]` is attached to the built-in strategy automatically when the strategy omits a hook. A client identifier alone does not enforce row access. VPD uses explicit `OracleSyncVPDStrategy` or `OracleAsyncVPDStrategy` instances and an admin-provisioned trusted package that sets and clears the context. Have your DBA confirm policy design, edition, and licensing for your deployment. See the [Oracle reference](../../reference/adapters/oracledb.md).

For mssql-python, `"rls"` and `"session_context"` default `pool_enabled` to `False` automatically when omitted from `connection_config` (rejecting explicit `pool_enabled=True`). Because `mssql-python` sets pooling process-wide before the first connection, set `connection_config={"pool_enabled": False, ...}` on any non-tenant administrative `MssqlPythonConfig` created in the same process. Owned keys use mutable values (`@read_only=0`) so checkin can restore them. See the [mssql-python reference](../../reference/adapters/mssql-python.md) and [pymssql reference](../../reference/adapters/pymssql.md).

## Oracle Client Context and Security

Use `OracleSyncClientContextStrategy` or `OracleAsyncClientContextStrategy` to bind a tenant to `CLIENTCONTEXT`. The SDK sends this state with the next request, with no extra setup query. List each allowed tenant key in `variable_keys`. Unknown keys fail checkout.

```python
from sqlspec.adapters.oracledb import (
    OracleAsyncClientContextStrategy,
    OracleAsyncConfig,
)
from sqlspec.utils.tenant import TenantContextPayload

config = OracleAsyncConfig(
    connection_config={
        "dsn": "localhost:1521/FREEPDB1",
        "user": "app",
        "password": "secret",
    },
    driver_features={
        "tenant_strategy": OracleAsyncClientContextStrategy(
            variable_keys=("region",)
        )
    },
)
payload = TenantContextPayload("acme", variables={"region": "west"})
```

Pass `payload` to `config.provide_session(tenant=payload)`. The [runnable example](https://github.com/litestar-org/sqlspec/blob/main/docs/examples/databases/oracledb/zero_rtt_tenancy.py) reads both values back from `SYS_CONTEXT`. A database policy must check this state to enforce tenant access.

??? example "Complete client-context example"

    ```python
    --8<-- "docs/examples/databases/oracledb/zero_rtt_tenancy.py:example"
    ```

Use `OracleSyncEndUserSecurityStrategy` or `OracleAsyncEndUserSecurityStrategy` for Deep Data Security on a supported Oracle 26ai Thin TCPS server. These bind a database access token, user identity, data roles, and declared fields. The token can be a value or a refresh callable. Token providers read the captured `end_user_token` tenant field. Trusted identities can use the tenant ID with an optional `end_user_key`. Each socket owns its security state. Checkin clears it. The SDK plugin's thread-local state is never used for request secrets.

Shard routing reads the captured payload before checkout. Choose one of `use_tenant_id_as_sharding_key=True`, `sharding_key_variable`, or `shard_resolver`. A resolver returns primary and super-shard key sequences. Direct client routing needs Thick mode. Thin mode uses a suitable routing endpoint. Tenant and stateful session hooks reject `pool_boundary="statement"` because a lease needs its state to last across queries.

The identifier and VPD strategies still verify the server state. Supported async Thin paths can batch setup and `SYS_CONTEXT` checks into one native pipeline. Finishing the transaction and release remain separate steps. Client-context and security strategies clear native state at checkin. If a clear, reset, or release fails, `pool.drop()` evicts the socket. The next tenant must never receive it.

## Verify a Deployment

Test with a non-owner app role and a real policy. Check reads and writes for two tenants on a size-one pool. Then inject body errors, cancellation, and reset failure; the next tenant must see only its own rows. Confirm that failed cleanup replaces the physical connection.

Local integration tests use pytest-databases containers for PostgreSQL, CockroachDB, Oracle, and SQL Server. Keep fixture namespaces distinct when running lanes in parallel. A service or platform check that could not run supplies no acceptance evidence. Validate your production role, policy, SDK mode, and any Oracle licensing separately from local test results.
