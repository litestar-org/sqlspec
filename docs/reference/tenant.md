# Tenant APIs

Tenant scopes store an immutable identity in the current Python context. The context helpers perform no database I/O. Configured adapter strategies apply the captured payload to a connection lease; database policies enforce row access.

## Session Providers

Pass an authorized tenant directly to a tenant-enabled config:

```python
async with config.provide_session(tenant="tenant-a") as session:
    rows = await session.select("SELECT id, name FROM projects")

async with config.provide_session(bypass_tenant=True) as lookup:
    user = await lookup.select_one_or_none(
        "SELECT id FROM users WHERE email = :email", email="a@example.com"
    )
```

Sync configs accept the same keywords with `with`, and `SQLSpec.provide_session(config, ..., tenant=..., bypass_tenant=...)` forwards both arguments. `tenant` accepts a string, UUID, or `TenantContextPayload` (use a payload to supply extra session variables); `bypass_tenant=True` binds `TenantContextPayload.bypass()` for a single pre-tenant lookup or health check and cannot be combined with `tenant`. The tenant or bypass state stays bound through session cleanup and restores the prior context on every exit. `tenant=None` with `bypass_tenant=False` inherits the ambient scope. Both keywords require a configured `tenant_strategy`; unsupported adapters reject them.

All tenant-enabled adapters accept `tenant_strategy=True` and `tenant_strategy="default"`. Adapter-specific shorthands select the same built-in strategy. Other adapters, including ADBC and Spanner, reject `tenant_strategy`:

| Database | Adapters | Shorthand | Built-in behavior |
| --- | --- | --- | --- |
| PostgreSQL | `asyncpg`, `psycopg`, `psqlpy` | `"rls"` | Sets session variables for database row policies. |
| CockroachDB | `cockroach_asyncpg`, `cockroach_psycopg` | `"rls"` | Sets session variables for database row policies. |
| SQL Server | `mssql_python`, `pymssql` | `"rls"` or `"session_context"` | Sets mutable `SESSION_CONTEXT` keys for database row policies. |
| Oracle | `oracledb` | `"identifier"` | Sets client-identifier metadata. |
| IBM Db2 | `db2` | `"rcac"` or `"global_variable"` | Sets session global variables for row and column access control. |

Oracle VPD requires an explicit `OracleSyncVPDStrategy` or `OracleAsyncVPDStrategy` with trusted setter, clearer, namespace, and attribute names. There is no parameter-free `"vpd"` shorthand. See the [Oracle reference](adapters/oracledb.md#tenant-strategies).

```python
from sqlspec import TenantContext, TenantContextPayload

tenant = TenantContextPayload("tenant-a", {"region": "west"})
with TenantContext.scope(tenant):
    print(TenantContext.get_tenant_id())
    with TenantContext.bypass():
        print(TenantContext.is_bypassed())
        print(TenantContext.get_tenant_id())
    print(TenantContext.get_tenant_id())
```

This prints `tenant-a`, `True`, `None`, and `tenant-a` in order.

`TenantContextPayload` accepts a nonblank string or UUID, which it converts to a string. Identities must be NUL-free and at most 256 UTF-8 bytes (`MAX_TENANT_ID_BYTES`). Variables are copied into a read-only mapping; keys and values must be NUL-free strings. An existing payload passed to `scope()` is reused unless `variables` supplies merged overrides.

`get()` returns the payload or `None` when unset. `get_tenant_id()` returns `None` for both unset and bypassed contexts; use `is_bypassed()` to distinguish explicit bypass. A bypass payload contains no identity or variables.

Synchronous and asynchronous scopes restore the preceding payload on normal exit, exceptions, and cancellation. Both `TenantContext.scope(...)` and `TenantContext.bypass()` implement the synchronous and asynchronous context-manager protocols directly:

```python
from sqlspec import TenantContext


async def read_tenant_context() -> tuple[str | None, bool]:
    async with TenantContext.scope("tenant-a"):
        tenant_id = TenantContext.get_tenant_id()
        async with TenantContext.bypass():
            bypassed = TenantContext.is_bypassed()
        return tenant_id, bypassed
```

Async tasks inherit context when created, and changes stay local to their context. `asyncio.to_thread()` copies context; unmanaged threads and direct executor submissions require an explicit `contextvars.copy_context()`.

Prefer scopes for automatic restoration. For manual binding, `set(payload)` or `set(None)` returns a token for `reset(token)` in the original context. `tenant_context_var` is available from `sqlspec.utils.tenant` for direct context access.

Invalid identities, variable entries, bypass payloads, or values passed to `set()` raise `TenantContextError`, a subclass of both `SQLSpecError` and `ValueError`. Token misuse follows `ContextVar.reset()` errors. `TenantCleanupError` reports failed connection cleanup; context scopes do not raise it. See the [exception reference](exceptions.md) for both error types.

All `TenantContext` methods are class methods:

| Method | Return and behavior |
| --- | --- |
| `get()` | Current `TenantContextPayload`, or `None` when unset; inside a framework request it resolves the request tenant on first use. |
| `aget()` | Awaitable form of `get()` that also awaits an asynchronous request extractor. |
| `require(key=None)` | Current payload, or the framework's 401/403 inside a request and `TenantContextError` outside one; `key` selects a database configuration. |
| `arequire(key=None)` | Awaitable form of `require()`. |
| `get_tenant_id()` | Current identity string, or `None` when unset or bypassed. |
| `is_bypassed()` | Whether explicit bypass state is active. |
| `set(payload)` | Restoration token; accepts `TenantContextPayload` or `None`. |
| `reset(token)` | Restores the preceding state; returns `None`. |
| `scope(tenant_id, *, variables=None)` | Context manager for `with` or `async with` yielding a payload; accepts a string, UUID, or existing payload. |
| `bypass()` | Context manager for `with` or `async with` yielding `TenantContextPayload.bypass()`. |
| `bind(payload)` | Sync context manager binding an already resolved payload, or `None`, around connection work. |
| `bind_request(binding)` | Binds a `TenantRequestBinding` and returns its restoration token; framework plugins call it at request start. |
| `current_binding()` | Raw bound value (payload, request binding, or `None`) without resolving anything. |

::: sqlspec.utils.tenant.TenantContextPayload
    options:
      heading_level: 4
      members: [bypass]

::: sqlspec.utils.tenant.TenantContext
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.TenantContextError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.TenantCleanupError
    options:
      heading_level: 4
      members: false

## Request Extraction

Extractors return an app-authorized `TenantContextPayload`, string, UUID, or `None`. Inside a framework plugin, `None` means the extractor has no tenant and resolution continues with the ambient scope; see [One Request Contract](../learn/recipes/multi-tenancy-frameworks.md#one-request-contract). `SQLSpecTenantMiddleware` binds its extractor result, including `None`, for the whole ASGI call. Middleware restores the caller's token on HTTP, WebSocket, errors, and cancellation. Bypass skips strategy checkout/checkin; it does not disable database policies.

::: sqlspec.utils.tenant.coerce_tenant_payload
    options:
      heading_level: 3

::: sqlspec.utils.tenant.SQLSpecTenantMiddleware
    options:
      heading_level: 3
      members: [__init__, __call__]

::: sqlspec.utils.tenant.single_membership_tenant_extractor
    options:
      heading_level: 3

::: sqlspec.utils.tenant.TenantRequestBinding
    options:
      heading_level: 3
      members: [__init__, resolve, aresolve, require, arequire, freeze, afreeze, frozen]

::: sqlspec.utils.tenant.tenant_bypass
    options:
      heading_level: 3

## Strategy and Lease Contracts

A strategy returns `TenantLeaseState` or a baseline `Mapping[str, str | None]` from checkout. The lease freezes a returned mapping with the captured payload before checkin. Implement these interfaces structurally. Interpreted strategies must not inherit compiled protocols. Built-in adapters own connection acquisition, transaction completion, release, and physical disposal through `SyncTenantLease` and `AsyncTenantLease` subclasses. A config with a tenant strategy but no lease implementation refuses to hand out connections.

::: sqlspec.core.tenant.base.TenantLeaseState
    options:
      heading_level: 3
      members: [__init__]

::: sqlspec.core.tenant.base.SyncTenantStrategy
    options:
      heading_level: 3
      members: [checkout, checkin]

::: sqlspec.core.tenant.base.AsyncTenantStrategy
    options:
      heading_level: 3
      members: [checkout, checkin]

## Litestar Tenant Resolution

Configure `extension_config["litestar"]` on `SQLSpecPlugin(config)` or attach `require_tenant()`, which inherits the registered plugin's `TenantResolutionSettings` automatically unless overridden per route. `resolve_litestar_tenant` also integrates `litestar-security` `Principal`, `SecurityContext`, `AuthorizationSnapshot` (`tenant_ids` and `tenant_roles`), `CredentialRestrictions`, `SessionHandle`, `requires_tenant`, and `requires_tenant_role`. `set_connection_tenant(..., key=...)` scopes a receipt to one database configuration. Other frameworks expose `set_request_tenant(...)` from `sqlspec.extensions.{fastapi,starlette,flask,sanic}` with the same `key=` argument. See [Tenant Framework Integration](../learn/recipes/multi-tenancy-frameworks.md) for complete examples.

::: sqlspec.extensions.litestar.tenant.TenantResolutionSettings
    options:
      heading_level: 4
      members: false

::: sqlspec.extensions.litestar.tenant.require_tenant
    options:
      heading_level: 4

::: sqlspec.extensions.litestar.tenant.is_scope_authenticated
    options:
      heading_level: 4

::: sqlspec.extensions.litestar.tenant.set_connection_tenant
    options:
      heading_level: 4

::: sqlspec.extensions.litestar.tenant.resolve_litestar_tenant
    options:
      heading_level: 4

See [Multi-Tenancy](../learn/recipes/multi-tenancy.md) for policy setup and [adapter references](adapters/index.md) for concrete strategies.
