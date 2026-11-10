# Tenant APIs

Tenant scopes store an immutable identity in the current Python context. The context helpers perform no database I/O. Configured adapter strategies apply the captured payload to a connection lease; database policies enforce row access.

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

Synchronous and asynchronous scopes restore the preceding payload on normal exit, exceptions, and cancellation. The async equivalents use the same context state:

```python
from sqlspec import TenantContext, async_tenant_bypass, async_tenant_scope


async def read_tenant_context() -> tuple[str | None, bool]:
    async with async_tenant_scope("tenant-a"):
        tenant_id = TenantContext.get_tenant_id()
        async with async_tenant_bypass():
            bypassed = TenantContext.is_bypassed()
        return tenant_id, bypassed
```

Async tasks inherit context when created, and changes stay local to their context. `asyncio.to_thread()` copies context; unmanaged threads and direct executor submissions require an explicit `contextvars.copy_context()`.

Prefer scopes for automatic restoration. For manual binding, `set(payload)` or `set(None)` returns a token for `reset(token)` in the original context. `tenant_context_var` is available from `sqlspec.utils.tenant` for direct context access.

Invalid identities, variable entries, bypass payloads, or values passed to `set()` raise `TenantContextError`, a subclass of both `SQLSpecError` and `ValueError`. Token misuse follows `ContextVar.reset()` errors. `TenantCleanupError` reports failed connection cleanup; context scopes do not raise it. See the [exception reference](exceptions.md) for both error types.

All `TenantContext` methods are class methods:

| Method | Return and behavior |
| --- | --- |
| `get()` | Current `TenantContextPayload`, or `None` when unset. |
| `get_tenant_id()` | Current identity string, or `None` when unset or bypassed. |
| `is_bypassed()` | Whether explicit bypass state is active. |
| `set(payload)` | Restoration token; accepts `TenantContextPayload` or `None`. |
| `reset(token)` | Restores the preceding state; returns `None`. |
| `scope(tenant_id, *, variables=None)` | Sync context manager yielding a payload; accepts a string, UUID, or existing payload. |
| `bypass()` | Sync context manager yielding `TenantContextPayload.bypass()`. |

::: sqlspec.utils.tenant.TenantContextPayload
    options:
      heading_level: 4
      members: [bypass]

::: sqlspec.utils.tenant.TenantContext
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.tenant.async_tenant_scope
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.tenant.async_tenant_bypass
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.exceptions.TenantContextError
    options:
      heading_level: 4
      members: false

::: sqlspec.exceptions.TenantCleanupError
    options:
      heading_level: 4
      members: false

## Request Extraction

Extractors return an app-authorized `TenantContextPayload`, string, UUID, or `None`. Returning `None` clears ambient identity for that request. Middleware restores the caller's token on HTTP, WebSocket, errors, and cancellation. Bypass skips strategy checkout/checkin; it does not disable database policies.

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

## Strategy and Lease Contracts

A strategy returns `TenantLeaseState` from checkout and restores its captured baseline at checkin. Implement these interfaces structurally. Interpreted strategies must not inherit compiled protocols. Built-in adapters own connection acquisition, transaction completion, release, and physical disposal.

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

Configure the plugin or use `require_tenant()` to bind an authorized tenant. An unverified header, path value, or guard declaration alone does not prove membership. See [Tenant Framework Integration](../learn/recipes/multi-tenancy-frameworks.md) for complete examples.

::: sqlspec.extensions.litestar.tenant.TenantResolutionSettings
    options:
      heading_level: 3
      members: false

::: sqlspec.extensions.litestar.tenant.require_tenant
    options:
      heading_level: 3

::: sqlspec.extensions.litestar.tenant.set_connection_tenant
    options:
      heading_level: 3

::: sqlspec.extensions.litestar.tenant.resolve_litestar_tenant
    options:
      heading_level: 3

See [Multi-Tenancy](../learn/recipes/multi-tenancy.md) for policy setup and [adapter references](adapters/index.md) for concrete strategies.
