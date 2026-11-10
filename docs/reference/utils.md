# Utilities

```python
from sqlspec.utils.arrow_helpers import (
    coerce_arrow_batches,
    coerce_arrow_table,
    encode_arrow_batch_pg_binary,
    iter_pgcopy_binary_chunks,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.deprecation import deprecated
from sqlspec.utils.env import get_env
from sqlspec.utils.fixtures import (
    export_table_fixtures_async,
    export_table_fixtures_sync,
    load_table_fixtures_async,
    load_table_fixtures_sync,
)
from sqlspec.utils.module_loader import import_optional, import_string
from sqlspec.utils.schema import to_schema
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_
from sqlspec.utils.text import camelize, snake_case
from sqlspec.utils.type_guards import (
    is_msgspec_struct,
    is_pydantic_model,
    is_schema,
)
from sqlspec.utils.uuids import nanoid, uuid4, uuid6, uuid7
```

Reusable helpers for runtime type narrowing, JSON serialization, schema model conversion, UUID/NanoID generation, Apache Arrow batch/table coercion, table fixture loading/exporting, tenant and correlation tracking, and sync/async bridging.

## Tenant Context

Tenant scopes store an immutable identity in the current Python context. They perform no database I/O and do not enforce database isolation. Connection leases, adapter tenant strategies, and automatic framework extraction are not implemented.

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

Invalid identities, variable entries, bypass payloads, or values passed to `set()` raise `TenantContextError`, a subclass of both `SQLSpecError` and `ValueError`. Token misuse follows `ContextVar.reset()` errors. `TenantCleanupError` is exported for connection cleanup failures; context scopes do not raise it.

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

## Schema, Serialization & Identifiers

::: sqlspec.utils.schema.to_schema
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.serializers.to_json
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.serializers.from_json
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.uuids.uuid4
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.uuids.uuid6
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.uuids.uuid7
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.uuids.nanoid
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.text.camelize
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.text.snake_case
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Arrow Coercion & Binary Helpers

::: sqlspec.utils.arrow_helpers.coerce_arrow_table
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.arrow_helpers.coerce_arrow_batches
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.arrow_helpers.encode_arrow_batch_pg_binary
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.arrow_helpers.iter_pgcopy_binary_chunks
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Table Fixtures & Environment Helpers

::: sqlspec.utils.fixtures.load_table_fixtures_sync
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.fixtures.load_table_fixtures_async
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.fixtures.export_table_fixtures_sync
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.fixtures.export_table_fixtures_async
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.env.get_env
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Type Guards & Module Loading

::: sqlspec.utils.type_guards.is_msgspec_struct
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.type_guards.is_pydantic_model
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.type_guards.is_schema
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.module_loader.import_string
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.module_loader.import_optional
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Sync/Async Bridging, Correlation & Deprecation

::: sqlspec.utils.sync_tools.run_
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.sync_tools.async_
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.correlation.CorrelationContext
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.deprecation.deprecated
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false
