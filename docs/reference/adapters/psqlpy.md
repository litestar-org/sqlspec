# psqlpy Adapter

```python
from sqlspec.adapters.psqlpy import (
    PsqlpyConfig,
    PsqlpyConnectionParams,
    PsqlpyDriver,
    PsqlpyDriverFeatures,
    PsqlpyExceptionHandler,
    PsqlpyPoolParams,
    PsqlpyRLSStrategy,
)
```

Rust-backed asynchronous PostgreSQL adapter built on `psqlpy`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (psqlpy) Setup Guide](../../databases/postgres/psqlpy.md) for usage examples and the [PostgreSQL Data Dictionary & SQL Catalog](../data-dictionary/dialects/postgres.md).

## Configuration & Parameters

::: sqlspec.adapters.psqlpy.PsqlpyConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psqlpy.PsqlpyConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.PsqlpyPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.PsqlpyDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Tenant Strategies

Set `driver_features={"tenant_strategy": "rls"}`; `"default"` and `True` select the same strategy. Omitted or `None` pooler mode becomes `"session"`; transaction, statement, and auto modes are rejected. Both session and connection providers require a tenant or explicit bypass; standalone `create_connection()` is rejected.

The strategy captures/applies and restores/verifies owned GUCs with one management query per boundary. It never uses blanket RESET ALL. Unfinished transactions roll back before cleanup. Dirty connections require confirmed backend termination; an unconfirmed or timed-out disposal closes and resets the pool so it cannot reuse a dirty session.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.psqlpy.PsqlpyRLSStrategy
    options:
      heading_level: 3
      members: [__init__, checkout, checkin]

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.psqlpy.PsqlpyDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psqlpy.PsqlpyExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psqlpy.data_dictionary.PsqlpyDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.psqlpy.litestar.PsqlpyLitestarConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.litestar.PsqlpyStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.events.PsqlpyEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.events.PsqlpyEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.events.PsqlpyEventsBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.events.PsqlpyHybridEventsBackend
    options:
      heading_level: 4
      members: false
