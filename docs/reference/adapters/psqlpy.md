# psqlpy Adapter

```python
from sqlspec.adapters.psqlpy import (
    PsqlpyConfig,
    PsqlpyConnectionParams,
    PsqlpyDriver,
    PsqlpyDriverFeatures,
    PsqlpyExceptionHandler,
    PsqlpyPoolParams,
)
```

Rust-backed asynchronous PostgreSQL adapter built on `psqlpy`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (psqlpy) Setup Guide](../../databases/postgres/psqlpy.md) for usage examples.

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

## Driver & Pool

::: sqlspec.adapters.psqlpy.PsqlpyDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.psqlpy.PsqlpyExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.psqlpy.litestar.PsqlpyStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.events.PsqlpyEventQueueStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.psqlpy.events.PsqlpyEventsBackend
    options:
      heading_level: 4
      members: false
