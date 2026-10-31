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

Rust-backed asynchronous PostgreSQL adapter built on `psqlpy`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [PostgreSQL (psqlpy) Setup Guide](../../databases/postgres/psqlpy.md) for usage examples and the [PostgreSQL Data Dictionary & SQL Catalog](../data-dictionary/dialects/postgres.md).

## Configuration & Parameters

::: sqlspec.adapters.psqlpy.PsqlpyConfig
    options:
      heading_level: 3

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

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKStore
    options:
      heading_level: 4

::: sqlspec.adapters.psqlpy.adk.PsqlpyADKMemoryStore
    options:
      heading_level: 4

::: sqlspec.adapters.psqlpy.events.PsqlpyEventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.psqlpy.events.PsqlpyEventQueueStore
    options:
      heading_level: 4

::: sqlspec.adapters.psqlpy.events.PsqlpyEventsBackend
    options:
      heading_level: 4

::: sqlspec.adapters.psqlpy.events.PsqlpyHybridEventsBackend
    options:
      heading_level: 4
