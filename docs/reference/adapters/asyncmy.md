# asyncmy Adapter

```python
from sqlspec.adapters.asyncmy import (
    AsyncmyConfig,
    AsyncmyConnectionParams,
    AsyncmyDriver,
    AsyncmyDriverFeatures,
    AsyncmyExceptionHandler,
    AsyncmyPoolParams,
)
```

Asynchronous MySQL and MariaDB adapter built on `asyncmy`. Inherited execution methods are documented on [`AsyncDriverAdapterBase`](../driver/async.md). See the [MySQL & MariaDB Setup Guide](../../databases/mysql.md) for usage examples.

## Configuration & Parameters

::: sqlspec.adapters.asyncmy.AsyncmyConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.asyncmy.AsyncmyConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncmy.AsyncmyPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.asyncmy.AsyncmyDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.asyncmy.AsyncmyDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.asyncmy.AsyncmyExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.asyncmy.litestar.AsyncmyStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncmy.adk.AsyncmyADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncmy.adk.AsyncmyADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.asyncmy.events.AsyncmyEventQueueStore
    options:
      heading_level: 4
      members: false
