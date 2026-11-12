# teradata Adapter

```python
from sqlspec.adapters.teradata import (
    TeradataConfig,
    TeradataConnectionParams,
    TeradataConnectionPool,
    TeradataDriver,
    TeradataDriverFeatures,
    TeradataExceptionHandler,
    TeradataPoolParams,
    TeradataSyncDataDictionary,
    TeradataSyncMigrationTracker,
)
```

Synchronous Teradata Vantage adapter using `teradatasql`. See the [setup guide](../../databases/teradata.md) and [catalog reference](../data-dictionary/dialects/teradata.md).

**Configuration & Parameters**

::: sqlspec.adapters.teradata.TeradataConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.teradata.TeradataConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.teradata.TeradataPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.teradata.TeradataDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

**Driver, Pool & Data Dictionary**

::: sqlspec.adapters.teradata.TeradataDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.teradata.TeradataExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.teradata.TeradataConnectionPool
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.teradata.data_dictionary.TeradataSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.teradata.migrations.TeradataSyncMigrationTracker
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.teradata.litestar.TeradataStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.teradata.adk.TeradataADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.teradata.adk.TeradataADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.teradata.events.TeradataEventQueueStore
    options:
      heading_level: 4
      members: false

**Driver Option Types**

::: sqlspec.adapters.teradata.TeradataLogMech
    options:
      show_root_toc_entry: false

::: sqlspec.adapters.teradata.TeradataSSLMode
    options:
      show_root_toc_entry: false

::: sqlspec.adapters.teradata.TeradataTransactionMode
    options:
      show_root_toc_entry: false
