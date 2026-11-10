# oracledb Adapter

```python
from sqlspec.adapters.oracledb import (
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleAsyncExceptionHandler,
    OracleConnectionParams,
    OracleDriverFeatures,
    OracleEndpoint,
    OraclePoolParams,
    OracleSessionContextConfig,
    OracleSyncConfig,
    OracleSyncDriver,
    OracleSyncExceptionHandler,
    OracleTopologyConfig,
    OracleSyncIdentifierStrategy,
    OracleAsyncIdentifierStrategy,
    OracleSyncVPDStrategy,
    OracleAsyncVPDStrategy,
)
```

Synchronous and asynchronous Oracle Database adapters built on `python-oracledb` (Thin and Thick modes) with `VECTOR`, `JSON`, `LOB`, and Advanced Queuing support. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md) and [`AsyncDriverAdapterBase`](../driver/async.md). See the [Oracle Database Setup Guide](../../databases/oracledb.md) for wallet, pool, and vector configuration, and the [Oracle Data Dictionary & SQL Catalog](../data-dictionary/dialects/oracle.md).

## Configuration & Parameters

::: sqlspec.adapters.oracledb.OracleSyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleAsyncConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.OraclePoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.OracleDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.OracleTopologyConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.OracleEndpoint
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.OracleSessionContextConfig
    options:
      heading_level: 4
      members: false

## Tenant Strategies

Set `tenant_strategy` to `"default"`, `"identifier"`, or `True` for client-identifier metadata. VPD requires an explicit mode-specific VPD strategy with trusted `set_procedure`, `clear_procedure`, `namespace`, and `attribute` names, plus an administrator-provisioned package and database policy. Confirm edition and licensing with your DBA. Identity alone does not enforce row access.

The strategy composes existing session hooks, preserves NLS/edition/time-zone behavior, and verifies the server identifier and VPD context. Oracle identifiers must fit in 64 UTF-8 bytes. Cleanup failure physically drops pooled connections. Both providers require an explicit tenant or bypass; standalone `create_connection()` is rejected in tenant mode. Hook calls and server verification can require multiple round trips.

Missing tenant identity raises `TenantContextError` before acquisition. Failed cleanup raises `TenantCleanupError` when no body exception is pending; otherwise the body error keeps its identity and receives a cleanup note. See [Multi-Tenancy](../../learn/recipes/multi-tenancy.md) and [Tenant Operations](../../learn/recipes/multi-tenancy-operations.md).

::: sqlspec.adapters.oracledb.OracleSyncIdentifierStrategy
    options:
      heading_level: 3
      members: [__init__, checkout, checkin]

::: sqlspec.adapters.oracledb.OracleAsyncIdentifierStrategy
    options:
      heading_level: 3
      members: [__init__, checkout, checkin]

::: sqlspec.adapters.oracledb.OracleSyncVPDStrategy
    options:
      heading_level: 3
      members: [__init__, checkout, checkin]

::: sqlspec.adapters.oracledb.OracleAsyncVPDStrategy
    options:
      heading_level: 3
      members: [__init__, checkout, checkin]

## Driver, Pool & Data Dictionary

::: sqlspec.adapters.oracledb.OracleSyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleAsyncDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleSyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.OracleAsyncExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.data_dictionary.OracledbSyncDataDictionary
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.oracledb.data_dictionary.OracledbAsyncDataDictionary
    options:
      heading_level: 3
      members: false

## Extension Stores & Configs

::: sqlspec.adapters.oracledb.litestar.OracleLitestarConfig
    options:
      heading_level: 4
      members: false
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.litestar.OracleSyncStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.litestar.OracleAsyncStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.adk.OracleADKConfig
    options:
      heading_level: 4
      members: false
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.adk.OracleSyncADKStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.adk.OracleAsyncADKStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.adk.OracleSyncADKMemoryStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.adk.OracleAsyncADKMemoryStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.events.OracleEventsConfig
    options:
      heading_level: 4
      members: false
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.oracledb.events.OracleSyncEventQueueStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.events.OracleAsyncEventQueueStore
    options:
      heading_level: 4
      members: false
      show_docstring_parameters: false

::: sqlspec.adapters.oracledb.events.OracleSyncAQEventBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleAsyncAQEventBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleSyncTxEventQEventBackend
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.oracledb.events.OracleAsyncTxEventQEventBackend
    options:
      heading_level: 4
      members: false
