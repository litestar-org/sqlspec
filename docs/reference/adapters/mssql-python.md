# mssql-python Adapter

```python
from sqlspec.adapters.mssql_python import (
    MssqlPythonBulkCopyResult,
    MssqlPythonConfig,
    MssqlPythonConnectionParams,
    MssqlPythonConnectionPool,
    MssqlPythonDriver,
    MssqlPythonDriverFeatures,
    MssqlPythonExceptionHandler,
    MssqlPythonPoolParams,
)
```

Microsoft SQL Server and Azure SQL adapter built on Microsoft's official `mssql-python` driver with native Bulk Copy (`BCP`) support. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [SQL Server Setup Guide](../../databases/mssql.md) for setup and bulk copy examples.

## Configuration & Parameters

::: sqlspec.adapters.mssql_python.MssqlPythonConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.MssqlPythonConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mssql_python.MssqlPythonPoolParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.mssql_python.MssqlPythonDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.mssql_python.MssqlPythonDriver
    options:
      heading_level: 3
      members:
        - bulk_copy

::: sqlspec.adapters.mssql_python.MssqlPythonBulkCopyResult
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.MssqlPythonExceptionHandler
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.mssql_python.MssqlPythonConnectionPool
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.mssql_python.litestar.MssqlPythonStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.mssql_python.adk.MssqlPythonADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.mssql_python.adk.MssqlPythonADKMemoryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.mssql_python.events.MssqlPythonEventQueueStore
    options:
      heading_level: 4
      members: false
