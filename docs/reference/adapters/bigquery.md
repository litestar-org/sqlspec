# bigquery Adapter

```python
from sqlspec.adapters.bigquery import (
    BigQueryConfig,
    BigQueryConnectionParams,
    BigQueryDriver,
    BigQueryDriverFeatures,
    BigQueryExceptionHandler,
)
```

Google Cloud BigQuery analytical adapter built on `google-cloud-bigquery` and `google-cloud-bigquery-storage` for high-throughput Arrow result streaming. Inherited execution methods are documented on [`SyncDriverAdapterBase`](../driver/sync.md). See the [BigQuery Setup Guide](../../databases/bigquery.md) for authentication and Storage Read API configuration.

## Configuration & Parameters

::: sqlspec.adapters.bigquery.BigQueryConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.bigquery.BigQueryConnectionParams
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.adapters.bigquery.BigQueryDriverFeatures
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

## Driver & Pool

::: sqlspec.adapters.bigquery.BigQueryDriver
    options:
      heading_level: 3
      members: false

::: sqlspec.adapters.bigquery.BigQueryExceptionHandler
    options:
      heading_level: 3
      members: false

## Extension Stores

::: sqlspec.adapters.bigquery.litestar.BigQueryStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.bigquery.adk.BigQueryADKStore
    options:
      heading_level: 4
      members: false

::: sqlspec.adapters.bigquery.events.BigQueryEventQueueStore
    options:
      heading_level: 4
      members: false
