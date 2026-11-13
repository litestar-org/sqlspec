# Configuration

```python
from sqlspec.config import (
    AsyncDatabaseConfig,
    DatabaseConfigProtocol,
    EventsConfig,
    ExtensionConfigs,
    FastAPIConfig,
    FlaskConfig,
    LifecycleConfig,
    MigrationConfig,
    MigrationTemplates,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
    ReadReplicaGroup,
    PythonTemplateOverride,
    SanicConfig,
    SQLTemplateOverride,
    StarletteConfig,
    SyncDatabaseConfig,
    validate_migration_config_keys,
)
```

Base configuration protocols, pooled and non-pooled database config classes, lifecycle hooks, migration settings, and typed `extension_config` dictionaries. See the [Databases](../databases/index.md) and [Integrations](../integrations/index.md) guides for concrete examples.

## Base Configuration Classes

::: sqlspec.config.DatabaseConfigProtocol
    options:
      heading_level: 3
      members:
        - create_connection
        - provide_connection
        - provide_session
        - provide_read_connection
        - provide_read_session
        - provide_routing_session
        - derive_pool
        - create_pool
        - close_pool
        - provide_pool
        - set_migration_config
        - attach_observability

::: sqlspec.config.SyncDatabaseConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.config.AsyncDatabaseConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.config.NoPoolSyncConfig
    options:
      heading_level: 3
      members: [derive_pool]

::: sqlspec.config.NoPoolAsyncConfig
    options:
      heading_level: 3
      members: [derive_pool]

## Lifecycle & Migration Settings

::: sqlspec.config.LifecycleConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.MigrationConfig
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.MigrationTemplates
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.PythonTemplateOverride
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.SQLTemplateOverride
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.validate_migration_config_keys
    options:
      heading_level: 4

## Extension Configuration Schemas

Top-level `extension_config` mapping passed to any database configuration class. [`LitestarConfig`](extensions/litestar.md) and [`ADKConfig`](extensions/adk.md) are documented on their respective extension pages.

::: sqlspec.config.ExtensionConfigs
    options:
      heading_level: 3
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.EventsConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.FastAPIConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.StarletteConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.FlaskConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.SanicConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.OpenTelemetryConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

::: sqlspec.config.PrometheusConfig
    options:
      heading_level: 4
      separate_signature: false
      show_signature_annotations: true

## Read Replica Topology

::: sqlspec.config.ReadReplicaGroup
    options:
      heading_level: 4
      members: false
