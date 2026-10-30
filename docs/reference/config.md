# Configuration

```python
from sqlspec.config import (
    ADKConfig,
    AsyncDatabaseConfig,
    DatabaseConfigProtocol,
    EventsConfig,
    ExtensionConfigs,
    FastAPIConfig,
    FlaskConfig,
    LifecycleConfig,
    LitestarConfig,
    MigrationConfig,
    NoPoolAsyncConfig,
    NoPoolSyncConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
    SanicConfig,
    StarletteConfig,
    SyncDatabaseConfig,
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
        - create_pool
        - close_pool
        - provide_pool
        - set_migration_config
        - attach_observability

::: sqlspec.config.SyncDatabaseConfig
    options:
      heading_level: 3
      members:
        - provide_connection
        - provide_session
        - provide_pool
        - create_connection
        - create_pool
        - close_pool

::: sqlspec.config.AsyncDatabaseConfig
    options:
      heading_level: 3
      members:
        - provide_connection
        - provide_session
        - provide_pool
        - create_connection
        - create_pool
        - close_pool

::: sqlspec.config.NoPoolSyncConfig
    options:
      heading_level: 3
      members:
        - provide_connection
        - provide_session
        - create_connection

::: sqlspec.config.NoPoolAsyncConfig
    options:
      heading_level: 3
      members:
        - provide_connection
        - provide_session
        - create_connection

## Lifecycle & Migration Settings

::: sqlspec.config.LifecycleConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.config.MigrationConfig
    options:
      heading_level: 3
      members: false

::: sqlspec.config.MigrationTemplates
    options:
      heading_level: 4
      members: false

::: sqlspec.config.PythonTemplateOverride
    options:
      heading_level: 4
      members: false

::: sqlspec.config.SQLTemplateOverride
    options:
      heading_level: 4
      members: false

::: sqlspec.config.validate_migration_config_keys
    options:
      heading_level: 4

## Extension Configuration Schemas

::: sqlspec.config.ExtensionConfigs
    options:
      heading_level: 3
      members: false

::: sqlspec.config.LitestarConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.ADKConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.EventsConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.FastAPIConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.StarletteConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.FlaskConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.SanicConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.OpenTelemetryConfig
    options:
      heading_level: 4
      members: false

::: sqlspec.config.PrometheusConfig
    options:
      heading_level: 4
      members: false
