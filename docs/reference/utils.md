# Utilities

```python
from sqlspec.utils import (
    correlation,
    deprecation,
    env,
    fixtures,
    module_loader,
    schema,
    serializers,
    sync_tools,
    text,
    type_guards,
    uuids,
)
```

Reusable helpers for runtime type narrowing, JSON serialization, schema model conversion, UUID/NanoID generation, table fixture loading/exporting, correlation tracking, and sync/async bridging.

## Schema, Serialization & Identifiers

::: sqlspec.utils.schema.to_schema
    options:
      heading_level: 4

::: sqlspec.utils.serializers.to_json
    options:
      heading_level: 4

::: sqlspec.utils.serializers.from_json
    options:
      heading_level: 4

::: sqlspec.utils.uuids.uuid4
    options:
      heading_level: 4

::: sqlspec.utils.uuids.uuid7
    options:
      heading_level: 4

::: sqlspec.utils.uuids.nanoid
    options:
      heading_level: 4

::: sqlspec.utils.text.camelize
    options:
      heading_level: 4

::: sqlspec.utils.text.snake_case
    options:
      heading_level: 4

## Table Fixtures & Environment Helpers

::: sqlspec.utils.fixtures.load_table_fixtures_sync
    options:
      heading_level: 4

::: sqlspec.utils.fixtures.load_table_fixtures_async
    options:
      heading_level: 4

::: sqlspec.utils.fixtures.export_table_fixtures_sync
    options:
      heading_level: 4

::: sqlspec.utils.fixtures.export_table_fixtures_async
    options:
      heading_level: 4

::: sqlspec.utils.env.get_env
    options:
      heading_level: 4

## Type Guards & Module Loading

::: sqlspec.utils.type_guards.is_msgspec_struct
    options:
      heading_level: 4

::: sqlspec.utils.type_guards.is_pydantic_model
    options:
      heading_level: 4

::: sqlspec.utils.type_guards.is_schema
    options:
      heading_level: 4

::: sqlspec.utils.module_loader.import_string
    options:
      heading_level: 4

::: sqlspec.utils.module_loader.import_optional
    options:
      heading_level: 4

## Sync/Async Bridging, Correlation & Deprecation

::: sqlspec.utils.sync_tools.run_
    options:
      heading_level: 4

::: sqlspec.utils.sync_tools.async_
    options:
      heading_level: 4

::: sqlspec.utils.correlation.CorrelationContext
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.deprecation.deprecated
    options:
      heading_level: 4
