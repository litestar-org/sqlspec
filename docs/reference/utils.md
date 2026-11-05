# Utilities

```python
from sqlspec.utils.arrow_helpers import (
    coerce_arrow_batches,
    coerce_arrow_table,
    encode_arrow_batch_pg_binary,
    iter_pgcopy_binary_chunks,
)
from sqlspec.utils.correlation import CorrelationContext
from sqlspec.utils.deprecation import deprecated
from sqlspec.utils.env import get_env
from sqlspec.utils.fixtures import (
    export_table_fixtures_async,
    export_table_fixtures_sync,
    load_table_fixtures_async,
    load_table_fixtures_sync,
)
from sqlspec.utils.module_loader import import_optional, import_string
from sqlspec.utils.schema import to_schema
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.sync_tools import async_, run_
from sqlspec.utils.text import camelize, snake_case
from sqlspec.utils.type_guards import (
    is_msgspec_struct,
    is_pydantic_model,
    is_schema,
)
from sqlspec.utils.uuids import nanoid, uuid4, uuid6, uuid7
```

Reusable helpers for runtime type narrowing, JSON serialization, schema model conversion, UUID/NanoID generation, Apache Arrow batch/table coercion, table fixture loading/exporting, correlation tracking, and sync/async bridging.

## Schema, Serialization & Identifiers

::: sqlspec.utils.schema.to_schema
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.serializers.to_json
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.serializers.from_json
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.uuids.uuid4
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.uuids.uuid6
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.uuids.uuid7
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.uuids.nanoid
    options:
      heading_level: 4
      separate_signature: false

::: sqlspec.utils.text.camelize
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.text.snake_case
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Arrow Coercion & Binary Helpers

::: sqlspec.utils.arrow_helpers.coerce_arrow_table
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.arrow_helpers.coerce_arrow_batches
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.arrow_helpers.encode_arrow_batch_pg_binary
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.arrow_helpers.iter_pgcopy_binary_chunks
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Table Fixtures & Environment Helpers

::: sqlspec.utils.fixtures.load_table_fixtures_sync
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.fixtures.load_table_fixtures_async
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.fixtures.export_table_fixtures_sync
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.fixtures.export_table_fixtures_async
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.env.get_env
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Type Guards & Module Loading

::: sqlspec.utils.type_guards.is_msgspec_struct
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.type_guards.is_pydantic_model
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.type_guards.is_schema
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.module_loader.import_string
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.module_loader.import_optional
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

## Sync/Async Bridging, Correlation & Deprecation

::: sqlspec.utils.sync_tools.run_
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.sync_tools.async_
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false

::: sqlspec.utils.correlation.CorrelationContext
    options:
      heading_level: 4
      members: false

::: sqlspec.utils.deprecation.deprecated
    options:
      heading_level: 4
      separate_signature: false
      show_docstring_parameters: false
