# Typing & Schema Aliases

```python
from sqlspec.typing import (
    UNSET,
    ArrowReturnFormat,
    ConnectionT,
    DataclassProtocol,
    DictLike,
    Empty,
    EmptyType,
    PoolT,
    SchemaT,
    StatementParameters,
    SupportedSchemaModel,
    UnsetType,
    get_type_adapter,
)
```

Public type variables, schema model union aliases, Apache Arrow return format literals, and sentinel types exported by `sqlspec.typing`. See [Schema Mapping](../learn/schema-mapping.md) for usage examples.

## Core Type Variables & Aliases

::: sqlspec.typing.ConnectionT
    options:
      heading_level: 3

::: sqlspec.typing.PoolT
    options:
      heading_level: 3

::: sqlspec.typing.SchemaT
    options:
      heading_level: 3

::: sqlspec.typing.SupportedSchemaModel
    options:
      heading_level: 3

::: sqlspec.typing.StatementParameters
    options:
      heading_level: 3

::: sqlspec.typing.ArrowReturnFormat
    options:
      heading_level: 3

## Schema Protocols & Adapter Helpers

::: sqlspec.typing.DictLike
    options:
      heading_level: 3

::: sqlspec.typing.DataclassProtocol
    options:
      heading_level: 3

::: sqlspec.typing.get_type_adapter
    options:
      heading_level: 3

## Sentinel Types & Optional Dependency Flags

::: sqlspec.typing.UnsetType
    options:
      heading_level: 4

::: sqlspec.typing.EmptyEnum
    options:
      heading_level: 4

| Constant | Description |
| --- | --- |
| `UNSET` | Singleton sentinel indicating an omitted optional parameter. |
| `Empty` | Legacy sentinel alias for omitted parameters. |
| `MSGSPEC_INSTALLED` | `True` when `msgspec` is available at runtime. |
| `PYDANTIC_INSTALLED` | `True` when `pydantic` is available at runtime. |
| `ATTRS_INSTALLED` | `True` when `attrs` is available at runtime. |
| `CATTRS_INSTALLED` | `True` when `cattrs` is available at runtime. |
| `PYARROW_INSTALLED` | `True` when `pyarrow` is available at runtime. |
| `POLARS_INSTALLED` | `True` when `polars` is available at runtime. |
| `PANDAS_INSTALLED` | `True` when `pandas` is available at runtime. |
| `NUMPY_INSTALLED` | `True` when `numpy` is available at runtime. |
| `LITESTAR_INSTALLED` | `True` when `litestar` is available at runtime. |
| `OPENTELEMETRY_INSTALLED` | `True` when `opentelemetry-api` is available at runtime. |
| `PROMETHEUS_INSTALLED` | `True` when `prometheus-client` is available at runtime. |
| `FSSPEC_INSTALLED` | `True` when `fsspec` is available at runtime. |
| `OBSTORE_INSTALLED` | `True` when `obstore` is available at runtime. |
