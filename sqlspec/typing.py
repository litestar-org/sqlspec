"""Public typing helpers and optional dependency aliases.

This module is the supported import surface for SQLSpec typing utilities.
The implementation lives in :mod:`sqlspec._typing`, which remains private so
adapter and optional dependency shims can evolve without expanding the public
API.
"""

from collections.abc import Iterator, Mapping
from functools import lru_cache
from typing import Annotated, Any, Literal, Protocol, TypeAlias

from typing_extensions import TypeVar

from sqlspec import _typing
from sqlspec._typing import (
    ALLOYDB_CONNECTOR_INSTALLED,
    ATTRS_INSTALLED,
    CATTRS_INSTALLED,
    CLOUD_SQL_CONNECTOR_INSTALLED,
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    NANOID_INSTALLED,
    NUMPY_INSTALLED,
    OBSTORE_INSTALLED,
    OPENTELEMETRY_INSTALLED,
    ORJSON_INSTALLED,
    PANDAS_INSTALLED,
    PGVECTOR_INSTALLED,
    POLARS_INSTALLED,
    PROMETHEUS_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
    UNSET,
    UUID_UTILS_INSTALLED,
    AttrsInstanceStub,
    BaseModelStub,
    DataclassProtocol,
    Empty,
    EmptyEnum,
    EmptyType,
    MsgspecValidationError,
    Struct,
    StructStub,
    UnsetType,
    convert,
    import_optional,
    import_optional_attr,
    module_available,
    msgspec_fields,
)

__all__ = (
    "ALLOYDB_CONNECTOR_INSTALLED",
    "ATTRS_INSTALLED",
    "CATTRS_INSTALLED",
    "CLOUD_SQL_CONNECTOR_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "NANOID_INSTALLED",
    "NUMPY_INSTALLED",
    "OBSTORE_INSTALLED",
    "OPENTELEMETRY_INSTALLED",
    "ORJSON_INSTALLED",
    "PANDAS_INSTALLED",
    "PGVECTOR_INSTALLED",
    "POLARS_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "PYDANTIC_USE_FAILFAST",
    "UNSET",
    "UUID_UTILS_INSTALLED",
    "ArrowRecordBatch",
    "ArrowRecordBatchReader",
    "ArrowRecordBatchReaderProtocol",
    "ArrowReturnFormat",
    "ArrowSchema",
    "ArrowSchemaProtocol",
    "ArrowTable",
    "AttrsInstance",
    "BaseModel",
    "ConnectionT",
    "Counter",
    "DTOData",
    "DataclassProtocol",
    "DictLike",
    "Empty",
    "EmptyEnum",
    "EmptyType",
    "FailFast",
    "Gauge",
    "Histogram",
    "MsgspecValidationError",
    "NumpyArray",
    "PandasDataFrame",
    "PolarsDataFrame",
    "PoolT",
    "SchemaT",
    "Span",
    "StatementParameters",
    "Status",
    "StatusCode",
    "Struct",
    "SupportedSchemaModel",
    "Tracer",
    "TypeAdapter",
    "UnsetType",
    "attrs_asdict",
    "attrs_define",
    "attrs_field",
    "attrs_fields",
    "attrs_has",
    "cattrs_structure",
    "cattrs_unstructure",
    "convert",
    "get_type_adapter",
    "import_optional",
    "import_optional_attr",
    "module_available",
    "msgspec_fields",
    "trace",
)


def __getattr__(name: str) -> Any:
    """Resolve lazy typing exports on demand."""

    if name not in __all__:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)

    try:
        value = getattr(_typing, name)
    except AttributeError:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg) from None

    globals()[name] = value
    return value


def __dir__() -> "list[str]":
    """Expose the public surface for autocomplete and ``dir()``."""

    return sorted(set(globals()) | set(__all__))


class DictLike(Protocol):
    """A protocol for objects that behave like a dictionary for reading."""

    def __getitem__(self, key: str) -> Any: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...


PYDANTIC_USE_FAILFAST = False


T = TypeVar("T")
ConnectionT = TypeVar("ConnectionT")
"""Type variable for connection types.

:class:`~sqlspec.typing.ConnectionT`
"""
PoolT = TypeVar("PoolT")
"""Type variable for pool types.

:class:`~sqlspec.typing.PoolT`
"""
SchemaT = TypeVar("SchemaT", default=dict[str, Any])
"""Type variable for schema types (models, TypedDict, dataclasses, etc.).

Unbounded TypeVar for use with schema_type parameter in driver methods.
Supports all schema types including TypedDict which cannot be bounded to a class hierarchy.
"""


SupportedSchemaModel: TypeAlias = (
    DictLike | StructStub | BaseModelStub | DataclassProtocol | AttrsInstanceStub | Mapping[str, Any]
)
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol` | :class:`AttrsInstance`
"""
StatementParameters: TypeAlias = "dict[str, object] | list[object] | tuple[object, ...] | object | None"
"""
Type alias for statement parameters.

Represents:
    - :type:`dict[str, object]`
    - :type:`list[object]`
    - :type:`tuple[object, ...]`
    - :type:`object`
    - :type:`None`
"""
ArrowReturnFormat: TypeAlias = Literal["table", "reader", "batch", "batches"]
"""
Type alias for Apache Arrow return format options.

Represents:
    - :literal:`"table"` - Return PyArrow Table
    - :literal:`"reader"` - Return PyArrow RecordBatchReader
    - :literal:`"batch"` - Return single PyArrow RecordBatch
    - :literal:`"batches"` - Return list of PyArrow RecordBatches
"""


@lru_cache(typed=True)
def get_type_adapter(f: "type[T]") -> Any:
    """Caches and returns a pydantic type adapter.

    Args:
        f: Type to create a type adapter for.

    Returns:
        :class:`pydantic.TypeAdapter`[:class:`typing.TypeVar`[T]]
    """
    if PYDANTIC_USE_FAILFAST:
        return TypeAdapter(Annotated[f, FailFast()])
    return TypeAdapter(f)
