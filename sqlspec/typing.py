# pyright: ignore[reportAttributeAccessIssue]
from collections.abc import Iterator
from functools import lru_cache
from typing import Annotated, Any, Literal, Protocol, TypeAlias, _TypedDict  # pyright: ignore

from typing_extensions import TypeVar

from sqlspec._typing import (
    AIOSQL_INSTALLED,
    ALLOYDB_CONNECTOR_INSTALLED,
    ATTRS_INSTALLED,
    CATTRS_INSTALLED,
    CLOUD_SQL_CONNECTOR_INSTALLED,
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
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
    AiosqlAsyncProtocol,
    AiosqlParamType,
    AiosqlSQLOperationType,
    AiosqlSyncProtocol,
    ArrowRecordBatch,
    ArrowRecordBatchReader,
    ArrowRecordBatchReaderProtocol,
    ArrowSchema,
    ArrowSchemaProtocol,
    ArrowTable,
    AttrsInstance,
    AttrsInstanceStub,
    BaseModel,
    BaseModelStub,
    Counter,
    DataclassProtocol,
    DTOData,
    Empty,
    EmptyEnum,
    EmptyType,
    FailFast,
    Gauge,
    Histogram,
    NumpyArray,
    PandasDataFrame,
    PolarsDataFrame,
    Span,
    Status,
    StatusCode,
    Struct,
    StructStub,
    Tracer,
    TypeAdapter,
    UnsetType,
    aiosql,
    attrs_asdict,
    attrs_define,
    attrs_field,
    attrs_fields,
    attrs_has,
    cattrs_structure,
    cattrs_unstructure,
    convert,
    module_available,
    trace,
)


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
    DictLike | StructStub | BaseModelStub | DataclassProtocol | AttrsInstanceStub | _TypedDict
)
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol` | :class:`AttrsInstance`
"""
StatementParameters: TypeAlias = "Any | dict[str, Any] | list[Any] | tuple[Any, ...] | None"
"""Type alias for statement parameters.

Represents:
- :type:`dict[str, Any]`
- :type:`list[Any]`
- :type:`tuple[Any, ...]`
- :type:`None`
"""
ArrowReturnFormat: TypeAlias = Literal["table", "reader", "batch", "batches"]
"""Type alias for Apache Arrow return format options.

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


__all__ = (
    "AIOSQL_INSTALLED",
    "ALLOYDB_CONNECTOR_INSTALLED",
    "ATTRS_INSTALLED",
    "CATTRS_INSTALLED",
    "CLOUD_SQL_CONNECTOR_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
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
    "AiosqlAsyncProtocol",
    "AiosqlParamType",
    "AiosqlSQLOperationType",
    "AiosqlSyncProtocol",
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
    "aiosql",
    "attrs_asdict",
    "attrs_define",
    "attrs_field",
    "attrs_fields",
    "attrs_has",
    "cattrs_structure",
    "cattrs_unstructure",
    "convert",
    "get_type_adapter",
    "module_available",
    "trace",
)
