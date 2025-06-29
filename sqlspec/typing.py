# pyright: ignore[reportAttributeAccessIssue]
from collections.abc import Mapping
from functools import lru_cache
from typing import TYPE_CHECKING, Annotated, Any, Union

from typing_extensions import TypeAlias, TypeVar

from sqlspec._typing import (
    AIOSQL_INSTALLED,
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    OBSTORE_INSTALLED,
    OPENTELEMETRY_INSTALLED,
    PGVECTOR_INSTALLED,
    PROMETHEUS_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
    UNSET,
    AiosqlAsyncProtocol,  # pyright: ignore[reportAttributeAccessIssue]
    AiosqlParamType,  # pyright: ignore[reportAttributeAccessIssue]
    AiosqlProtocol,  # pyright: ignore[reportAttributeAccessIssue]
    AiosqlSQLOperationType,  # pyright: ignore[reportAttributeAccessIssue]
    AiosqlSyncProtocol,  # pyright: ignore[reportAttributeAccessIssue]
    ArrowRecordBatch,
    ArrowTable,
    BaseModel,
    Counter,  # pyright: ignore[reportAttributeAccessIssue]
    DataclassProtocol,
    DTOData,
    Empty,
    EmptyType,
    Gauge,  # pyright: ignore[reportAttributeAccessIssue]
    Histogram,  # pyright: ignore[reportAttributeAccessIssue]
    Span,  # pyright: ignore[reportAttributeAccessIssue]
    Status,  # pyright: ignore[reportAttributeAccessIssue]
    StatusCode,  # pyright: ignore[reportAttributeAccessIssue]
    Struct,
    Tracer,  # pyright: ignore[reportAttributeAccessIssue]
    TypeAdapter,
    UnsetType,
    aiosql,
    convert,  # pyright: ignore[reportAttributeAccessIssue]
    trace,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


PYDANTIC_USE_FAILFAST = False  # leave permanently disabled for now


T = TypeVar("T")
ConnectionT = TypeVar("ConnectionT")
"""Type variable for connection types.

:class:`~sqlspec.typing.ConnectionT`
"""
PoolT = TypeVar("PoolT")
"""Type variable for pool types.

:class:`~sqlspec.typing.PoolT`
"""
PoolT_co = TypeVar("PoolT_co", covariant=True)
"""Type variable for covariant pool types.

:class:`~sqlspec.typing.PoolT_co`
"""
ModelT = TypeVar("ModelT", bound="Union[dict[str, Any], Struct, BaseModel, DataclassProtocol]")
"""Type variable for model types.

:class:`dict[str, Any]` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol`
"""


DictRow: TypeAlias = "dict[str, Any]"
"""Type variable for DictRow types."""
TupleRow: TypeAlias = "tuple[Any, ...]"
"""Type variable for TupleRow types."""
RowT = TypeVar("RowT", default=dict[str, Any])

SupportedSchemaModel: TypeAlias = "Union[Struct, BaseModel, DataclassProtocol]"
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol`
"""
StatementParameters: TypeAlias = "Union[Any, dict[str, Any], list[Any], tuple[Any, ...], None]"
"""Type alias for statement parameters.

Represents:
- :type:`dict[str, Any]`
- :type:`list[Any]`
- :type:`tuple[Any, ...]`
- :type:`None`
"""
# Backward compatibility alias
SQLParameterType: TypeAlias = StatementParameters
ModelDTOT = TypeVar("ModelDTOT", bound="SupportedSchemaModel")
"""Type variable for model DTOs.

:class:`msgspec.Struct`|:class:`pydantic.BaseModel`
"""
PydanticOrMsgspecT = SupportedSchemaModel
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` or :class:`pydantic.BaseModel`
"""
ModelDict: TypeAlias = "Union[dict[str, Any], SupportedSchemaModel, DTOData[SupportedSchemaModel]]"
"""Type alias for model dictionaries.

Represents:
- :type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` |  :class:`pydantic.BaseModel`
"""
ModelDictList: TypeAlias = "Sequence[Union[dict[str, Any], SupportedSchemaModel]]"
"""Type alias for model dictionary lists.

A list or sequence of any of the following:
- :type:`Sequence`[:type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`]

"""
BulkModelDict: TypeAlias = (
    "Union[Sequence[Union[dict[str, Any], SupportedSchemaModel]], DTOData[list[SupportedSchemaModel]]]"
)
"""Type alias for bulk model dictionaries.

Represents:
- :type:`Sequence`[:type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`]
- :class:`DTOData`[:type:`list[ModelT]`]
"""


@lru_cache(typed=True)
def get_type_adapter(f: "type[T]") -> "TypeAdapter[T]":
    """Caches and returns a pydantic type adapter.

    Args:
        f: Type to create a type adapter for.

    Returns:
        :class:`pydantic.TypeAdapter`[:class:`typing.TypeVar`[T]]
    """
    if PYDANTIC_USE_FAILFAST:
        return TypeAdapter(Annotated[f, FailFast()])
    return TypeAdapter(f)


def MixinOf(base: type[T]) -> type[T]:  # noqa: N802
    """Useful function to make mixins with baseclass type hint

    ```
    class StorageMixin(MixinOf(DriverProtocol)): ...
    ```
    """
    if TYPE_CHECKING:
        return base
    return type("<MixinOf>", (base,), {})


__all__ = (
    "AIOSQL_INSTALLED",
    "FSSPEC_INSTALLED",
    "LITESTAR_INSTALLED",
    "MSGSPEC_INSTALLED",
    "OBSTORE_INSTALLED",
    "OPENTELEMETRY_INSTALLED",
    "PGVECTOR_INSTALLED",
    "PROMETHEUS_INSTALLED",
    "PYARROW_INSTALLED",
    "PYDANTIC_INSTALLED",
    "PYDANTIC_USE_FAILFAST",
    "UNSET",
    "AiosqlAsyncProtocol",
    "AiosqlParamType",
    "AiosqlProtocol",
    "AiosqlSQLOperationType",
    "AiosqlSyncProtocol",
    "ArrowRecordBatch",
    "ArrowTable",
    "BaseModel",
    "BulkModelDict",
    "ConnectionT",
    "Counter",
    "DTOData",
    "DataclassProtocol",
    "DictRow",
    "Empty",
    "EmptyType",
    "FailFast",
    "Gauge",
    "Histogram",
    "Mapping",
    "MixinOf",
    "ModelDTOT",
    "ModelDict",
    "ModelDict",
    "ModelDictList",
    "ModelDictList",
    "ModelT",
    "PoolT",
    "PoolT_co",
    "PydanticOrMsgspecT",
    "RowT",
    "SQLParameterType",
    "Span",
    "StatementParameters",
    "Status",
    "StatusCode",
    "Struct",
    "SupportedSchemaModel",
    "Tracer",
    "TupleRow",
    "TypeAdapter",
    "UnsetType",
    "aiosql",
    "convert",
    "get_type_adapter",
    "trace",
)

if TYPE_CHECKING:
    if not PYDANTIC_INSTALLED:
        from sqlspec._typing import BaseModel, FailFast, TypeAdapter
    else:
        from pydantic import BaseModel, FailFast, TypeAdapter  # noqa: TC004

    if not MSGSPEC_INSTALLED:
        from sqlspec._typing import UNSET, Struct, UnsetType, convert
    else:
        from msgspec import UNSET, Struct, UnsetType, convert  # noqa: TC004

    if not PYARROW_INSTALLED:
        from sqlspec._typing import ArrowRecordBatch, ArrowTable
    else:
        from pyarrow import RecordBatch as ArrowRecordBatch  # noqa: TC004
        from pyarrow import Table as ArrowTable  # noqa: TC004
    if not LITESTAR_INSTALLED:
        from sqlspec._typing import DTOData
    else:
        from litestar.dto import DTOData  # noqa: TC004
    if not OPENTELEMETRY_INSTALLED:
        from sqlspec._typing import Span, Status, StatusCode, Tracer, trace  # noqa: TC004  # pyright: ignore
    else:
        from opentelemetry.trace import (  # pyright: ignore[reportMissingImports] # noqa: TC004
            Span,
            Status,
            StatusCode,
            Tracer,
        )
    if not PROMETHEUS_INSTALLED:
        from sqlspec._typing import Counter, Gauge, Histogram  # pyright: ignore
    else:
        from prometheus_client import Counter, Gauge, Histogram  # noqa: TC004 # pyright: ignore # noqa: TC004

    if not AIOSQL_INSTALLED:
        from sqlspec._typing import (
            AiosqlAsyncProtocol,  # pyright: ignore[reportAttributeAccessIssue]
            AiosqlParamType,  # pyright: ignore[reportAttributeAccessIssue]
            AiosqlProtocol,  # pyright: ignore[reportAttributeAccessIssue]
            AiosqlSQLOperationType,  # pyright: ignore[reportAttributeAccessIssue]
            AiosqlSyncProtocol,  # pyright: ignore[reportAttributeAccessIssue]
            aiosql,
        )
    else:
        import aiosql  # noqa: TC004 # pyright: ignore
        from aiosql.types import (  # noqa: TC004 # pyright: ignore[reportMissingImports]
            AsyncDriverAdapterProtocol as AiosqlAsyncProtocol,
        )
        from aiosql.types import (  # noqa: TC004 # pyright: ignore[reportMissingImports]
            DriverAdapterProtocol as AiosqlProtocol,
        )
        from aiosql.types import ParamType as AiosqlParamType  # noqa: TC004 # pyright: ignore[reportMissingImports]
        from aiosql.types import (
            SQLOperationType as AiosqlSQLOperationType,  # noqa: TC004 # pyright: ignore[reportMissingImports]
        )
        from aiosql.types import (  # noqa: TC004 # pyright: ignore[reportMissingImports]
            SyncDriverAdapterProtocol as AiosqlSyncProtocol,
        )
