# pyright: ignore[reportAttributeAccessIssue]
from collections.abc import Iterator, Mapping
from functools import lru_cache
from typing import TYPE_CHECKING, Annotated, Any, Protocol, Union

from typing_extensions import TypeAlias, TypeVar

from sqlspec._typing import (
    AIOSQL_INSTALLED,
    ATTRS_INSTALLED,
    CATTRS_INSTALLED,
    FSSPEC_INSTALLED,
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    OBSTORE_INSTALLED,
    OPENTELEMETRY_INSTALLED,
    PGVECTOR_INSTALLED,
    PROMETHEUS_INSTALLED,
    PYARROW_INSTALLED,
    PYDANTIC_INSTALLED,
    DataclassProtocol,
    Empty,
    EmptyEnum,
    EmptyType,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    try:
        from attrs import AttrsInstance
        from attrs import asdict as attrs_asdict
        from attrs import define as attrs_define
        from attrs import field as attrs_field
        from attrs import fields as attrs_fields
        from attrs import has as attrs_has
    except ImportError:
        from sqlspec._typing import AttrsInstance, attrs_asdict, attrs_define, attrs_field, attrs_fields, attrs_has

    try:
        from pydantic import BaseModel, FailFast, TypeAdapter
    except ImportError:
        from sqlspec._typing import BaseModel, FailFast, TypeAdapter

    try:
        from msgspec import UNSET, Struct, UnsetType, convert
    except ImportError:
        from sqlspec._typing import UNSET, Struct, UnsetType, convert

    try:
        from pyarrow import RecordBatch as ArrowRecordBatch
        from pyarrow import Table as ArrowTable
    except ImportError:
        from sqlspec._typing import ArrowRecordBatch, ArrowTable

    try:
        from litestar.dto import DTOData
    except ImportError:
        from sqlspec._typing import DTOData

    try:
        from opentelemetry import trace
        from opentelemetry.trace import Span, Status, StatusCode, Tracer
    except ImportError:
        from sqlspec._typing import Span, Status, StatusCode, Tracer, trace

    try:
        from prometheus_client import Counter, Gauge, Histogram
    except ImportError:
        from sqlspec._typing import Counter, Gauge, Histogram

    try:
        import aiosql
        from aiosql.types import AsyncDriverAdapterProtocol as AiosqlAsyncProtocol
        from aiosql.types import DriverAdapterProtocol as AiosqlProtocol
        from aiosql.types import ParamType as AiosqlParamType
        from aiosql.types import SQLOperationType as AiosqlSQLOperationType
        from aiosql.types import SyncDriverAdapterProtocol as AiosqlSyncProtocol
    except ImportError:
        from sqlspec._typing import (
            AiosqlAsyncProtocol,
            AiosqlParamType,
            AiosqlProtocol,
            AiosqlSQLOperationType,
            AiosqlSyncProtocol,
            aiosql,
        )
    try:
        from cattrs import structure as cattrs_structure
        from cattrs import unstructure as cattrs_unstructure
    except ImportError:
        from sqlspec._typing import cattrs_structure, cattrs_unstructure
else:
    if not PYDANTIC_INSTALLED:
        from sqlspec._typing import BaseModel, FailFast, TypeAdapter
    else:
        from pydantic import BaseModel, FailFast, TypeAdapter

    if not MSGSPEC_INSTALLED:
        from sqlspec._typing import UNSET, Struct, UnsetType, convert
    else:
        from msgspec import UNSET, Struct, UnsetType, convert

    if not PYARROW_INSTALLED:
        from sqlspec._typing import ArrowRecordBatch, ArrowTable
    else:
        from pyarrow import RecordBatch as ArrowRecordBatch
        from pyarrow import Table as ArrowTable

    if not LITESTAR_INSTALLED:
        from sqlspec._typing import DTOData
    else:
        from litestar.dto import DTOData

    if not OPENTELEMETRY_INSTALLED:
        from sqlspec._typing import Span, Status, StatusCode, Tracer, trace
    else:
        from opentelemetry import trace
        from opentelemetry.trace import Span, Status, StatusCode, Tracer

    if not PROMETHEUS_INSTALLED:
        from sqlspec._typing import Counter, Gauge, Histogram
    else:
        from prometheus_client import Counter, Gauge, Histogram

    if not AIOSQL_INSTALLED:
        from sqlspec._typing import (
            AiosqlAsyncProtocol,
            AiosqlParamType,
            AiosqlProtocol,
            AiosqlSQLOperationType,
            AiosqlSyncProtocol,
            aiosql,
        )
    else:
        import aiosql
        from aiosql.types import AsyncDriverAdapterProtocol as AiosqlAsyncProtocol
        from aiosql.types import DriverAdapterProtocol as AiosqlProtocol
        from aiosql.types import ParamType as AiosqlParamType
        from aiosql.types import SQLOperationType as AiosqlSQLOperationType
        from aiosql.types import SyncDriverAdapterProtocol as AiosqlSyncProtocol

    if not ATTRS_INSTALLED:
        from sqlspec._typing import AttrsInstance, attrs_asdict, attrs_define, attrs_field, attrs_fields, attrs_has
    else:
        from attrs import AttrsInstance
        from attrs import asdict as attrs_asdict
        from attrs import define as attrs_define
        from attrs import field as attrs_field
        from attrs import fields as attrs_fields
        from attrs import has as attrs_has

    if not CATTRS_INSTALLED:
        from sqlspec._typing import cattrs_structure, cattrs_unstructure
    else:
        from cattrs import structure as cattrs_structure
        from cattrs import unstructure as cattrs_unstructure


class DictLike(Protocol):
    """A protocol for objects that behave like a dictionary for reading."""

    def __getitem__(self, key: str) -> Any: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...


PYDANTIC_USE_FAILFAST = False  # leave permanently disabled for now


# TypeVars with TYPE_CHECKING guard for mypyc compatibility
if TYPE_CHECKING:
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
    ModelT = TypeVar("ModelT", bound="Union[DictLike, Struct, BaseModel, DataclassProtocol, AttrsInstance]")
    """Type variable for model types.

    :class:`DictLike` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol` | :class:`AttrsInstance`
    """
    RowT = TypeVar("RowT", bound="dict[str, Any]")
else:
    T = Any
    ConnectionT = Any
    PoolT = Any
    PoolT_co = Any
    ModelT = Any
    RowT = dict[str, Any]


DictRow: TypeAlias = "dict[str, Any]"
"""Type variable for DictRow types."""
TupleRow: TypeAlias = "tuple[Any, ...]"
"""Type variable for TupleRow types."""

SupportedSchemaModel: TypeAlias = "Union[DictLike, Struct, BaseModel, DataclassProtocol, AttrsInstance]"
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` | :class:`pydantic.BaseModel` | :class:`DataclassProtocol` | :class:`AttrsInstance`
"""
StatementParameters: TypeAlias = "Union[Any, dict[str, Any], list[Any], tuple[Any, ...], None]"
"""Type alias for statement parameters.

Represents:
- :type:`dict[str, Any]`
- :type:`list[Any]`
- :type:`tuple[Any, ...]`
- :type:`None`
"""
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
    "ATTRS_INSTALLED",
    "CATTRS_INSTALLED",
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
    "AttrsInstance",
    "BaseModel",
    "BulkModelDict",
    "ConnectionT",
    "Counter",
    "DTOData",
    "DataclassProtocol",
    "DictLike",
    "DictRow",
    "Empty",
    "EmptyEnum",
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
    "attrs_asdict",
    "attrs_define",
    "attrs_field",
    "attrs_fields",
    "attrs_has",
    "cattrs_structure",
    "cattrs_unstructure",
    "convert",
    "get_type_adapter",
    "trace",
)
