from collections.abc import Iterable, Mapping
from collections.abc import Set as AbstractSet
from dataclasses import Field, fields
from functools import lru_cache
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union, cast

from sqlglot import exp
from typing_extensions import TypeAlias, TypeGuard, TypeVar

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
    from collections.abc import Iterable, Sequence


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


def is_dataclass_instance(obj: Any) -> TypeGuard[DataclassProtocol]:
    """Check if an object is a dataclass instance.

    Args:
        obj: An object to check.

    Returns:
        True if the object is a dataclass instance.
    """
    # Ensure obj is an instance and not the class itself,
    # and that its type is a dataclass.
    return not isinstance(obj, type) and hasattr(type(obj), "__dataclass_fields__")


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


def is_pydantic_model(obj: Any) -> "TypeGuard[BaseModel]":
    """Check if a value is a pydantic model.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return PYDANTIC_INSTALLED and isinstance(obj, BaseModel)


def is_pydantic_model_with_field(obj: "Any", field_name: str) -> "TypeGuard[BaseModel]":
    """Check if a pydantic model has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(obj) and hasattr(obj, field_name)


def is_pydantic_model_without_field(obj: "Any", field_name: str) -> "TypeGuard[BaseModel]":
    """Check if a pydantic model does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(obj) and not hasattr(obj, field_name)


def is_msgspec_struct(obj: "Any") -> "TypeGuard[Struct]":
    """Check if a value is a msgspec struct.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return MSGSPEC_INSTALLED and isinstance(obj, Struct)


def is_msgspec_struct_with_field(obj: "Any", field_name: str) -> "TypeGuard[Struct]":
    """Check if a msgspec struct has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) and hasattr(obj, field_name)


def is_msgspec_struct_without_field(obj: "Any", field_name: str) -> "TypeGuard[Struct]":
    """Check if a msgspec struct does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) and not hasattr(obj, field_name)


def is_dict(obj: "Any") -> "TypeGuard[dict[str, Any]]":
    """Check if a value is a dictionary.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, dict)


def is_dict_with_field(obj: "Any", field_name: str) -> "TypeGuard[dict[str, Any]]":
    """Check if a dictionary has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(obj) and field_name in obj


def is_dict_without_field(obj: "Any", field_name: str) -> "TypeGuard[dict[str, Any]]":
    """Check if a dictionary does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(obj) and field_name not in obj


def is_schema(obj: "Any") -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) or is_pydantic_model(obj)


def is_schema_or_dict(obj: "Any") -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return is_schema(obj) or is_dict(obj)


def is_schema_with_field(obj: "Any", field_name: str) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct_with_field(obj, field_name) or is_pydantic_model_with_field(obj, field_name)


def is_schema_without_field(obj: "Any", field_name: str) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model without a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_with_field(obj, field_name)


def is_schema_or_dict_with_field(
    obj: "Any", field_name: str
) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_schema_with_field(obj, field_name) or is_dict_with_field(obj, field_name)


def is_schema_or_dict_without_field(
    obj: "Any", field_name: str
) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict without a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_or_dict_with_field(obj, field_name)


def is_dataclass(obj: "Any") -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    if isinstance(obj, type) and hasattr(obj, "__dataclass_fields__"):
        return True
    return is_dataclass_instance(obj)


def is_dataclass_with_field(
    obj: "Any", field_name: str
) -> "TypeGuard[object]":  # Can't specify dataclass type directly
    """Check if an object is a dataclass and has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(obj) and hasattr(obj, field_name)


def is_dataclass_without_field(obj: "Any", field_name: str) -> "TypeGuard[object]":
    """Check if an object is a dataclass and does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(obj) and not hasattr(obj, field_name)


def extract_dataclass_fields(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "Optional[AbstractSet[str]]" = None,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "tuple[Field[Any], ...]":
    """Extract dataclass fields.

    Args:
        obj: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        include: An iterable of fields to include.
        exclude: An iterable of fields to exclude.

    Raises:
        ValueError: If there are fields that are both included and excluded.

    Returns:
        A tuple of dataclass fields.
    """
    include = include or set()
    exclude = exclude or set()

    if common := (include & exclude):
        msg = f"Fields {common} are both included and excluded."
        raise ValueError(msg)

    dataclass_fields: Iterable[Field[Any]] = fields(obj)
    if exclude_none:
        dataclass_fields = (field for field in dataclass_fields if getattr(obj, field.name) is not None)
    if exclude_empty:
        dataclass_fields = (field for field in dataclass_fields if getattr(obj, field.name) is not Empty)
    if include:
        dataclass_fields = (field for field in dataclass_fields if field.name in include)
    if exclude:
        dataclass_fields = (field for field in dataclass_fields if field.name not in exclude)

    return tuple(dataclass_fields)


def extract_dataclass_items(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: "Optional[AbstractSet[str]]" = None,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "tuple[tuple[str, Any], ...]":
    """Extract dataclass name, value pairs.

    Unlike the 'asdict' method exports by the stdlib, this function does not pickle values.

    Args:
        obj: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        include: An iterable of fields to include.
        exclude: An iterable of fields to exclude.

    Returns:
        A tuple of key/value pairs.
    """
    dataclass_fields = extract_dataclass_fields(obj, exclude_none, exclude_empty, include, exclude)
    return tuple((field.name, getattr(obj, field.name)) for field in dataclass_fields)


def dataclass_to_dict(
    obj: "DataclassProtocol",
    exclude_none: bool = False,
    exclude_empty: bool = False,
    convert_nested: bool = True,
    exclude: "Optional[AbstractSet[str]]" = None,
) -> "dict[str, Any]":
    """Convert a dataclass to a dictionary.

    This method has important differences to the standard library version:
    - it does not deepcopy values
    - it does not recurse into collections

    Args:
        obj: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        convert_nested: Whether to recursively convert nested dataclasses.
        exclude: An iterable of fields to exclude.

    Returns:
        A dictionary of key/value pairs.
    """
    ret = {}
    for field in extract_dataclass_fields(obj, exclude_none, exclude_empty, exclude=exclude):
        value = getattr(obj, field.name)
        if is_dataclass_instance(value) and convert_nested:
            ret[field.name] = dataclass_to_dict(value, exclude_none, exclude_empty)
        else:
            ret[field.name] = getattr(obj, field.name)
    return cast("dict[str, Any]", ret)


def schema_dump(
    data: "Union[dict[str, Any],   DataclassProtocol, Struct, BaseModel]", exclude_unset: bool = True
) -> "dict[str, Any]":
    """Dump a data object to a dictionary.

    Args:
        data:  :type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`
        exclude_unset: :type:`bool` Whether to exclude unset values.

    Returns:
        :type:`dict[str, Any]`
    """
    if is_dict(data):
        return data
    if is_dataclass(data):
        return dataclass_to_dict(data, exclude_empty=exclude_unset)
    if is_pydantic_model(data):
        return data.model_dump(exclude_unset=exclude_unset)
    if is_msgspec_struct(data):
        if exclude_unset:
            return {f: val for f in data.__struct_fields__ if (val := getattr(data, f, None)) != UNSET}
        return {f: getattr(data, f, None) for f in data.__struct_fields__}

    if hasattr(data, "__dict__"):
        return data.__dict__
    return cast("dict[str, Any]", data)


def is_dto_data(v: Any) -> TypeGuard[DTOData[Any]]:
    """Check if a value is a Litestar DTOData object.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return LITESTAR_INSTALLED and isinstance(v, DTOData)


def is_expression(obj: "Any") -> "TypeGuard[exp.Expression]":
    """Check if a value is a sqlglot Expression.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, exp.Expression)


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
    "dataclass_to_dict",
    "extract_dataclass_fields",
    "extract_dataclass_items",
    "get_type_adapter",
    "is_dataclass",
    "is_dataclass_instance",
    "is_dataclass_with_field",
    "is_dataclass_without_field",
    "is_dict",
    "is_dict_with_field",
    "is_dict_without_field",
    "is_dto_data",
    "is_expression",
    "is_msgspec_struct",
    "is_msgspec_struct_with_field",
    "is_msgspec_struct_without_field",
    "is_pydantic_model",
    "is_pydantic_model_with_field",
    "is_pydantic_model_without_field",
    "is_schema",
    "is_schema_or_dict",
    "is_schema_or_dict_with_field",
    "is_schema_or_dict_without_field",
    "is_schema_with_field",
    "is_schema_without_field",
    "schema_dump",
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
