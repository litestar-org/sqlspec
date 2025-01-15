from __future__ import annotations  # noqa: A005

from collections.abc import Sequence
from dataclasses import Field, fields
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import TypeAlias, TypeGuard

from sqlspec._typing import (
    MSGSPEC_INSTALLED,
    PYDANTIC_INSTALLED,
    UNSET,
    BaseModel,
    DataclassProtocol,
    Empty,
    EmptyType,
    FailFast,
    Struct,
    TypeAdapter,
    UnsetType,
    convert,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Set as AbstractSet

    from sqlspec.filters import StatementFilter


PYDANTIC_USE_FAILFAST = False  # leave permanently disabled for now


T = TypeVar("T")

ModelT = TypeVar("ModelT", bound="Struct | BaseModel | DataclassProtocol")

FilterTypeT = TypeVar("FilterTypeT", bound="StatementFilter")
"""Type variable for filter types.

:class:`~advanced_alchemy.filters.StatementFilter`
"""
ModelDTOT = TypeVar("ModelDTOT", bound="Struct | BaseModel")
"""Type variable for model DTOs.

:class:`msgspec.Struct`|:class:`pydantic.BaseModel`
"""
PydanticOrMsgspecT: TypeAlias = Union[Struct, BaseModel]
"""Type alias for pydantic or msgspec models.

:class:`msgspec.Struct` or :class:`pydantic.BaseModel`
"""
ModelDictT: TypeAlias = Union[dict[str, Any], ModelT, DataclassProtocol, Struct, BaseModel]
"""Type alias for model dictionaries.

Represents:
- :type:`dict[str, Any]` |  :class:`msgspec.Struct` |  :class:`pydantic.BaseModel`
"""
ModelDictListT: TypeAlias = Sequence[Union[dict[str, Any], ModelT, DataclassProtocol, Struct, BaseModel]]
"""Type alias for model dictionary lists.

A list or sequence of any of the following:
- :type:`Sequence`[:type:`dict[str, Any]` | :class:`~advanced_alchemy.base.ModelProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`]

"""


def is_dataclass_instance(obj: Any) -> TypeGuard[DataclassProtocol]:
    """Check if an object is a dataclass instance.

    Args:
        obj: An object to check.

    Returns:
        True if the object is a dataclass instance.
    """
    return hasattr(type(obj), "__dataclass_fields__")  # pyright: ignore[reportUnknownArgumentType]


@lru_cache(typed=True)
def get_type_adapter(f: type[T]) -> TypeAdapter[T]:
    """Caches and returns a pydantic type adapter.

    Args:
        f: Type to create a type adapter for.

    Returns:
        :class:`pydantic.TypeAdapter`[:class:`typing.TypeVar`[T]]
    """
    if PYDANTIC_USE_FAILFAST:
        return TypeAdapter(
            Annotated[f, FailFast()],
        )
    return TypeAdapter(f)


def is_pydantic_model(v: Any) -> TypeGuard[BaseModel]:
    """Check if a value is a pydantic model.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return PYDANTIC_INSTALLED and isinstance(v, BaseModel)


def is_pydantic_model_with_field(v: Any, field_name: str) -> TypeGuard[BaseModel]:
    """Check if a pydantic model has a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(v) and field_name in v.model_fields


def is_pydantic_model_without_field(v: Any, field_name: str) -> TypeGuard[BaseModel]:
    """Check if a pydantic model does not have a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_pydantic_model_with_field(v, field_name)


def is_msgspec_struct(v: Any) -> TypeGuard[Struct]:
    """Check if a value is a msgspec model.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return MSGSPEC_INSTALLED and isinstance(v, Struct)


def is_msgspec_struct_with_field(v: Any, field_name: str) -> TypeGuard[Struct]:
    """Check if a msgspec model has a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(v) and field_name in v.__struct_fields__


def is_msgspec_struct_without_field(v: Any, field_name: str) -> TypeGuard[Struct]:
    """Check if a msgspec model does not have a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_msgspec_struct_with_field(v, field_name)


def is_dict(v: Any) -> TypeGuard[dict[str, Any]]:
    """Check if a value is a dictionary.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return isinstance(v, dict)


def is_dict_with_field(v: Any, field_name: str) -> TypeGuard[dict[str, Any]]:
    """Check if a dictionary has a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(v) and field_name in v


def is_dict_without_field(v: Any, field_name: str) -> TypeGuard[dict[str, Any]]:
    """Check if a dictionary does not have a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(v) and field_name not in v


def is_schema(v: Any) -> TypeGuard[Struct | BaseModel]:
    """Check if a value is a msgspec Struct or Pydantic model.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return is_msgspec_struct(v) or is_pydantic_model(v)


def is_schema_or_dict(v: Any) -> TypeGuard[Struct | BaseModel | dict[str, Any]]:
    """Check if a value is a msgspec Struct, Pydantic model, or dict.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return is_schema(v) or is_dict(v)


def is_schema_with_field(v: Any, field_name: str) -> TypeGuard[Struct | BaseModel]:
    """Check if a value is a msgspec Struct or Pydantic model with a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct_with_field(v, field_name) or is_pydantic_model_with_field(v, field_name)


def is_schema_without_field(v: Any, field_name: str) -> TypeGuard[Struct | BaseModel]:
    """Check if a value is a msgspec Struct or Pydantic model without a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_with_field(v, field_name)


def is_schema_or_dict_with_field(v: Any, field_name: str) -> TypeGuard[Struct | BaseModel | dict[str, Any]]:
    """Check if a value is a msgspec Struct, Pydantic model, or dict with a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_schema_with_field(v, field_name) or is_dict_with_field(v, field_name)


def is_schema_or_dict_without_field(v: Any, field_name: str) -> TypeGuard[Struct | BaseModel | dict[str, Any]]:
    """Check if a value is a msgspec Struct, Pydantic model, or dict without a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_or_dict_with_field(v, field_name)


def is_dataclass(v: Any) -> TypeGuard[DataclassProtocol]:
    """Check if a value is a dataclass.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return is_dataclass_instance(v)


def is_dataclass_with_field(v: Any, field_name: str) -> TypeGuard[DataclassProtocol]:
    """Check if a dataclass has a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(v) and field_name in v.__dataclass_fields__


def is_dataclass_without_field(v: Any, field_name: str) -> TypeGuard[DataclassProtocol]:
    """Check if a dataclass does not have a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(v) and field_name not in v.__dataclass_fields__


def extract_dataclass_fields(
    dt: DataclassProtocol,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: AbstractSet[str] | None = None,
    exclude: AbstractSet[str] | None = None,
) -> tuple[Field[Any], ...]:
    """Extract dataclass fields.

    Args:
        dt: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        include: An iterable of fields to include.
        exclude: An iterable of fields to exclude.


    Returns:
        A tuple of dataclass fields.
    """
    include = include or set()
    exclude = exclude or set()

    if common := (include & exclude):
        msg = f"Fields {common} are both included and excluded."
        raise ValueError(msg)

    dataclass_fields: Iterable[Field[Any]] = fields(dt)
    if exclude_none:
        dataclass_fields = (field for field in dataclass_fields if getattr(dt, field.name) is not None)
    if exclude_empty:
        dataclass_fields = (field for field in dataclass_fields if getattr(dt, field.name) is not Empty)
    if include:
        dataclass_fields = (field for field in dataclass_fields if field.name in include)
    if exclude:
        dataclass_fields = (field for field in dataclass_fields if field.name not in exclude)

    return tuple(dataclass_fields)


def extract_dataclass_items(
    dt: DataclassProtocol,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    include: AbstractSet[str] | None = None,
    exclude: AbstractSet[str] | None = None,
) -> tuple[tuple[str, Any], ...]:
    """Extract dataclass name, value pairs.

    Unlike the 'asdict' method exports by the stdlib, this function does not pickle values.

    Args:
        dt: A dataclass instance.
        exclude_none: Whether to exclude None values.
        exclude_empty: Whether to exclude Empty values.
        include: An iterable of fields to include.
        exclude: An iterable of fields to exclude.

    Returns:
        A tuple of key/value pairs.
    """
    dataclass_fields = extract_dataclass_fields(dt, exclude_none, exclude_empty, include, exclude)
    return tuple((field.name, getattr(dt, field.name)) for field in dataclass_fields)


def dataclass_to_dict(
    obj: DataclassProtocol,
    exclude_none: bool = False,
    exclude_empty: bool = False,
    convert_nested: bool = True,
    exclude: set[str] | None = None,
) -> dict[str, Any]:
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
    data: dict[str, Any] | Struct | BaseModel | DataclassProtocol,
    exclude_unset: bool = True,
) -> dict[str, Any]:
    """Dump a data object to a dictionary.

    Args:
        data:  dict[str, Any] | ModelT | Struct | BaseModel | DataclassProtocol
        exclude_unset: :type:`bool` Whether to exclude unset values.

    Returns:
        :type: dict[str, Any]
    """
    if is_dict(data):
        return data
    if is_dataclass(data):
        return dataclass_to_dict(data, exclude_empty=exclude_unset)
    if is_pydantic_model(data):
        return data.model_dump(exclude_unset=exclude_unset)
    if is_msgspec_struct(data) and exclude_unset:
        return {f: val for f in data.__struct_fields__ if (val := getattr(data, f, None)) != UNSET}
    if is_msgspec_struct(data) and not exclude_unset:
        return {f: getattr(data, f, None) for f in data.__struct_fields__}
    return cast("dict[str,Any]", data)


__all__ = (
    "MSGSPEC_INSTALLED",
    "PYDANTIC_INSTALLED",
    "PYDANTIC_USE_FAILFAST",
    "UNSET",
    "BaseModel",
    "DataclassProtocol",
    "Empty",
    "EmptyType",
    "FailFast",
    "FilterTypeT",
    "ModelDictListT",
    "ModelDictT",
    "Struct",
    "TypeAdapter",
    "UnsetType",
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
    "is_msgspec_struct",
    "is_msgspec_struct_with_field",
    "is_msgspec_struct_without_field",
    "is_pydantic_model",
    "is_pydantic_model_with_field",
    "is_pydantic_model_without_field",
    "schema_dump",
)

if TYPE_CHECKING:
    if not PYDANTIC_INSTALLED:
        from ._typing import BaseModel, FailFast, TypeAdapter
    else:
        from pydantic import BaseModel, FailFast, TypeAdapter  # noqa: TC004

    if not MSGSPEC_INSTALLED:
        from ._typing import UNSET, Struct, UnsetType, convert
    else:
        from msgspec import UNSET, Struct, UnsetType, convert  # noqa: TC004
