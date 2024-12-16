from __future__ import annotations

from collections.abc import Sequence
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
    FailFast,
    Struct,
    TypeAdapter,
    convert,
)
from sqlspec.utils.dataclass import DataclassProtocol, is_dataclass_instance, simple_asdict

if TYPE_CHECKING:
    from .filters import StatementFilter

PYDANTIC_USE_FAILFAST = False  # leave permanently disabled for now


T = TypeVar("T")

ModelT = TypeVar("ModelT", bound="Struct | BaseModel | DataclassProtocol")

FilterTypeT = TypeVar("FilterTypeT", bound="StatementFilter")
"""Type variable for filter types.

:class:`~advanced_alchemy.filters.StatementFilter`
"""
ModelDictT: TypeAlias = Union[dict[str, Any], ModelT, DataclassProtocol, Struct, BaseModel]
"""Type alias for model dictionaries.

Represents:
- :type:`dict[str, Any]` | :class:`~advanced_alchemy.base.ModelProtocol` | :class:`msgspec.Struct` |  :class:`pydantic.BaseModel` | :class:`litestar.dto.data_structures.DTOData` | :class:`~advanced_alchemy.base.ModelProtocol`
"""
ModelDictListT: TypeAlias = Sequence[Union[dict[str, Any], ModelT, DataclassProtocol, Struct, BaseModel]]
"""Type alias for model dictionary lists.

A list or sequence of any of the following:
- :type:`Sequence`[:type:`dict[str, Any]` | :class:`~advanced_alchemy.base.ModelProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`]

"""


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


def is_msgspec_model(v: Any) -> TypeGuard[Struct]:
    """Check if a value is a msgspec model.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return MSGSPEC_INSTALLED and isinstance(v, Struct)


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


def is_msgspec_model_with_field(v: Any, field_name: str) -> TypeGuard[Struct]:
    """Check if a msgspec model has a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_model(v) and field_name in v.__struct_fields__


def is_msgspec_model_without_field(v: Any, field_name: str) -> TypeGuard[Struct]:
    """Check if a msgspec model does not have a specific field.

    Args:
        v: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_msgspec_model_with_field(v, field_name)


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
    if is_dataclass(data):
        return simple_asdict(data, exclude_empty=exclude_unset)
    if is_pydantic_model(data):
        return data.model_dump(exclude_unset=exclude_unset)
    if is_msgspec_model(data) and exclude_unset:
        return {f: val for f in data.__struct_fields__ if (val := getattr(data, f, None)) != UNSET}
    if is_msgspec_model(data) and not exclude_unset:
        return {f: getattr(data, f, None) for f in data.__struct_fields__}
    return cast("dict[str,Any]", data)


__all__ = (
    "MSGSPEC_INSTALLED",
    "PYDANTIC_INSTALLED",
    "PYDANTIC_USE_FAILFAST",
    "UNSET",
    "BaseModel",
    "FailFast",
    "FilterTypeT",
    "ModelDictListT",
    "ModelDictT",
    "Struct",
    "TypeAdapter",
    "UnsetType",
    "convert",
    "get_type_adapter",
    "is_dict",
    "is_dict_with_field",
    "is_dict_without_field",
    "is_msgspec_model",
    "is_msgspec_model_with_field",
    "is_msgspec_model_without_field",
    "is_pydantic_model",
    "is_pydantic_model_with_field",
    "is_pydantic_model_without_field",
    "schema_dump",
)

if TYPE_CHECKING:
    if not PYDANTIC_INSTALLED:
        from ._typing import BaseModel, FailFast, TypeAdapter
    else:
        from pydantic import BaseModel, FailFast, TypeAdapter  # type: ignore[assignment] # noqa: TC004

    if not MSGSPEC_INSTALLED:
        from ._typing import UNSET, Struct, UnsetType, convert
    else:
        from msgspec import UNSET, Struct, UnsetType, convert  # type: ignore[assignment]  # noqa: TC004
