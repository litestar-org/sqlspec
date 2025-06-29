"""Type guard functions for runtime type checking in SQLSpec.

This module provides type-safe runtime checks that help the type checker
understand type narrowing, replacing defensive hasattr() and duck typing patterns.
"""

from collections.abc import Iterable, Sequence
from collections.abc import Set as AbstractSet
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from sqlspec.protocols import DictProtocol, IndexableRow, WithMethodProtocol
from sqlspec.typing import (
    LITESTAR_INSTALLED,
    MSGSPEC_INSTALLED,
    PYDANTIC_INSTALLED,
    BaseModel,
    DataclassProtocol,
    DTOData,
    Struct,
)

if TYPE_CHECKING:
    from dataclasses import Field

    from sqlglot import exp
    from typing_extensions import TypeGuard

    from sqlspec.statement.builder import SelectBuilder
    from sqlspec.statement.filters import LimitOffsetFilter, StatementFilter
    from sqlspec.typing import SupportedSchemaModel

__all__ = (
    "dataclass_to_dict",
    "extract_dataclass_fields",
    "extract_dataclass_items",
    "has_dict_attribute",
    "has_with_method",
    "is_dataclass",
    "is_dataclass_instance",
    "is_dataclass_with_field",
    "is_dataclass_without_field",
    "is_dict",
    "is_dict_row",
    "is_dict_with_field",
    "is_dict_without_field",
    "is_dto_data",
    "is_expression",
    "is_indexable_row",
    "is_iterable_parameters",
    "is_limit_offset_filter",
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
    "is_select_builder",
    "is_statement_filter",
    "schema_dump",
)


def is_statement_filter(obj: Any) -> "TypeGuard[StatementFilter]":
    """Check if an object implements the StatementFilter protocol.

    Args:
        obj: The object to check

    Returns:
        True if the object is a StatementFilter, False otherwise
    """
    # Import here to avoid circular imports
    from sqlspec.statement.filters import StatementFilter as FilterProtocol

    return isinstance(obj, FilterProtocol)


def is_limit_offset_filter(obj: Any) -> "TypeGuard[LimitOffsetFilter]":
    """Check if an object is a LimitOffsetFilter.

    Args:
        obj: The object to check

    Returns:
        True if the object is a LimitOffsetFilter, False otherwise
    """
    # Import here to avoid circular imports
    from sqlspec.statement.filters import LimitOffsetFilter

    return isinstance(obj, LimitOffsetFilter)


def is_select_builder(obj: Any) -> "TypeGuard[SelectBuilder]":
    """Check if an object is a SelectBuilder.

    Args:
        obj: The object to check

    Returns:
        True if the object is a SelectBuilder, False otherwise
    """
    from sqlspec.statement.builder import SelectBuilder

    return isinstance(obj, SelectBuilder)


def is_dict_row(row: Any) -> "TypeGuard[dict[str, Any]]":
    """Check if a row is a dictionary.

    Args:
        row: The row to check

    Returns:
        True if the row is a dictionary, False otherwise
    """
    return isinstance(row, dict)


def is_indexable_row(row: Any) -> "TypeGuard[IndexableRow]":
    """Check if a row supports index access via protocol.

    Args:
        row: The row to check

    Returns:
        True if the row is indexable, False otherwise
    """
    return isinstance(row, IndexableRow)


def is_iterable_parameters(params: Any) -> "TypeGuard[Sequence[Any]]":
    """Check if parameters are iterable (but not string or dict).

    Args:
        params: The parameters to check

    Returns:
        True if the parameters are iterable, False otherwise
    """
    return isinstance(params, Sequence) and not isinstance(params, (str, bytes, dict))


def has_with_method(obj: Any) -> "TypeGuard[WithMethodProtocol]":
    """Check if an object has a callable 'with_' method.

    This is a more specific check than hasattr for SQLGlot expressions.

    Args:
        obj: The object to check

    Returns:
        True if the object has a callable with_ method, False otherwise
    """
    return isinstance(obj, WithMethodProtocol)


# Type guards migrated from typing.py


def is_dataclass_instance(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass instance.

    Args:
        obj: An object to check.

    Returns:
        True if the object is a dataclass instance.
    """
    # Ensure obj is an instance and not the class itself,
    # and that its type is a dataclass.
    return not isinstance(obj, type) and hasattr(type(obj), "__dataclass_fields__")


def is_dataclass(obj: Any) -> "TypeGuard[DataclassProtocol]":
    """Check if an object is a dataclass.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    if isinstance(obj, type) and hasattr(obj, "__dataclass_fields__"):
        return True
    return is_dataclass_instance(obj)


def is_dataclass_with_field(obj: Any, field_name: str) -> "TypeGuard[object]":
    """Check if an object is a dataclass and has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(obj) and hasattr(obj, field_name)


def is_dataclass_without_field(obj: Any, field_name: str) -> "TypeGuard[object]":
    """Check if an object is a dataclass and does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dataclass(obj) and not hasattr(obj, field_name)


def is_pydantic_model(obj: Any) -> "TypeGuard[BaseModel]":
    """Check if a value is a pydantic model.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return PYDANTIC_INSTALLED and isinstance(obj, BaseModel)


def is_pydantic_model_with_field(obj: Any, field_name: str) -> "TypeGuard[BaseModel]":
    """Check if a pydantic model has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(obj) and hasattr(obj, field_name)


def is_pydantic_model_without_field(obj: Any, field_name: str) -> "TypeGuard[BaseModel]":
    """Check if a pydantic model does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_pydantic_model(obj) and not hasattr(obj, field_name)


def is_msgspec_struct(obj: Any) -> "TypeGuard[Struct]":
    """Check if a value is a msgspec struct.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return MSGSPEC_INSTALLED and isinstance(obj, Struct)


def is_msgspec_struct_with_field(obj: Any, field_name: str) -> "TypeGuard[Struct]":
    """Check if a msgspec struct has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) and hasattr(obj, field_name)


def is_msgspec_struct_without_field(obj: Any, field_name: str) -> "TypeGuard[Struct]":
    """Check if a msgspec struct does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) and not hasattr(obj, field_name)


def is_dict(obj: Any) -> "TypeGuard[dict[str, Any]]":
    """Check if a value is a dictionary.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, dict)


def is_dict_with_field(obj: Any, field_name: str) -> "TypeGuard[dict[str, Any]]":
    """Check if a dictionary has a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(obj) and field_name in obj


def is_dict_without_field(obj: Any, field_name: str) -> "TypeGuard[dict[str, Any]]":
    """Check if a dictionary does not have a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_dict(obj) and field_name not in obj


def is_schema(obj: Any) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return is_msgspec_struct(obj) or is_pydantic_model(obj)


def is_schema_or_dict(obj: Any) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return is_schema(obj) or is_dict(obj)


def is_schema_with_field(obj: Any, field_name: str) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_msgspec_struct_with_field(obj, field_name) or is_pydantic_model_with_field(obj, field_name)


def is_schema_without_field(obj: Any, field_name: str) -> "TypeGuard[SupportedSchemaModel]":
    """Check if a value is a msgspec Struct or Pydantic model without a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_with_field(obj, field_name)


def is_schema_or_dict_with_field(obj: Any, field_name: str) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict with a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return is_schema_with_field(obj, field_name) or is_dict_with_field(obj, field_name)


def is_schema_or_dict_without_field(
    obj: Any, field_name: str
) -> "TypeGuard[Union[SupportedSchemaModel, dict[str, Any]]]":
    """Check if a value is a msgspec Struct, Pydantic model, or dict without a specific field.

    Args:
        obj: Value to check.
        field_name: Field name to check for.

    Returns:
        bool
    """
    return not is_schema_or_dict_with_field(obj, field_name)


def is_dto_data(v: Any) -> "TypeGuard[DTOData[Any]]":
    """Check if a value is a Litestar DTOData object.

    Args:
        v: Value to check.

    Returns:
        bool
    """
    return LITESTAR_INSTALLED and isinstance(v, DTOData)


def is_expression(obj: Any) -> "TypeGuard[exp.Expression]":
    """Check if a value is a sqlglot Expression.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    from sqlglot import exp

    return isinstance(obj, exp.Expression)


def has_dict_attribute(obj: Any) -> "TypeGuard[DictProtocol]":
    """Check if an object has a __dict__ attribute.

    Args:
        obj: Value to check.

    Returns:
        bool
    """
    return isinstance(obj, DictProtocol)


# Dataclass utility functions


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
    from dataclasses import Field, fields

    from sqlspec._typing import Empty

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
    data: "Union[dict[str, Any], DataclassProtocol, Struct, BaseModel]", exclude_unset: bool = True
) -> "dict[str, Any]":
    """Dump a data object to a dictionary.

    Args:
        data:  :type:`dict[str, Any]` | :class:`DataclassProtocol` | :class:`msgspec.Struct` | :class:`pydantic.BaseModel`
        exclude_unset: :type:`bool` Whether to exclude unset values.

    Returns:
        :type:`dict[str, Any]`
    """
    from sqlspec._typing import UNSET

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

    if has_dict_attribute(data):
        return data.__dict__
    return cast("dict[str, Any]", data)
