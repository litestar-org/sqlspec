"""Schema transformation utilities for converting data to various schema types."""

import datetime
from collections.abc import Callable, Sequence
from enum import Enum
from functools import lru_cache, partial
from pathlib import Path, PurePath
from typing import Any, Final, TypeGuard, cast, overload
from uuid import UUID

from typing_extensions import TypeVar

from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import (
    CATTRS_INSTALLED,
    NUMPY_INSTALLED,
    SchemaT,
    attrs_asdict,
    cattrs_structure,
    cattrs_unstructure,
    convert,
    get_type_adapter,
)
from sqlspec.utils.data_transformation import transform_dict_keys
from sqlspec.utils.logging import get_logger
from sqlspec.utils.text import camelize, kebabize, pascalize
from sqlspec.utils.type_guards import (
    get_msgspec_rename_config,
    is_attrs_instance,
    is_attrs_schema,
    is_dataclass,
    is_dict,
    is_msgspec_struct,
    is_pydantic_model,
    is_typed_dict,
)

__all__ = (
    "_DEFAULT_TYPE_DECODERS",
    "DataT",
    "_convert_numpy_recursive",
    "_convert_numpy_to_list",
    "_default_msgspec_deserializer",
    "_is_list_type_target",
    "to_schema",
)

DataT = TypeVar("DataT", default=dict[str, Any])

logger = get_logger(__name__)

_DATETIME_TYPES: Final[set[type]] = {datetime.datetime, datetime.date, datetime.time}
_DATETIME_TYPE_TUPLE: Final[tuple[type, ...]] = (datetime.datetime, datetime.date, datetime.time)


def _is_list_type_target(target_type: Any) -> TypeGuard[list[object]]:
    """Check if target type is a list type (e.g., list[float])."""
    try:
        origin = target_type.__origin__
    except (AttributeError, TypeError):
        return False
    return origin is list


def _convert_numpy_to_list(target_type: Any, value: Any) -> Any:
    """Convert numpy array to list if target is a list type."""
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    if isinstance(value, np.ndarray) and _is_list_type_target(target_type):
        return value.tolist()

    return value


@lru_cache(maxsize=128)
def _detect_schema_type(schema_type: type) -> "str | None":
    """Detect schema type with LRU caching.

    Args:
        schema_type: Type to detect

    Returns:
        Type identifier string or None if unsupported
    """
    return (
        "typed_dict"
        if is_typed_dict(schema_type)
        else "dataclass"
        if is_dataclass(schema_type)
        else "msgspec"
        if is_msgspec_struct(schema_type)
        else "pydantic"
        if is_pydantic_model(schema_type)
        else "attrs"
        if is_attrs_schema(schema_type)
        else None
    )


def _convert_typed_dict(data: Any, schema_type: Any) -> Any:
    """Convert data to TypedDict."""
    return [item for item in data if is_dict(item)] if isinstance(data, list) else data


def _convert_dataclass(data: Any, schema_type: Any) -> Any:
    """Convert data to dataclass."""
    if isinstance(data, list):
        return [schema_type(**dict(item)) if is_dict(item) else item for item in data]
    return schema_type(**dict(data)) if is_dict(data) else (schema_type(**data) if isinstance(data, dict) else data)


class _IsTypePredicate:
    """Callable predicate to check if a type matches a target type."""

    __slots__ = ("_type",)

    def __init__(self, target_type: type) -> None:
        self._type = target_type

    def __call__(self, x: Any) -> bool:
        return x is self._type


class _UUIDDecoder:
    """Decoder for UUID types."""

    __slots__ = ()

    def __call__(self, t: type, v: Any) -> Any:
        return t(v.hex)


class _ISOFormatDecoder:
    """Decoder for types with isoformat() method (datetime, date, time)."""

    __slots__ = ()

    def __call__(self, t: type, v: Any) -> Any:
        return t(v.isoformat())


class _EnumDecoder:
    """Decoder for Enum types."""

    __slots__ = ()

    def __call__(self, t: type, v: Any) -> Any:
        return t(v.value)


_DEFAULT_TYPE_DECODERS: Final["list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]]"] = [
    (_IsTypePredicate(UUID), _UUIDDecoder()),
    (_IsTypePredicate(datetime.datetime), _ISOFormatDecoder()),
    (_IsTypePredicate(datetime.date), _ISOFormatDecoder()),
    (_IsTypePredicate(datetime.time), _ISOFormatDecoder()),
    (_IsTypePredicate(Enum), _EnumDecoder()),
    (_is_list_type_target, _convert_numpy_to_list),
]


def _default_msgspec_deserializer(
    target_type: Any, value: Any, type_decoders: "Sequence[tuple[Any, Any]] | None" = None
) -> Any:
    """Convert msgspec types with type decoder support.

    Args:
        target_type: Type to convert to
        value: Value to convert
        type_decoders: Optional sequence of (predicate, decoder) pairs

    Returns:
        Converted value or original value if conversion not applicable
    """
    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(value, np.ndarray) and _is_list_type_target(target_type):
            return value.tolist()

    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)

    if target_type is UUID and isinstance(value, UUID):
        return value.hex

    if target_type in _DATETIME_TYPES and isinstance(value, _DATETIME_TYPE_TUPLE):
        datetime_value = cast("datetime.datetime | datetime.date | datetime.time", value)
        return datetime_value.isoformat()

    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value

    try:
        if isinstance(target_type, type) and isinstance(value, target_type):
            return value
    except TypeError:
        pass

    if isinstance(target_type, type):
        try:
            if issubclass(target_type, (Path, PurePath)) or issubclass(target_type, UUID):
                return target_type(str(value))
        except (TypeError, ValueError):
            pass

    return value


def _convert_numpy_recursive(obj: Any) -> Any:
    """Recursively convert numpy arrays to lists.

    This is a module-level function to avoid nested function definitions
    which are problematic for mypyc compilation.

    Args:
        obj: Object to convert (may contain numpy arrays nested in dicts/lists)

    Returns:
        Object with all numpy arrays converted to lists
    """
    if not NUMPY_INSTALLED:
        return obj

    import numpy as np

    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: _convert_numpy_recursive(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(_convert_numpy_recursive(item) for item in obj)
    return obj


def _convert_msgspec(data: Any, schema_type: Any) -> Any:
    """Convert data to msgspec Struct."""
    rename_config = get_msgspec_rename_config(schema_type)
    deserializer = partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS)

    transformed_data = data
    if (rename_config and is_dict(data)) or (isinstance(data, Sequence) and data and is_dict(data[0])):
        try:
            converter_map: dict[str, Callable[[str], str]] = {"camel": camelize, "kebab": kebabize, "pascal": pascalize}
            converter = converter_map.get(rename_config) if rename_config else None
            if converter:
                transformed_data = (
                    [transform_dict_keys(item, converter) if is_dict(item) else item for item in data]
                    if isinstance(data, Sequence)
                    else (transform_dict_keys(data, converter) if is_dict(data) else data)
                )
        except Exception as e:
            logger.debug("Field name transformation failed for msgspec schema: %s", e)

    if NUMPY_INSTALLED:
        transformed_data = _convert_numpy_recursive(transformed_data)

    return convert(
        obj=transformed_data,
        type=(list[schema_type] if isinstance(transformed_data, Sequence) else schema_type),
        from_attributes=True,
        dec_hook=deserializer,
    )


def _convert_pydantic(data: Any, schema_type: Any) -> Any:
    """Convert data to Pydantic model."""
    if isinstance(data, Sequence):
        return get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True)
    return get_type_adapter(schema_type).validate_python(data, from_attributes=True)


def _convert_attrs(data: Any, schema_type: Any) -> Any:
    """Convert data to attrs class."""
    if CATTRS_INSTALLED:
        if isinstance(data, Sequence):
            return cattrs_structure(data, list[schema_type])
        structured = cattrs_unstructure(data) if is_attrs_instance(data) else data
        return cattrs_structure(structured, schema_type)

    if isinstance(data, list):
        return [schema_type(**dict(item)) if is_dict(item) else schema_type(**attrs_asdict(item)) for item in data]
    return schema_type(**dict(data)) if is_dict(data) else data


_SCHEMA_CONVERTERS: "dict[str, Callable[[Any, Any], Any]]" = {
    "typed_dict": _convert_typed_dict,
    "dataclass": _convert_dataclass,
    "msgspec": _convert_msgspec,
    "pydantic": _convert_pydantic,
    "attrs": _convert_attrs,
}


@overload
def to_schema(data: "list[DataT]", *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...
@overload
def to_schema(data: "list[DataT]", *, schema_type: None = None) -> "list[DataT]": ...
@overload
def to_schema(data: "DataT", *, schema_type: "type[SchemaT]") -> "SchemaT": ...
@overload
def to_schema(data: "DataT", *, schema_type: None = None) -> "DataT": ...


def to_schema(data: Any, *, schema_type: Any = None) -> Any:
    """Convert data to a specified schema type.

    Supports transformation to various schema types including:
    - TypedDict
    - dataclasses
    - msgspec Structs
    - Pydantic models
    - attrs classes

    Args:
        data: Input data to convert (dict, list of dicts, or other)
        schema_type: Target schema type for conversion. If None, returns data unchanged.

    Returns:
        Converted data in the specified schema type, or original data if schema_type is None

    Raises:
        SQLSpecError: If schema_type is not a supported type
    """
    if schema_type is None:
        return data

    schema_type_key = _detect_schema_type(schema_type)
    if schema_type_key is None:
        msg = "`schema_type` should be a valid Dataclass, Pydantic model, Msgspec struct, Attrs class, or TypedDict"
        raise SQLSpecError(msg)

    return _SCHEMA_CONVERTERS[schema_type_key](data, schema_type)
