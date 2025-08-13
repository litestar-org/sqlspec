# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import datetime
import logging
from collections.abc import Sequence
from enum import Enum
from functools import partial
from pathlib import Path, PurePath
from typing import Any, Callable, Final, Optional, overload
from uuid import UUID

from mypy_extensions import trait

from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import (
    CATTRS_INSTALLED,
    ModelDTOT,
    ModelT,
    attrs_asdict,
    cattrs_structure,
    cattrs_unstructure,
    convert,
    get_type_adapter,
)
from sqlspec.utils.type_guards import is_attrs_schema, is_dataclass, is_msgspec_struct, is_pydantic_model

__all__ = ("_DEFAULT_TYPE_DECODERS", "_default_msgspec_deserializer")


logger = logging.getLogger(__name__)

# Constants for performance optimization
_DATETIME_TYPES: Final[set[type]] = {datetime.datetime, datetime.date, datetime.time}
_PATH_TYPES: Final[tuple[type, ...]] = (Path, PurePath, UUID)

_DEFAULT_TYPE_DECODERS: Final[list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]]] = [
    (lambda x: x is UUID, lambda t, v: t(v.hex)),
    (lambda x: x is datetime.datetime, lambda t, v: t(v.isoformat())),
    (lambda x: x is datetime.date, lambda t, v: t(v.isoformat())),
    (lambda x: x is datetime.time, lambda t, v: t(v.isoformat())),
    (lambda x: x is Enum, lambda t, v: t(v.value)),
]


def _default_msgspec_deserializer(
    target_type: Any, value: Any, type_decoders: "Optional[Sequence[tuple[Any, Any]]]" = None
) -> Any:
    """Default msgspec deserializer with type conversion support.

    Converts values to appropriate types for msgspec deserialization, including
    UUID, datetime, date, time, Enum, Path, and PurePath types.
    """
    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)

    # Fast path checks using type identity and isinstance
    if target_type is UUID and isinstance(value, UUID):
        return value.hex

    # Use pre-computed set for faster lookup
    if target_type in _DATETIME_TYPES:
        try:
            return value.isoformat()
        except AttributeError:
            pass

    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value

    if isinstance(value, target_type):
        return value

    # Check for path types using pre-computed tuple
    if isinstance(target_type, type):
        try:
            if issubclass(target_type, (Path, PurePath)) or issubclass(target_type, UUID):
                return target_type(str(value))
        except (TypeError, ValueError):
            pass

    return value


@trait
class ToSchemaMixin:
    __slots__ = ()

    # Schema conversion overloads - handle common cases first
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]") -> "list[dict[str, Any]]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: "type[ModelDTOT]") -> "list[ModelDTOT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: None = None) -> "list[dict[str, Any]]": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]") -> "dict[str, Any]": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[ModelDTOT]") -> "ModelDTOT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: None = None) -> "dict[str, Any]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[ModelT]") -> "list[ModelT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[ModelT]", *, schema_type: "type[ModelDTOT]") -> "list[ModelDTOT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[ModelT]", *, schema_type: None = None) -> "list[ModelT]": ...
    @overload
    @staticmethod
    def to_schema(data: "ModelT") -> "ModelT": ...
    @overload
    @staticmethod
    def to_schema(data: Any, *, schema_type: None = None) -> Any: ...

    @staticmethod
    def to_schema(data: Any, *, schema_type: "Optional[type[ModelDTOT]]" = None) -> Any:
        """Convert data to a specified schema type.

        Supports conversion to dataclasses, msgspec structs, Pydantic models, and attrs classes.
        Handles both single objects and sequences.

        Raises:
            SQLSpecError if `schema_type` is not a valid type.

        Returns:
            Converted data in the specified schema type.

        """
        if schema_type is None:
            return data
        if is_dataclass(schema_type):
            if isinstance(data, list):
                result: list[Any] = []
                for item in data:
                    if hasattr(item, "keys"):
                        result.append(schema_type(**dict(item)))  # type: ignore[operator]
                    else:
                        result.append(item)
                return result
            if hasattr(data, "keys"):
                return schema_type(**dict(data))  # type: ignore[operator]
            if isinstance(data, dict):
                return schema_type(**data)  # type: ignore[operator]
            return data
        if is_msgspec_struct(schema_type):
            # Cache the deserializer to avoid repeated partial() calls
            deserializer = partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS)
            if not isinstance(data, Sequence):
                return convert(obj=data, type=schema_type, from_attributes=True, dec_hook=deserializer)
            return convert(
                obj=data,
                type=list[schema_type],  # type: ignore[valid-type]  # pyright: ignore
                from_attributes=True,
                dec_hook=deserializer,
            )
        if is_pydantic_model(schema_type):
            if not isinstance(data, Sequence):
                adapter = get_type_adapter(schema_type)
                return adapter.validate_python(data, from_attributes=True)  # pyright: ignore
            list_adapter = get_type_adapter(list[schema_type])  # type: ignore[valid-type]  # pyright: ignore
            return list_adapter.validate_python(data, from_attributes=True)
        if is_attrs_schema(schema_type):
            if CATTRS_INSTALLED:
                if isinstance(data, Sequence):
                    return cattrs_structure(data, list[schema_type])  # type: ignore[valid-type]  # pyright: ignore
                if hasattr(data, "__attrs_attrs__"):
                    unstructured_data = cattrs_unstructure(data)
                    return cattrs_structure(unstructured_data, schema_type)  # pyright: ignore
                return cattrs_structure(data, schema_type)  # pyright: ignore
            if isinstance(data, list):
                attrs_result: list[Any] = []
                for item in data:
                    if hasattr(item, "keys"):
                        attrs_result.append(schema_type(**dict(item)))
                    else:
                        attrs_result.append(schema_type(**attrs_asdict(item)))
                return attrs_result
            if hasattr(data, "keys"):
                return schema_type(**dict(data))
            if isinstance(data, dict):
                return schema_type(**data)
            return data
        msg = "`schema_type` should be a valid Dataclass, Pydantic model, Msgspec struct, or Attrs class"
        raise SQLSpecError(msg)
