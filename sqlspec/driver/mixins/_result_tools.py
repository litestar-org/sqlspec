# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import datetime
import logging
from collections.abc import Sequence
from enum import Enum
from functools import partial
from pathlib import Path, PurePath
from typing import Any, Callable, Optional, Union, cast, overload
from uuid import UUID

from mypy_extensions import trait

from sqlspec.exceptions import SQLSpecError, wrap_exceptions
from sqlspec.typing import (
    CATTRS_INSTALLED,
    AttrsInstance,
    BaseModel,
    DataclassProtocol,
    DictLike,
    ModelDTOT,
    ModelT,
    Struct,
    attrs_asdict,
    cattrs_structure,
    cattrs_unstructure,
    convert,
    get_type_adapter,
)
from sqlspec.utils.type_guards import is_attrs_schema, is_dataclass, is_msgspec_struct, is_pydantic_model

__all__ = ("_DEFAULT_TYPE_DECODERS", "_default_msgspec_deserializer")


WINDOWS_PATH_MIN_LENGTH = 3
logger = logging.getLogger(__name__)
_DEFAULT_TYPE_DECODERS: list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]] = [
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
    if target_type is UUID and isinstance(value, UUID):
        return value.hex
    if target_type in {datetime.datetime, datetime.date, datetime.time}:
        with wrap_exceptions(suppress=AttributeError):
            return value.isoformat()
    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value
    if isinstance(value, target_type):
        return value
    if issubclass(target_type, (Path, PurePath, UUID)):
        return target_type(value)
    return value


@trait
class ToSchemaMixin:
    __slots__ = ()

    # Schema conversion overloads (most specific first)
    @overload
    @staticmethod
    def to_schema(data: "list[dict[str, Any]]", *, schema_type: "type[ModelDTOT]") -> "list[ModelDTOT]": ...
    @overload
    @staticmethod
    def to_schema(data: "list[DictLike]", *, schema_type: "type[ModelDTOT]") -> "list[ModelDTOT]": ...
    @overload
    @staticmethod
    def to_schema(data: "Sequence[ModelT]", *, schema_type: "type[ModelDTOT]") -> "Sequence[ModelDTOT]": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[ModelDTOT]") -> "ModelDTOT": ...
    @overload
    @staticmethod
    def to_schema(
        data: "Union[DictLike, Struct, BaseModel, DataclassProtocol, AttrsInstance]", *, schema_type: "type[ModelDTOT]"
    ) -> "ModelDTOT": ...
    @overload
    @staticmethod
    def to_schema(data: "list[ModelT]", *, schema_type: None = None) -> "list[ModelT]": ...
    @overload
    @staticmethod
    def to_schema(data: "ModelT", *, schema_type: None = None) -> "ModelT": ...

    @staticmethod
    def to_schema(
        data: "Union[ModelT, dict[str, Any], DictLike, Struct, BaseModel, DataclassProtocol, AttrsInstance, list[ModelT], list[dict[str, Any]], list[DictLike]]",
        *,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Union[ModelT, ModelDTOT, Sequence[ModelT], Sequence[ModelDTOT]]":
        """Convert data to a specified schema type.

        Supports conversion to dataclasses, msgspec structs, Pydantic models, and attrs classes.
        Handles both single objects and sequences.

        Raises:
            SQLSpecError if `schema_type` is not a valid type.

        Returns:
            Converted data in the specified schema type.

        """
        if schema_type is None:
            if not isinstance(data, Sequence):
                return cast("ModelT", data)
            return cast("Sequence[ModelT]", data)
        if is_dataclass(schema_type):
            if isinstance(data, list):
                return cast(
                    "Sequence[ModelDTOT]",
                    [schema_type(**dict(item) if hasattr(item, "keys") else item) for item in data],  # type: ignore[operator,arg-type]
                )
            if hasattr(data, "keys"):
                return cast("ModelDTOT", schema_type(**dict(data)))  # type: ignore[operator,arg-type]
            if isinstance(data, dict):
                return cast("ModelDTOT", schema_type(**data))  # type: ignore[operator]
            # Fallback for other types
            return cast("ModelDTOT", data)
        if is_msgspec_struct(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    convert(
                        obj=data,
                        type=schema_type,
                        from_attributes=True,
                        dec_hook=partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS),
                    ),
                )
            return cast(
                "Sequence[ModelDTOT]",
                convert(
                    obj=data,
                    type=list[schema_type],  # type: ignore[valid-type]  # pyright: ignore
                    from_attributes=True,
                    dec_hook=partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS),
                ),
            )
        if is_pydantic_model(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    get_type_adapter(schema_type).validate_python(data, from_attributes=True),  # pyright: ignore
                )
            return cast(
                "Sequence[ModelDTOT]",
                get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True),  # type: ignore[valid-type]  # pyright: ignore
            )
        if is_attrs_schema(schema_type):
            if CATTRS_INSTALLED:
                if isinstance(data, Sequence):
                    return cast("Sequence[ModelDTOT]", cattrs_structure(data, list[schema_type]))  # type: ignore[valid-type]
                # If data is already structured (attrs instance), unstructure it first
                if hasattr(data, "__attrs_attrs__"):
                    data = cattrs_unstructure(data)
                return cast("ModelDTOT", cattrs_structure(data, schema_type))
            if isinstance(data, list):
                return cast(
                    "Sequence[ModelDTOT]",
                    [schema_type(**dict(item) if hasattr(item, "keys") else attrs_asdict(item)) for item in data],  # type: ignore[arg-type]
                )
            if hasattr(data, "keys"):
                return cast("ModelDTOT", schema_type(**dict(data)))  # type: ignore[arg-type]
            if isinstance(data, dict):
                return cast("ModelDTOT", schema_type(**data))
            # Fallback for other types
            return cast("ModelDTOT", data)
        msg = "`schema_type` should be a valid Dataclass, Pydantic model, Msgspec struct, or Attrs class"
        raise SQLSpecError(msg)
