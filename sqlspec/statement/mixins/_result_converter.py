import datetime
from collections.abc import Sequence
from enum import Enum
from functools import partial
from pathlib import Path, PurePath
from typing import Any, Callable, Optional, Union, cast, overload
from uuid import UUID

from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import (
    ModelDTOT,
    ModelT,
    convert,
    get_type_adapter,
    is_dataclass,
    is_msgspec_struct,
    is_pydantic_model,
)

_DEFAULT_TYPE_DECODERS: list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]] = [
    (lambda x: x is UUID, lambda t, v: t(v.hex)),
    (lambda x: x is datetime.datetime, lambda t, v: t(v.isoformat())),
    (lambda x: x is datetime.date, lambda t, v: t(v.isoformat())),
    (lambda x: x is datetime.time, lambda t, v: t(v.isoformat())),
    (lambda x: x is Enum, lambda t, v: t(v.value)),
]


def _default_msgspec_deserializer(
    target_type: Any,
    value: Any,
    type_decoders: "Optional[Sequence[tuple[Any, Any]]]" = None,
) -> Any:
    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)
    if target_type is UUID and isinstance(value, UUID):
        return value.hex
    if target_type in {datetime.datetime, datetime.date, datetime.time} and hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value
    if isinstance(value, target_type):
        return value
    if issubclass(target_type, (Path, PurePath, UUID)):
        return target_type(value)
    try:
        return target_type(value)
    except Exception as e:
        msg = f"Unsupported type: {type(value)!r}"
        raise TypeError(msg) from e


class ResultConverter:
    @overload
    @staticmethod
    def to_schema(data: "ModelT", *, schema_type: None = None) -> "ModelT": ...
    @overload
    @staticmethod
    def to_schema(data: "dict[str, Any]", *, schema_type: "type[ModelDTOT]") -> "ModelDTOT": ...
    @overload
    @staticmethod
    def to_schema(data: "Sequence[ModelT]", *, schema_type: None = None) -> "Sequence[ModelT]": ...
    @overload
    @staticmethod
    def to_schema(data: "Sequence[dict[str, Any]]", *, schema_type: "type[ModelDTOT]") -> "Sequence[ModelDTOT]": ...

    @staticmethod
    def to_schema(
        data: "Union[ModelT, dict[str, Any], Sequence[ModelT], Sequence[dict[str, Any]]]",
        *,
        schema_type: "Optional[type[ModelDTOT]]" = None,
    ) -> "Any":
        if schema_type is None:
            if not isinstance(data, Sequence):
                return cast("ModelT", data)
            return cast("Sequence[ModelT]", data)
        if is_dataclass(schema_type):
            if not isinstance(data, Sequence):
                return cast("ModelDTOT", schema_type(**data))
            return cast("Sequence[ModelDTOT]", [schema_type(**item) for item in data])
        if is_msgspec_struct(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    convert(
                        obj=data,
                        type=schema_type,
                        from_attributes=True,
                        dec_hook=partial(
                            _default_msgspec_deserializer,
                            type_decoders=_DEFAULT_TYPE_DECODERS,
                        ),
                    ),
                )
            return cast(
                "Sequence[ModelDTOT]",
                convert(
                    obj=data,
                    type=list[schema_type],
                    from_attributes=True,
                    dec_hook=partial(
                        _default_msgspec_deserializer,
                        type_decoders=_DEFAULT_TYPE_DECODERS,
                    ),
                ),
            )
        if schema_type is not None and is_pydantic_model(schema_type):
            if not isinstance(data, Sequence):
                return cast(
                    "ModelDTOT",
                    get_type_adapter(schema_type).validate_python(data, from_attributes=True),
                )
            return cast(
                "Sequence[ModelDTOT]",
                get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True),
            )
        msg = "`schema_type` should be a valid Dataclass, Pydantic model or Msgspec struct"
        raise SQLSpecError(msg)
