"""Serialization utilities for SQLSpec."""

from importlib import import_module
from typing import TYPE_CHECKING, Any

from sqlspec import _COMPILED

if TYPE_CHECKING:
    from sqlspec.utils.serializers._json import DEFAULT_TYPE_ENCODERS, TypeEncodersMap
    from sqlspec.utils.serializers._json import decode_json as from_json
    from sqlspec.utils.serializers._json import encode_json as to_json
    from sqlspec.utils.serializers._numpy import numpy_array_dec_hook, numpy_array_enc_hook
    from sqlspec.utils.serializers._schema import (
        SchemaSerializer,
        get_collection_serializer,
        get_serializer_metrics,
        reset_serializer_cache,
        schema_dump,
        serialize_collection,
    )

__all__ = (
    "DEFAULT_TYPE_ENCODERS",
    "SchemaSerializer",
    "TypeEncodersMap",
    "from_json",
    "get_collection_serializer",
    "get_serializer_metrics",
    "numpy_array_dec_hook",
    "numpy_array_enc_hook",
    "reset_serializer_cache",
    "schema_dump",
    "serialize_collection",
    "to_json",
)

_EXPORTS: dict[str, tuple[str, str]] = {
    "DEFAULT_TYPE_ENCODERS": ("sqlspec.utils.serializers._json", "DEFAULT_TYPE_ENCODERS"),
    "SchemaSerializer": ("sqlspec.utils.serializers._schema", "SchemaSerializer"),
    "TypeEncodersMap": ("sqlspec.utils.serializers._json", "TypeEncodersMap"),
    "from_json": ("sqlspec.utils.serializers._json", "decode_json"),
    "get_collection_serializer": ("sqlspec.utils.serializers._schema", "get_collection_serializer"),
    "get_serializer_metrics": ("sqlspec.utils.serializers._schema", "get_serializer_metrics"),
    "numpy_array_dec_hook": ("sqlspec.utils.serializers._numpy", "numpy_array_dec_hook"),
    "numpy_array_enc_hook": ("sqlspec.utils.serializers._numpy", "numpy_array_enc_hook"),
    "reset_serializer_cache": ("sqlspec.utils.serializers._schema", "reset_serializer_cache"),
    "schema_dump": ("sqlspec.utils.serializers._schema", "schema_dump"),
    "serialize_collection": ("sqlspec.utils.serializers._schema", "serialize_collection"),
    "to_json": ("sqlspec.utils.serializers._json", "encode_json"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | _EXPORTS.keys())


if _COMPILED:
    for _name in __all__:
        if _name not in globals():
            __getattr__(_name)
