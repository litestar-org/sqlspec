"""Spanner type conversion - output and input handling.

Combines output conversion (database results → Python) and input conversion
(Python params → Spanner format) in a single module. Designed for mypyc
compilation with no nested functions.

Output conversion handles:
    - UUID detection and conversion from strings/bytes
    - JSON detection and deserialization

Input conversion handles:
    - UUID → 36-character strings (when automatic conversion is enabled)
    - bytes → base64-encoded bytes
    - datetime timezone awareness
    - dict/list → JsonObject wrapping
    - param_types inference
"""

import base64
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from sqlspec.adapters.spanner._typing import SpannerInterval as Interval
from sqlspec.adapters.spanner._typing import SpannerJsonObject as JsonObject
from sqlspec.adapters.spanner._typing import spanner_param_types as param_types
from sqlspec.core import TypedParameter
from sqlspec.utils.module_loader import import_optional_attr
from sqlspec.utils.type_converters import should_json_encode_sequence
from sqlspec.utils.uuids import uuid_from_bytes

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.protocols import SpannerParamTypesProtocol

__all__ = (
    "bytes_to_spanner",
    "coerce_params_for_spanner",
    "infer_spanner_param_types",
    "spanner_json",
    "spanner_to_bytes",
    "spanner_to_uuid",
    "uuid_to_spanner",
)

_UUID_TYPES: "tuple[type[Any], ...]" = (UUID,)
_uuid_utils_uuid = import_optional_attr("uuid_utils", "UUID")
if _uuid_utils_uuid is not None:
    _UUID_TYPES = (UUID, _uuid_utils_uuid)

_STRING_PARAM_TYPES: "tuple[type[Any], ...]" = (str, *_UUID_TYPES)

UUID_BYTE_LENGTH: int = 16
_SPANNER_PARAM_TYPES: "SpannerParamTypesProtocol | None" = None
_JSON_OBJECT_TYPE: "type[Any] | None" = None


def bytes_to_spanner(value: "bytes | None") -> "bytes | None":
    """Convert Python bytes to Spanner BYTES format.

    The Spanner Python client requires base64-encoded bytes when
    param_types.BYTES is specified.

    Args:
        value: Python bytes or None.

    Returns:
        Base64-encoded bytes or None.
    """
    if value is None:
        return None
    return base64.b64encode(value)


def spanner_to_bytes(value: Any) -> "bytes | None":
    """Convert Spanner BYTES result to Python bytes.

    Handles both raw bytes and base64-encoded bytes.

    Args:
        value: Value from Spanner (bytes or None).

    Returns:
        Python bytes or None.
    """
    if value is None:
        return None
    if isinstance(value, (bytes, str)):
        return base64.b64decode(value)
    return None


def uuid_to_spanner(value: UUID) -> bytes:
    """Convert Python UUID to 16-byte binary for Spanner BYTES(16).

    Args:
        value: Python UUID object.

    Returns:
        16-byte binary representation (RFC 4122 big-endian).
    """
    return value.bytes


def spanner_to_uuid(value: "bytes | None") -> "UUID | bytes | None":
    """Convert 16-byte binary from Spanner to Python UUID.

    Falls back to bytes if value is not valid UUID format.

    Args:
        value: 16-byte binary from Spanner or None.

    Returns:
        Python UUID if valid, original bytes if invalid, None if NULL.
    """
    if value is None:
        return None
    if not isinstance(value, bytes):
        return None
    if len(value) != UUID_BYTE_LENGTH:
        return value
    try:
        return uuid_from_bytes(value)
    except (ValueError, TypeError):
        return value


def spanner_json(value: Any) -> Any:
    """Wrap JSON values for Spanner JSON parameters.

    Args:
        value: JSON-compatible value (dict, list, tuple, or scalar).

    Returns:
        JsonObject wrapper when available, otherwise the original value.
    """
    json_type = _get_json_object_type()
    if isinstance(value, json_type):
        return value
    return json_type(value)


def coerce_params_for_spanner(
    params: "dict[str, Any] | None",
    json_serializer: "Callable[[Any], str] | None" = None,
    enable_uuid_conversion: bool = True,
) -> "dict[str, Any] | None":
    """Coerce Python types to Spanner-compatible formats.

    Handles:
        - UUID → 36-character string (when enable_uuid_conversion is active)
        - bytes → base64-encoded bytes
        - datetime timezone awareness
        - dict → JsonObject for JSON columns
        - nested sequences → JsonObject for JSON arrays
        - TypedParameter ``semantic_name`` FLOAT32 and ARRAY<FLOAT32>/VECTOR coercion

    Args:
        params: Parameter dictionary or None.
        json_serializer: Optional JSON serializer (unused for JSON dicts).
        enable_uuid_conversion: Enable automatic UUID string conversion.

    Returns:
        Coerced parameter dictionary or None.
    """
    if params is None:
        return None

    json_object_type = _get_json_object_type()
    coerced: dict[str, Any] = {}
    changed = False
    for key, raw_value in params.items():
        value = raw_value
        if type(raw_value) is TypedParameter:
            value = raw_value.value
            changed = True
            declared = raw_value.semantic_name
            if declared is not None:
                normalized_declared = declared.strip().upper()
                if normalized_declared in {"ARRAY<FLOAT32>", "VECTOR"} and isinstance(value, (list, tuple)):
                    coerced[key] = [float(item) for item in value]
                    continue
                if normalized_declared == "FLOAT32" and value is not None:
                    coerced[key] = float(value)
                    continue
        if isinstance(value, _UUID_TYPES):
            if enable_uuid_conversion:
                coerced[key] = str(value)
                changed = True
            else:
                coerced[key] = value
        elif isinstance(value, bytes):
            coerced[key] = bytes_to_spanner(value)
            changed = True
        elif isinstance(value, datetime) and value.tzinfo is None:
            coerced[key] = value.replace(tzinfo=timezone.utc)
            changed = True
        elif isinstance(value, timedelta):
            coerced[key] = Interval(days=value.days, nanos=(value.seconds * 1_000_000 + value.microseconds) * 1000)
            changed = True
        elif isinstance(value, json_object_type):
            coerced[key] = value
        elif isinstance(value, dict):
            coerced[key] = spanner_json(value)
            changed = True
        elif isinstance(value, (list, tuple)):
            if any(isinstance(item, timedelta) for item in value):
                coerced[key] = [
                    Interval(days=item.days, nanos=(item.seconds * 1_000_000 + item.microseconds) * 1000)
                    if isinstance(item, timedelta)
                    else item
                    for item in value
                ]
                changed = True
            elif should_json_encode_sequence(value):
                coerced[key] = spanner_json(list(value))
                changed = True
            elif isinstance(value, tuple):
                coerced[key] = list(value)
                changed = True
            else:
                coerced[key] = value
        else:
            coerced[key] = value
    return coerced if changed else params


_NULL_PARAM_TYPE_NAMES: "dict[type[Any], str]" = {
    bool: "BOOL",
    int: "INT64",
    float: "FLOAT64",
    str: "STRING",
    bytes: "BYTES",
    datetime: "TIMESTAMP",
    date: "DATE",
    timedelta: "INTERVAL",
    Decimal: "NUMERIC",
    UUID: "STRING",
    dict: "JSON",
}


def _resolve_declared_param_type(
    raw_value: TypedParameter, param_types_mod: "SpannerParamTypesProtocol"
) -> "Any | None":
    """Resolve a TypedParameter's declared Spanner type.

    ``semantic_name`` names a Spanner type such as ``"FLOAT32"``,
    ``"ARRAY<FLOAT32>"``, ``"VECTOR"``, ``"INTERVAL"``, or ``"JSON"``; without
    one, the declared Python ``original_type`` maps to its Spanner type.

    Args:
        raw_value: The typed parameter.
        param_types_mod: The Spanner param_types module.

    Returns:
        The Spanner param type, or None when nothing is declared.
    """
    semantic_name = raw_value.semantic_name
    if semantic_name is not None:
        normalized = semantic_name.strip().upper()
        if normalized == "FLOAT32":
            return param_types_mod.FLOAT32
        if normalized in {"ARRAY<FLOAT32>", "VECTOR"}:
            return param_types_mod.Array(param_types_mod.FLOAT32)
        if normalized == "INTERVAL":
            return param_types_mod.INTERVAL
        if normalized == "JSON":
            return _json_param_type()
        return getattr(param_types_mod, normalized, None)
    resolver = _NULL_PARAM_TYPE_NAMES.get(raw_value.original_type)
    if resolver == "JSON":
        return _json_param_type()
    return getattr(param_types_mod, resolver, None) if resolver is not None else None


def _infer_sequence_param_type(value: Any, param_types: Any, json_type: Any) -> Any | None:
    """Infer Spanner parameter type for sequence values.

    Args:
        value: Sequence value to inspect.
        param_types: The Spanner param_types module.
        json_type: The Spanner JSON param type.

    Returns:
        Spanner Array type, JSON type, or None if sequence is empty or unhandled.
    """
    if should_json_encode_sequence(value):
        return json_type
    if not value:
        return None
    first = value[0]
    if isinstance(first, bool):
        return param_types.Array(param_types.BOOL)
    if isinstance(first, int):
        return param_types.Array(param_types.INT64)
    if isinstance(first, str):
        return param_types.Array(param_types.STRING)
    if isinstance(first, float):
        return param_types.Array(param_types.FLOAT64)
    if isinstance(first, Decimal):
        return param_types.Array(param_types.NUMERIC)
    if isinstance(first, timedelta):
        return param_types.Array(param_types.INTERVAL)
    return None


def infer_spanner_param_types(params: "dict[str, Any] | None") -> "dict[str, Any]":
    """Infer Spanner param_types from Python values.

    Args:
        params: Parameter dictionary or None.

    Returns:
        Dictionary mapping parameter names to Spanner param_types.
    """
    if not params:
        return {}

    param_types = _get_param_types()
    json_object_type = _get_json_object_type()
    types: dict[str, Any] = {}
    json_type = _json_param_type()
    for key, raw_value in params.items():
        is_typed = type(raw_value) is TypedParameter
        value = raw_value.value if is_typed else raw_value
        if is_typed:
            declared_param_type = _resolve_declared_param_type(raw_value, param_types)
            if declared_param_type is not None:
                types[key] = declared_param_type
                continue
        if value is None:
            null_type = _null_param_type(raw_value, param_types)
            if null_type is not None:
                types[key] = null_type
            continue
        if isinstance(value, bool):
            types[key] = param_types.BOOL
        elif isinstance(value, int):
            types[key] = param_types.INT64
        elif isinstance(value, float):
            types[key] = param_types.FLOAT64
        elif isinstance(value, Decimal):
            types[key] = param_types.NUMERIC
        elif isinstance(value, _STRING_PARAM_TYPES):
            types[key] = param_types.STRING
        elif isinstance(value, bytes):
            types[key] = param_types.BYTES
        elif isinstance(value, datetime):
            types[key] = param_types.TIMESTAMP
        elif isinstance(value, date):
            types[key] = param_types.DATE
        elif isinstance(value, timedelta):
            types[key] = param_types.INTERVAL
        elif isinstance(value, (dict, json_object_type)):
            types[key] = json_type
        elif isinstance(value, (list, tuple)):
            seq_type = _infer_sequence_param_type(value, param_types, json_type)
            if seq_type is not None:
                types[key] = seq_type
    return types


def _null_param_type(raw_value: Any, param_types: "SpannerParamTypesProtocol") -> "Any | None":
    """Resolve the Spanner type for a NULL parameter, when one is declared.

    Spanner offers no ANY type, so a NULL can only be typed from a declared
    Python type on a :class:`TypedParameter`. Without one the entry is omitted
    and Spanner infers the type from the surrounding query, which is what it
    does for an ordinary ``None`` binding.

    Args:
        raw_value: The parameter as supplied, before coercion.
        param_types: The Spanner param_types module.

    Returns:
        The Spanner param type, or None when no type is declared.
    """
    if type(raw_value) is not TypedParameter:
        return None
    return _resolve_declared_param_type(raw_value, param_types)


def _get_param_types() -> "SpannerParamTypesProtocol":
    global _SPANNER_PARAM_TYPES
    if _SPANNER_PARAM_TYPES is None:
        _SPANNER_PARAM_TYPES = cast("SpannerParamTypesProtocol", param_types)
    return _SPANNER_PARAM_TYPES


def _get_json_object_type() -> "type[Any]":
    global _JSON_OBJECT_TYPE
    if _JSON_OBJECT_TYPE is None:
        _JSON_OBJECT_TYPE = JsonObject
    return _JSON_OBJECT_TYPE


def _json_param_type() -> Any:
    """Get Spanner JSON param type with fallback to STRING.

    Returns:
        JSON param type or STRING as fallback.
    """
    param_types = _get_param_types()
    try:
        return param_types.JSON
    except AttributeError:
        return param_types.STRING
