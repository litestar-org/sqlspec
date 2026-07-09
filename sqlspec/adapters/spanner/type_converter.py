"""Spanner type conversion - output and input handling.

Combines output conversion (database results → Python) and input conversion
(Python params → Spanner format) in a single module. Designed for mypyc
compilation with no nested functions.

Output conversion handles:
    - UUID detection and conversion from strings/bytes
    - JSON detection and deserialization

Input conversion handles:
    - UUID → base64-encoded bytes
    - bytes → base64-encoded bytes
    - datetime timezone awareness
    - dict/list → JsonObject wrapping
    - param_types inference
"""

import base64
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from sqlspec.core import TypedParameter
from sqlspec.utils.module_loader import import_optional_attr
from sqlspec.utils.type_converters import should_json_encode_sequence

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

UUID_BYTE_LENGTH: int = 16
_SPANNER_PARAM_TYPES: "SpannerParamTypesProtocol | None" = None
_JSON_OBJECT_TYPE: "type[Any] | None" = None


def _get_param_types() -> "SpannerParamTypesProtocol":
    global _SPANNER_PARAM_TYPES
    if _SPANNER_PARAM_TYPES is None:
        from google.cloud.spanner_v1 import param_types

        _SPANNER_PARAM_TYPES = cast("SpannerParamTypesProtocol", param_types)
    return _SPANNER_PARAM_TYPES


def _get_json_object_type() -> "type[Any]":
    global _JSON_OBJECT_TYPE
    if _JSON_OBJECT_TYPE is None:
        from google.cloud.spanner_v1 import JsonObject

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
        return UUID(bytes=value)
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
    params: "dict[str, Any] | None", json_serializer: "Callable[[Any], str] | None" = None
) -> "dict[str, Any] | None":
    """Coerce Python types to Spanner-compatible formats.

    Handles:
        - UUID → base64-encoded bytes
        - bytes → base64-encoded bytes
        - datetime timezone awareness
        - dict → JsonObject for JSON columns
        - nested sequences → JsonObject for JSON arrays

    Args:
        params: Parameter dictionary or None.
        json_serializer: Optional JSON serializer (unused for JSON dicts).

    Returns:
        Coerced parameter dictionary or None.
    """
    if params is None:
        return None

    json_object_type = _get_json_object_type()
    coerced: dict[str, Any] = {}
    for key, value in params.items():
        if type(value) is TypedParameter:
            value = value.value
        if isinstance(value, _UUID_TYPES):
            std_uuid = value if isinstance(value, UUID) else UUID(bytes=value.bytes)
            coerced[key] = bytes_to_spanner(uuid_to_spanner(std_uuid))
        elif isinstance(value, bytes):
            coerced[key] = bytes_to_spanner(value)
        elif isinstance(value, datetime) and value.tzinfo is None:
            coerced[key] = value.replace(tzinfo=timezone.utc)
        elif isinstance(value, json_object_type):
            coerced[key] = value
        elif isinstance(value, dict):
            coerced[key] = spanner_json(value)
        elif isinstance(value, (list, tuple)):
            if should_json_encode_sequence(value):
                coerced[key] = spanner_json(list(value))
            else:
                coerced[key] = list(value) if isinstance(value, tuple) else value
        else:
            coerced[key] = value
    return coerced


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
    for key, value in params.items():
        if isinstance(value, bool):
            types[key] = param_types.BOOL
        elif isinstance(value, int):
            types[key] = param_types.INT64
        elif isinstance(value, float):
            types[key] = param_types.FLOAT64
        elif isinstance(value, str):
            types[key] = param_types.STRING
        elif isinstance(value, bytes):
            types[key] = param_types.BYTES
        elif isinstance(value, datetime):
            types[key] = param_types.TIMESTAMP
        elif isinstance(value, date):
            types[key] = param_types.DATE
        elif isinstance(value, (dict, json_object_type)):
            types[key] = json_type
        elif isinstance(value, (list, tuple)):
            if should_json_encode_sequence(value):
                types[key] = json_type
                continue
            sequence = list(value)
            if not sequence:
                continue
            first = sequence[0]
            if isinstance(first, int):
                types[key] = param_types.Array(param_types.INT64)
            elif isinstance(first, str):
                types[key] = param_types.Array(param_types.STRING)
            elif isinstance(first, float):
                types[key] = param_types.Array(param_types.FLOAT64)
            elif isinstance(first, bool):
                types[key] = param_types.Array(param_types.BOOL)
    return types
