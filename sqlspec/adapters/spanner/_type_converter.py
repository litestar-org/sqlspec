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
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

from google.cloud.spanner_v1 import JsonObject, param_types

from sqlspec.core._type_converter import CachedOutputConverter
from sqlspec.core.type_conversion import convert_uuid
from sqlspec.protocols import SpannerParamTypesProtocol
from sqlspec.utils.serializers import from_json
from sqlspec.utils.type_converters import should_json_encode_sequence

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "SPANNER_SPECIAL_CHARS",
    "SpannerOutputConverter",
    "bytes_to_spanner",
    "coerce_params_for_spanner",
    "infer_spanner_param_types",
    "spanner_json",
    "spanner_to_bytes",
    "spanner_to_uuid",
    "uuid_to_spanner",
)

SPANNER_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})
SPANNER_PARAM_TYPES: SpannerParamTypesProtocol = cast("SpannerParamTypesProtocol", param_types)
UUID_BYTE_LENGTH: Final[int] = 16


class SpannerOutputConverter(CachedOutputConverter):
    """Spanner-specific output conversion with UUID and JSON support.

    Extends CachedOutputConverter with Spanner-specific functionality
    including UUID bytes handling and JSON deserialization.
    """

    __slots__ = ("_enable_uuid_conversion", "_json_deserializer")

    def __init__(
        self,
        cache_size: int = 5000,
        enable_uuid_conversion: bool = True,
        json_deserializer: "Callable[[str], Any] | None" = None,
    ) -> None:
        """Initialize converter with Spanner-specific options.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
            enable_uuid_conversion: Enable automatic UUID conversion (default: True)
            json_deserializer: Custom JSON deserializer (default: from_json)
        """
        super().__init__(special_chars=SPANNER_SPECIAL_CHARS, cache_size=cache_size)
        self._enable_uuid_conversion = enable_uuid_conversion
        self._json_deserializer = json_deserializer if json_deserializer is not None else from_json

    def _convert_detected(self, value: str, detected_type: str) -> Any:
        """Convert value with Spanner-specific handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value according to Spanner requirements.
        """
        if detected_type == "uuid":
            if not self._enable_uuid_conversion:
                return value
            try:
                return convert_uuid(value)
            except ValueError:
                return value
        if detected_type == "json":
            try:
                return self._json_deserializer(value)
            except (ValueError, TypeError):
                return value
        return value

    def convert(self, value: Any) -> Any:
        """Convert value with Spanner-specific byte UUID handling.

        Args:
            value: Value to potentially convert.

        Returns:
            Converted value or original value.
        """
        if self._enable_uuid_conversion and isinstance(value, bytes) and len(value) == UUID_BYTE_LENGTH:
            try:
                return UUID(bytes=value)
            except ValueError:
                return value

        if not isinstance(value, str):
            return value
        return self._convert_cache(value)


def _json_param_type() -> Any:
    """Get Spanner JSON param type with fallback to STRING.

    Returns:
        JSON param type or STRING as fallback.
    """
    try:
        return SPANNER_PARAM_TYPES.JSON
    except AttributeError:
        return SPANNER_PARAM_TYPES.STRING


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
    if isinstance(value, JsonObject):
        return value
    return JsonObject(value)  # type: ignore[no-untyped-call]


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

    coerced: dict[str, Any] = {}
    for key, value in params.items():
        if isinstance(value, UUID):
            coerced[key] = bytes_to_spanner(uuid_to_spanner(value))
        elif isinstance(value, bytes):
            coerced[key] = bytes_to_spanner(value)
        elif isinstance(value, datetime) and value.tzinfo is None:
            coerced[key] = value.replace(tzinfo=timezone.utc)
        elif isinstance(value, JsonObject):
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

    types: dict[str, Any] = {}
    json_type = _json_param_type()
    for key, value in params.items():
        if isinstance(value, bool):
            types[key] = SPANNER_PARAM_TYPES.BOOL
        elif isinstance(value, int):
            types[key] = SPANNER_PARAM_TYPES.INT64
        elif isinstance(value, float):
            types[key] = SPANNER_PARAM_TYPES.FLOAT64
        elif isinstance(value, str):
            types[key] = SPANNER_PARAM_TYPES.STRING
        elif isinstance(value, bytes):
            types[key] = SPANNER_PARAM_TYPES.BYTES
        elif isinstance(value, datetime):
            types[key] = SPANNER_PARAM_TYPES.TIMESTAMP
        elif isinstance(value, date):
            types[key] = SPANNER_PARAM_TYPES.DATE
        elif isinstance(value, (dict, JsonObject)):
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
                types[key] = SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.INT64)
            elif isinstance(first, str):
                types[key] = SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.STRING)
            elif isinstance(first, float):
                types[key] = SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.FLOAT64)
            elif isinstance(first, bool):
                types[key] = SPANNER_PARAM_TYPES.Array(SPANNER_PARAM_TYPES.BOOL)
    return types
