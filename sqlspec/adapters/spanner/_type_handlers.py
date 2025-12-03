"""Spanner type handlers for automatic parameter conversion.

Unlike Oracle which has connection-level type handlers, Spanner requires
explicit param_types mapping. This module provides helpers to:
1. Coerce Python types to Spanner-compatible formats
2. Infer param_types from Python values
3. Convert Spanner results back to Python types
"""

import base64
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID

from google.cloud.spanner_v1 import param_types

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = (
    "bytes_to_spanner",
    "coerce_params_for_spanner",
    "infer_spanner_param_types",
    "spanner_to_bytes",
    "spanner_to_uuid",
    "uuid_to_spanner",
)


def bytes_to_spanner(value: "bytes | None") -> "bytes | None":
    """Convert Python bytes to Spanner BYTES format.

    The Spanner Python client requires base64-encoded bytes when
    param_types.BYTES is specified. This function base64-encodes
    raw bytes for storage.

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

    When reading BYTES columns, Spanner may return:
    - Raw bytes (direct access via gRPC)
    - Base64-encoded bytes (the format we stored with bytes_to_spanner)

    This function handles both cases and returns raw Python bytes.

    Args:
        value: Value from Spanner (bytes or None).

    Returns:
        Python bytes or None.
    """
    if value is None:
        return None
    if isinstance(value, bytes | str):
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


UUID_BYTE_LENGTH = 16


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


def coerce_params_for_spanner(
    params: "dict[str, Any] | None", json_serializer: "Callable[[Any], str] | None" = None
) -> "dict[str, Any] | None":
    """Coerce Python types to Spanner-compatible formats.

    Handles:
    - UUID -> base64-encoded bytes (via uuid_to_spanner + bytes_to_spanner)
    - bytes -> base64-encoded bytes (required by Spanner Python client)
    - datetime timezone awareness
    - dict -> JSON string

    Args:
        params: Parameter dictionary or None.
        json_serializer: Optional JSON serializer for dict values.

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
        elif isinstance(value, dict) and json_serializer is not None:
            coerced[key] = json_serializer(value)
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
        elif isinstance(value, dict) and hasattr(param_types, "JSON"):
            types[key] = param_types.JSON
        elif isinstance(value, list):
            if not value:
                continue
            first = value[0]
            if isinstance(first, int):
                types[key] = param_types.Array(param_types.INT64)  # type: ignore[no-untyped-call]
            elif isinstance(first, str):
                types[key] = param_types.Array(param_types.STRING)  # type: ignore[no-untyped-call]
            elif isinstance(first, float):
                types[key] = param_types.Array(param_types.FLOAT64)  # type: ignore[no-untyped-call]
            elif isinstance(first, bool):
                types[key] = param_types.Array(param_types.BOOL)  # type: ignore[no-untyped-call]
    return types
