"""SQLite custom type handlers for optional JSON and type conversion support.

Provides registration functions for SQLite's adapter/converter system to enable
custom type handling. All handlers are optional and must be explicitly enabled
via SqliteDriverFeatures configuration.
"""

import json
import sqlite3
from typing import TYPE_CHECKING, Any

from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("json_adapter", "json_converter", "register_type_handlers", "unregister_type_handlers")

logger = get_logger(__name__)

DEFAULT_JSON_TYPE = "JSON"


def json_adapter(value: Any, serializer: "Callable[[Any], str] | None" = None) -> str:
    """Convert Python dict/list to JSON string for SQLite storage.

    Args:
        value: Python dict or list to serialize.
        serializer: Optional JSON serializer callable. Defaults to standard json.dumps.

    Returns:
        JSON string representation.
    """
    if serializer is None:
        return json.dumps(value, ensure_ascii=False)
    return serializer(value)


def json_converter(value: bytes, deserializer: "Callable[[str], Any] | None" = None) -> Any:
    """Convert JSON string from SQLite to Python dict/list.

    Args:
        value: UTF-8 encoded JSON bytes from SQLite.
        deserializer: Optional JSON deserializer callable. Defaults to standard json.loads.

    Returns:
        Deserialized Python object (dict or list).
    """
    if deserializer is None:
        return json.loads(value.decode("utf-8"))
    return deserializer(value.decode("utf-8"))


def register_type_handlers(
    json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> None:
    """Register custom type adapters and converters with sqlite3 module.

    This function registers handlers globally for the sqlite3 module. It should be
    called once during application initialization if custom type handling is needed.

    Args:
        json_serializer: Optional custom JSON serializer (e.g., orjson.dumps).
        json_deserializer: Optional custom JSON deserializer (e.g., orjson.loads).
    """
    sqlite3.register_adapter(dict, lambda v: json_adapter(v, json_serializer))
    sqlite3.register_adapter(list, lambda v: json_adapter(v, json_serializer))

    sqlite3.register_converter(DEFAULT_JSON_TYPE, lambda v: json_converter(v, json_deserializer))

    logger.debug("Registered SQLite custom type handlers (JSON dict/list adapters)")


def unregister_type_handlers() -> None:
    """Unregister custom type handlers from sqlite3 module.

    Note: sqlite3 module does not provide an official unregister API, so this
    function is a no-op placeholder for API consistency with other adapters.
    """
    logger.debug("SQLite type handler unregistration requested (no-op - not supported by sqlite3)")
