"""SQLite custom type handlers for optional JSON and type conversion support.

Provides registration functions for SQLite's adapter/converter system to enable
custom type handling. All handlers are optional and must be explicitly enabled
via SqliteDriverFeatures configuration.

All functions are designed for mypyc compilation using functools.partial
instead of lambdas for adapter registration.
"""

import json
import sqlite3
from functools import partial
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


def _make_json_adapter(serializer: "Callable[[Any], str] | None") -> "Callable[[Any], str]":
    """Create a JSON adapter function with bound serializer.

    This is a module-level factory to avoid lambda closures which are
    problematic for mypyc compilation.

    Args:
        serializer: Optional JSON serializer callable.

    Returns:
        Adapter function ready for sqlite3.register_adapter.
    """
    return partial(json_adapter, serializer=serializer)


def _make_json_converter(deserializer: "Callable[[str], Any] | None") -> "Callable[[bytes], Any]":
    """Create a JSON converter function with bound deserializer.

    This is a module-level factory to avoid lambda closures which are
    problematic for mypyc compilation.

    Args:
        deserializer: Optional JSON deserializer callable.

    Returns:
        Converter function ready for sqlite3.register_converter.
    """
    return partial(json_converter, deserializer=deserializer)


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
    dict_adapter = _make_json_adapter(json_serializer)
    list_adapter = _make_json_adapter(json_serializer)
    converter = _make_json_converter(json_deserializer)

    sqlite3.register_adapter(dict, dict_adapter)
    sqlite3.register_adapter(list, list_adapter)
    sqlite3.register_converter(DEFAULT_JSON_TYPE, converter)


def unregister_type_handlers() -> None:
    """Unregister custom type handlers from sqlite3 module.

    Note: sqlite3 module does not provide an official unregister API, so this
    function is a no-op placeholder for API consistency with other adapters.
    """
