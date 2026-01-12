"""Enhanced serialization module with byte-aware encoding and class-based architecture.

Provides a Protocol-based serialization system that users can extend.
Supports msgspec, orjson, and standard library JSON with automatic fallback.

Features optional numpy array serialization when numpy is installed.
Arrays are automatically converted to lists during JSON encoding.
"""

import contextlib
import datetime
import enum
import json
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any, Final, Literal, Protocol, overload

from sqlspec._typing import NUMPY_INSTALLED
from sqlspec.core.filters import OffsetPagination
from sqlspec.typing import MSGSPEC_INSTALLED, ORJSON_INSTALLED, PYDANTIC_INSTALLED, BaseModel


def _type_to_string(value: Any) -> Any:  # pragma: no cover
    """Convert special types to strings for JSON serialization.

    Handles datetime, date, enums, Decimal, Pydantic models, and numpy arrays.

    Args:
        value: Value to convert.

    Returns:
        Serializable representation of the value (string, list, dict, etc.).

    Raises:
        TypeError: If value cannot be serialized.
    """
    if isinstance(value, datetime.datetime):
        return convert_datetime_to_gmt_iso(value)
    if isinstance(value, datetime.date):
        return convert_date_to_iso(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, enum.Enum):
        return str(value.value)
    if PYDANTIC_INSTALLED and isinstance(value, BaseModel):
        return value.model_dump_json()
    if isinstance(value, OffsetPagination):
        return {"items": value.items, "limit": value.limit, "offset": value.offset, "total": value.total}
    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(value, np.ndarray):
            return value.tolist()
    try:
        return str(value)
    except Exception as exc:
        msg = f"Cannot serialize {type(value).__name__}"
        raise TypeError(msg) from exc


class JSONSerializer(Protocol):
    """Protocol for JSON serialization implementations.

    Users can implement this protocol to create custom serializers.
    """

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data to JSON.

        Args:
            data: Data to encode.
            as_bytes: Whether to return bytes instead of string.

        Returns:
            JSON string or bytes depending on as_bytes parameter.
        """
        ...

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode from JSON.

        Args:
            data: JSON string or bytes to decode.
            decode_bytes: Whether to decode bytes input.

        Returns:
            Decoded Python object.
        """
        ...


class BaseJSONSerializer(ABC):
    """Base class for JSON serializers with common functionality."""

    __slots__ = ()

    @abstractmethod
    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data to JSON."""
        ...

    @abstractmethod
    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode from JSON."""
        ...


class MsgspecSerializer(BaseJSONSerializer):
    """Msgspec-based JSON serializer."""

    __slots__ = ("_decoder", "_encoder")

    def __init__(self) -> None:
        """Initialize msgspec encoder and decoder."""
        from msgspec.json import Decoder, Encoder

        self._encoder: Final[Encoder] = Encoder(enc_hook=_type_to_string)
        self._decoder: Final[Decoder] = Decoder()

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data using msgspec."""
        try:
            if as_bytes:
                return self._encoder.encode(data)
            return self._encoder.encode(data).decode("utf-8")
        except (TypeError, ValueError):
            if ORJSON_INSTALLED:
                return OrjsonSerializer().encode(data, as_bytes=as_bytes)
            return StandardLibSerializer().encode(data, as_bytes=as_bytes)

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode data using msgspec."""
        if isinstance(data, bytes):
            if decode_bytes:
                try:
                    return self._decoder.decode(data)
                except (TypeError, ValueError):
                    if ORJSON_INSTALLED:
                        return OrjsonSerializer().decode(data, decode_bytes=decode_bytes)
                    return StandardLibSerializer().decode(data, decode_bytes=decode_bytes)
            return data

        try:
            return self._decoder.decode(data.encode("utf-8"))
        except (TypeError, ValueError):
            if ORJSON_INSTALLED:
                return OrjsonSerializer().decode(data, decode_bytes=decode_bytes)
            return StandardLibSerializer().decode(data, decode_bytes=decode_bytes)


class OrjsonSerializer(BaseJSONSerializer):
    """Orjson-based JSON serializer with native datetime/UUID support.

    Automatically enables numpy serialization if numpy is installed.
    """

    __slots__ = ()

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data using orjson.

        Args:
            data: Data to encode.
            as_bytes: Whether to return bytes instead of string.

        Returns:
            JSON string or bytes depending on as_bytes parameter.
        """
        from orjson import (
            OPT_NAIVE_UTC,  # pyright: ignore[reportUnknownVariableType]
            OPT_SERIALIZE_UUID,  # pyright: ignore[reportUnknownVariableType]
        )
        from orjson import dumps as _orjson_dumps  # pyright: ignore[reportMissingImports]

        options = OPT_NAIVE_UTC | OPT_SERIALIZE_UUID

        if NUMPY_INSTALLED:
            from orjson import OPT_SERIALIZE_NUMPY  # pyright: ignore[reportUnknownVariableType]

            options |= OPT_SERIALIZE_NUMPY

        result = _orjson_dumps(data, default=_type_to_string, option=options)
        return result if as_bytes else result.decode("utf-8")

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode data using orjson."""
        from orjson import loads as _orjson_loads  # pyright: ignore[reportMissingImports]

        if isinstance(data, bytes):
            if decode_bytes:
                return _orjson_loads(data)
            return data
        return _orjson_loads(data)


class StandardLibSerializer(BaseJSONSerializer):
    """Standard library JSON serializer as fallback."""

    __slots__ = ()

    def encode(self, data: Any, *, as_bytes: bool = False) -> str | bytes:
        """Encode data using standard library json."""
        json_str = json.dumps(data, default=_type_to_string)
        return json_str.encode("utf-8") if as_bytes else json_str

    def decode(self, data: str | bytes, *, decode_bytes: bool = True) -> Any:
        """Decode data using standard library json."""
        if isinstance(data, bytes):
            if decode_bytes:
                return json.loads(data.decode("utf-8"))
            return data
        return json.loads(data)


_default_serializer: JSONSerializer | None = None


def get_default_serializer() -> JSONSerializer:
    """Get the default serializer based on available libraries.

    Priority: msgspec > orjson > stdlib

    Returns:
        The best available JSON serializer.
    """
    global _default_serializer

    if _default_serializer is None:
        if MSGSPEC_INSTALLED:
            with contextlib.suppress(ImportError):
                _default_serializer = MsgspecSerializer()

        if _default_serializer is None and ORJSON_INSTALLED:
            with contextlib.suppress(ImportError):
                _default_serializer = OrjsonSerializer()

        if _default_serializer is None:
            _default_serializer = StandardLibSerializer()

    assert _default_serializer is not None
    return _default_serializer


@overload
def encode_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...  # pragma: no cover


@overload
def encode_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...  # pragma: no cover


def encode_json(data: Any, *, as_bytes: bool = False) -> str | bytes:
    """Encode to JSON, optionally returning bytes.

    Args:
        data: The data to encode.
        as_bytes: Whether to return bytes instead of string.

    Returns:
        JSON string or bytes depending on as_bytes parameter.
    """
    return get_default_serializer().encode(data, as_bytes=as_bytes)


def decode_json(data: str | bytes, *, decode_bytes: bool = True) -> Any:
    """Decode from JSON string or bytes efficiently.

    Args:
        data: JSON string or bytes to decode.
        decode_bytes: Whether to decode bytes input.

    Returns:
        Decoded Python object.
    """
    return get_default_serializer().decode(data, decode_bytes=decode_bytes)


def convert_datetime_to_gmt_iso(dt: datetime.datetime) -> str:  # pragma: no cover
    """Handle datetime serialization for nested timestamps.

    Args:
        dt: The datetime to convert.

    Returns:
        The ISO formatted datetime string.
    """
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def convert_date_to_iso(dt: datetime.date) -> str:  # pragma: no cover
    """Handle datetime serialization for nested timestamps.

    Args:
        dt: The date to convert.

    Returns:
        The ISO formatted date string.
    """
    return dt.isoformat()


__all__ = (
    "BaseJSONSerializer",
    "JSONSerializer",
    "MsgspecSerializer",
    "OrjsonSerializer",
    "StandardLibSerializer",
    "convert_date_to_iso",
    "convert_datetime_to_gmt_iso",
    "decode_json",
    "encode_json",
    "get_default_serializer",
)
