"""Base classes and detection for adapter type conversion."""

import re
from collections.abc import Callable
from datetime import date, datetime, time, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Any, Final
from uuid import UUID

from mypy_extensions import mypyc_attr

from sqlspec._serialization import decode_json

__all__ = (
    "DEFAULT_CACHE_SIZE",
    "DEFAULT_SPECIAL_CHARS",
    "BaseInputConverter",
    "BaseTypeConverter",
    "CachedOutputConverter",
    "convert_decimal",
    "convert_iso_date",
    "convert_iso_datetime",
    "convert_iso_time",
    "convert_json",
    "convert_uuid",
    "format_datetime_rfc3339",
    "parse_datetime_rfc3339",
)

DEFAULT_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})
DEFAULT_CACHE_SIZE: Final[int] = 5000
DEFAULT_DETECTION_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T"})

SPECIAL_TYPE_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<uuid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})|"
    r"(?P<iso_datetime>\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?)|"
    r"(?P<iso_date>\d{4}-\d{2}-\d{2})|"
    r"(?P<iso_time>\d{2}:\d{2}:\d{2}(?:\.\d+)?)|"
    r"(?P<json>[\[{].*[\]}])|"
    r"(?P<ipv4>(?:\d{1,3}\.){3}\d{1,3})|"
    r"(?P<ipv6>(?:[0-9a-f]{1,4}:){7}[0-9a-f]{1,4})|"
    r"(?P<mac>(?:[0-9a-f]{2}:){5}[0-9a-f]{2})"
    r")$",
    re.IGNORECASE | re.DOTALL,
)


def convert_uuid(value: str) -> "UUID":
    """Convert UUID string to UUID object.

    Args:
        value: UUID string.

    Returns:
        UUID object.
    """
    return UUID(value)


def convert_iso_datetime(value: str) -> "datetime":
    """Convert ISO 8601 datetime string to datetime object.

    Args:
        value: ISO datetime string.

    Returns:
        datetime object.
    """
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    if " " in value and "T" not in value:
        value = value.replace(" ", "T")

    return datetime.fromisoformat(value)


def convert_iso_date(value: str) -> "date":
    """Convert ISO date string to date object.

    Args:
        value: ISO date string.

    Returns:
        date object.
    """
    return date.fromisoformat(value)


def convert_iso_time(value: str) -> "time":
    """Convert ISO time string to time object.

    Args:
        value: ISO time string.

    Returns:
        time object.
    """
    return time.fromisoformat(value)


def convert_json(value: str) -> "Any":
    """Convert JSON string to Python object.

    Args:
        value: JSON string.

    Returns:
        Decoded Python object.
    """
    return decode_json(value)


def convert_decimal(value: str) -> "Decimal":
    """Convert string to Decimal for precise arithmetic.

    Args:
        value: Decimal string.

    Returns:
        Decimal object.
    """
    return Decimal(value)


def format_datetime_rfc3339(dt: "datetime") -> str:
    """Format datetime as RFC 3339 compliant string.

    Args:
        dt: datetime object.

    Returns:
        RFC 3339 formatted datetime string.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def parse_datetime_rfc3339(dt_str: str) -> "datetime":
    """Parse RFC 3339 datetime string.

    Args:
        dt_str: RFC 3339 datetime string.

    Returns:
        datetime object.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.fromisoformat(dt_str)


_TYPE_CONVERTERS: Final[dict[str, Callable[[str], Any]]] = {
    "uuid": convert_uuid,
    "iso_datetime": convert_iso_datetime,
    "iso_date": convert_iso_date,
    "iso_time": convert_iso_time,
    "json": convert_json,
}


class _CachedConverter:
    __slots__ = ("_cached", "_converter", "_special_chars")

    def __init__(self, converter: "CachedOutputConverter", special_chars: "frozenset[str]", cache_size: int) -> None:
        self._converter = converter
        self._special_chars = special_chars
        self._cached = lru_cache(maxsize=cache_size)(self._convert)

    def _convert(self, value: str) -> "Any":
        if not value or not any(c in value for c in self._special_chars):
            return value
        detected_type = self._converter.detect_type(value)
        if detected_type:
            return self._converter._convert_detected(value, detected_type)  # pyright: ignore[reportPrivateUsage]
        return value

    def __call__(self, value: str) -> "Any":
        return self._cached(value)


def _make_cached_converter(
    converter: "CachedOutputConverter", special_chars: "frozenset[str]", cache_size: int
) -> "Callable[[str], Any]":
    """Create a cached conversion function for an output converter.

    Args:
        converter: The output converter instance to use for type detection/conversion.
        special_chars: Characters that trigger type detection.
        cache_size: Maximum entries in the LRU cache.

    Returns:
        A cached function that converts string values.
    """

    return _CachedConverter(converter, special_chars, cache_size)


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseTypeConverter:
    """Universal type detection and conversion for all adapters."""

    __slots__ = ()

    def detect_type(self, value: str) -> str | None:
        """Detect special types from string values.

        Args:
            value: String value to analyze.

        Returns:
            Type name if detected, None otherwise.
        """
        if not isinstance(value, str):  # pyright: ignore
            return None
        if not value:
            return None

        match = SPECIAL_TYPE_REGEX.match(value)
        if not match:
            return None

        return next((key for key, match_value in match.groupdict().items() if match_value), None)

    def convert_value(self, value: str, detected_type: str) -> "Any":
        """Convert string value to appropriate Python type.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value in appropriate Python type.
        """
        converter = _TYPE_CONVERTERS.get(detected_type)
        if converter:
            return converter(value)
        return value

    def convert_if_detected(self, value: "Any") -> "Any":
        """Convert value only if special type detected, else return original.

        Args:
            value: Value to potentially convert.

        Returns:
            Converted value if special type detected, original value otherwise.
        """
        if not isinstance(value, str):
            return value

        if not any(c in value for c in DEFAULT_DETECTION_CHARS):
            return value

        detected_type = self.detect_type(value)
        if detected_type:
            try:
                return self.convert_value(value, detected_type)
            except Exception:
                return value
        return value


@mypyc_attr(allow_interpreted_subclasses=True)
class CachedOutputConverter(BaseTypeConverter):
    """Base class for converting database results to Python types."""

    __slots__ = ("_convert_cache", "_special_chars")

    def __init__(self, special_chars: "frozenset[str] | None" = None, cache_size: int = DEFAULT_CACHE_SIZE) -> None:
        """Initialize converter with caching.

        Args:
            special_chars: Characters that trigger type detection.
            cache_size: Maximum entries in LRU cache.
        """
        super().__init__()
        self._special_chars = special_chars if special_chars is not None else DEFAULT_SPECIAL_CHARS
        self._convert_cache = _make_cached_converter(self, self._special_chars, cache_size)

    def _convert_detected(self, value: str, detected_type: str) -> "Any":
        """Convert value with detected type. Override for adapter-specific logic.

        Args:
            value: String value to convert.
            detected_type: Detected type name from detect_type().

        Returns:
            Converted value, or original value on conversion failure.
        """
        try:
            return self.convert_value(value, detected_type)
        except Exception:
            return value

    def convert(self, value: "Any") -> "Any":
        """Convert value using cached detection and conversion.

        Args:
            value: Value to potentially convert.

        Returns:
            Converted value if string with special type, original otherwise.
        """
        if not isinstance(value, str):
            return value
        return self._convert_cache(value)


@mypyc_attr(allow_interpreted_subclasses=True)
class BaseInputConverter:
    """Base class for converting Python params to database format."""

    __slots__ = ()

    def convert_params(self, params: "dict[str, Any] | None") -> "dict[str, Any] | None":
        """Convert parameters for database execution.

        Args:
            params: Dictionary of parameters to convert.

        Returns:
            Converted parameters dictionary, or None if input was None.
        """
        return params

    def convert_value(self, value: "Any") -> "Any":
        """Convert a single parameter value.

        Args:
            value: Value to convert.

        Returns:
            Converted value.
        """
        return value
