"""Centralized type conversion and detection for SQLSpec.

Provides unified type detection and conversion utilities for all database
adapters, with MyPyC-compatible optimizations.
"""

import re
from collections.abc import Callable
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, ClassVar, Final
from uuid import UUID

from sqlspec._serialization import decode_json

# MyPyC-compatible pre-compiled patterns
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


class BaseTypeConverter:
    """Universal type detection and conversion for all adapters.

    Provides centralized type detection and conversion functionality
    that can be used across all database adapters to ensure consistent
    behavior. Users can extend this class for custom type conversion needs.
    """

    __slots__: "ClassVar[tuple[str, ...]]" = ()

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

        return next((k for k, v in match.groupdict().items() if v), None)

    def convert_value(self, value: str, detected_type: str) -> Any:
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

    def convert_if_detected(self, value: Any) -> Any:
        """Convert value only if special type detected, else return original.

        This method provides performance optimization by avoiding expensive
        regex operations on plain strings that don't contain special characters.

        Args:
            value: Value to potentially convert.

        Returns:
            Converted value if special type detected, original value otherwise.

        """
        if not isinstance(value, str):
            return value

        # Quick pre-check for performance - avoid regex on plain strings
        if not any(c in value for c in ["{", "[", "-", ":", "T"]):
            return value  # Skip regex entirely for "hello world" etc.

        detected_type = self.detect_type(value)
        if detected_type:
            try:
                return self.convert_value(value, detected_type)
            except Exception:
                # If conversion fails, return original value
                return value
        return value


def convert_uuid(value: str) -> UUID:
    """Convert UUID string to UUID object.

    Args:
        value: UUID string.

    Returns:
        UUID object.

    """
    return UUID(value)


def convert_iso_datetime(value: str) -> datetime:
    """Convert ISO 8601 datetime string to datetime object.

    Args:
        value: ISO datetime string.

    Returns:
        datetime object.

    """
    # Handle various ISO formats with timezone
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    # Replace space with T for standard ISO format
    if " " in value and "T" not in value:
        value = value.replace(" ", "T")

    return datetime.fromisoformat(value)


def convert_iso_date(value: str) -> date:
    """Convert ISO date string to date object.

    Args:
        value: ISO date string.

    Returns:
        date object.

    """
    return date.fromisoformat(value)


def convert_iso_time(value: str) -> time:
    """Convert ISO time string to time object.

    Args:
        value: ISO time string.

    Returns:
        time object.

    """
    return time.fromisoformat(value)


def convert_json(value: str) -> Any:
    """Convert JSON string to Python object.

    Args:
        value: JSON string.

    Returns:
        Decoded Python object.

    """
    return decode_json(value)


def convert_decimal(value: str) -> Decimal:
    """Convert string to Decimal for precise arithmetic.

    Args:
        value: Decimal string.

    Returns:
        Decimal object.

    """
    return Decimal(value)


# Converter registry
_TYPE_CONVERTERS: Final[dict[str, Callable[[str], Any]]] = {
    "uuid": convert_uuid,
    "iso_datetime": convert_iso_datetime,
    "iso_date": convert_iso_date,
    "iso_time": convert_iso_time,
    "json": convert_json,
}


def format_datetime_rfc3339(dt: datetime) -> str:
    """Format datetime as RFC 3339 compliant string.

    Args:
        dt: datetime object.

    Returns:
        RFC 3339 formatted datetime string.

    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def parse_datetime_rfc3339(dt_str: str) -> datetime:
    """Parse RFC 3339 datetime string.

    Args:
        dt_str: RFC 3339 datetime string.

    Returns:
        datetime object.

    """
    # Handle Z suffix
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    return datetime.fromisoformat(dt_str)


__all__ = (
    "BaseTypeConverter",
    "convert_decimal",
    "convert_iso_date",
    "convert_iso_datetime",
    "convert_iso_time",
    "convert_json",
    "convert_uuid",
    "format_datetime_rfc3339",
    "parse_datetime_rfc3339",
)
