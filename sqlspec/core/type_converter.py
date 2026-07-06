"""Base classes and scalar helpers for adapter type conversion."""

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from uuid import UUID

from mypy_extensions import mypyc_attr

__all__ = (
    "BaseInputConverter",
    "convert_decimal",
    "convert_iso_date",
    "convert_iso_datetime",
    "convert_iso_time",
    "convert_json",
    "convert_uuid",
    "format_datetime_rfc3339",
    "parse_datetime_rfc3339",
)


def convert_uuid(value: str) -> UUID:
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
    return json.loads(value)


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
    return convert_iso_datetime(dt_str)


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
