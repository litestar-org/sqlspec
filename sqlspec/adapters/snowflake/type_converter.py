"""Snowflake scalar and semi-structured output conversion."""

from collections.abc import Callable
from typing import Any, Final

from sqlspec.utils.serializers import from_json

SNOWFLAKE_VARIANT_TYPE_CODES: Final[frozenset[int]] = frozenset({5, 9, 10})

__all__ = ("SNOWFLAKE_VARIANT_TYPE_CODES", "SnowflakeOutputConverter")


class SnowflakeOutputConverter:
    """Decode Snowflake semi-structured values while preserving native scalars."""

    __slots__ = ("_json_deserializer",)

    def __init__(self, json_deserializer: Callable[[str], Any] | None = None) -> None:
        self._json_deserializer = json_deserializer or from_json

    def convert_variant(self, value: Any) -> Any:
        """Decode JSON text, preserving values already decoded by the connector."""
        if isinstance(value, (str, bytes, bytearray)):
            try:
                text = value if isinstance(value, str) else value.decode("utf-8")
                return self._json_deserializer(text.strip())
            except (ValueError, TypeError):
                return value
        return value

    def convert_value(self, value: Any, *, type_code: int | None = None) -> Any:
        """Convert a value according to its Snowflake result type code."""
        return self.convert_variant(value) if type_code in SNOWFLAKE_VARIANT_TYPE_CODES else value
