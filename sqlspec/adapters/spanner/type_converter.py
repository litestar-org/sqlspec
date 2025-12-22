from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final
from uuid import UUID

from sqlspec.core import BaseTypeConverter, convert_uuid
from sqlspec.utils.serializers import from_json

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("SpannerTypeConverter",)

SPANNER_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})


class SpannerTypeConverter(BaseTypeConverter):
    """Type conversion for Spanner-specific types."""

    __slots__ = ("_convert_cache", "_enable_uuid_conversion", "_json_deserializer")

    def __init__(
        self, enable_uuid_conversion: "bool" = True, json_deserializer: "Callable[[str], Any]" = from_json
    ) -> None:
        super().__init__()
        self._enable_uuid_conversion = enable_uuid_conversion
        self._json_deserializer = json_deserializer

        @lru_cache(maxsize=5000)
        def _cached_convert(value: str) -> Any:
            if not value or not any(c in value for c in SPANNER_SPECIAL_CHARS):
                return value
            detected_type = self.detect_type(value)
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

        self._convert_cache = _cached_convert

    def convert_if_detected(self, value: Any) -> Any:
        """Auto-detect and convert UUID and JSON strings."""
        uuid_byte_length = 16
        if self._enable_uuid_conversion and isinstance(value, bytes) and len(value) == uuid_byte_length:
            try:
                return UUID(bytes=value)
            except ValueError:
                return value

        if not isinstance(value, str):
            return value
        return self._convert_cache(value)
