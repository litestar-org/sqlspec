from collections.abc import Callable
from typing import Any
from uuid import UUID

from sqlspec.utils.serializers import from_json


class SpannerTypeConverter:
    """Type conversion for Spanner-specific types."""

    def __init__(
        self,
        enable_uuid_conversion: bool = True,
        json_deserializer: Callable[[str], Any] = from_json,
    ) -> None:
        self.enable_uuid_conversion = enable_uuid_conversion
        self.json_deserializer = json_deserializer

    def convert_if_detected(self, value: Any) -> Any:
        """Auto-detect and convert UUID, JSON strings."""
        if isinstance(value, str):
            if self.enable_uuid_conversion:
                try:
                    return UUID(value)
                except ValueError:
                    pass

            # Basic JSON detection (heuristic)
            stripped = value.strip()
            if (stripped.startswith("{") and stripped.endswith("}")) or (
                stripped.startswith("[") and stripped.endswith("]")
            ):
                try:
                    return self.json_deserializer(value)
                except (ValueError, TypeError):
                    pass

        return value
