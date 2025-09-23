"""Oracle-specific type conversion with LOB optimization.

Provides specialized type handling for Oracle databases, including
efficient LOB (Large Object) processing and JSON storage detection.
"""

import re
from datetime import datetime
from typing import Any, Final

from sqlspec.core.type_conversion import BaseTypeConverter
from sqlspec.utils.sync_tools import ensure_async_

# Oracle-specific JSON storage detection
ORACLE_JSON_STORAGE_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<json_type>JSON)|"
    r"(?P<blob_oson>BLOB.*OSON)|"
    r"(?P<blob_json>BLOB.*JSON)|"
    r"(?P<clob_json>CLOB.*JSON)"
    r")$",
    re.IGNORECASE,
)


class OracleTypeConverter(BaseTypeConverter):
    """Oracle-specific type conversion with LOB optimization.

    Extends the base TypeDetector with Oracle-specific functionality
    including streaming LOB support and JSON storage type detection.
    """

    __slots__ = ()

    async def process_lob(self, value: Any) -> Any:
        """Process Oracle LOB objects efficiently.

        Args:
            value: Potential LOB object or regular value.

        Returns:
            LOB content if value is a LOB, original value otherwise.
        """
        if not hasattr(value, "read"):
            return value

        # Use ensure_async_ for unified sync/async handling
        read_func = ensure_async_(value.read)
        return await read_func()

    def detect_json_storage_type(self, column_info: dict[str, Any]) -> bool:
        """Detect if column stores JSON data.

        Args:
            column_info: Database column metadata.

        Returns:
            True if column is configured for JSON storage.
        """
        type_name = column_info.get("type_name", "").upper()
        return bool(ORACLE_JSON_STORAGE_REGEX.match(type_name))

    def format_datetime_for_oracle(self, dt: datetime) -> str:
        """Format datetime for Oracle TO_DATE function.

        Args:
            dt: datetime object to format.

        Returns:
            Oracle TO_DATE SQL expression.
        """
        return f"TO_DATE('{dt.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"

    def handle_large_lob(self, lob_obj: Any, chunk_size: int = 1024 * 1024) -> bytes:
        """Handle large LOB objects with streaming.

        Args:
            lob_obj: Oracle LOB object.
            chunk_size: Size of chunks to read at a time.

        Returns:
            Complete LOB content as bytes.
        """
        if not hasattr(lob_obj, "read"):
            return lob_obj if isinstance(lob_obj, bytes) else str(lob_obj).encode("utf-8")

        chunks = []
        while True:
            chunk = lob_obj.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)

        if not chunks:
            return b""

        return b"".join(chunks) if isinstance(chunks[0], bytes) else "".join(chunks).encode("utf-8")

    def convert_oracle_value(self, value: Any, column_info: dict[str, Any]) -> Any:
        """Convert Oracle-specific value with column context.

        Args:
            value: Value to convert.
            column_info: Column metadata for context.

        Returns:
            Converted value appropriate for the column type.
        """
        # Handle LOB objects
        if hasattr(value, "read"):
            if self.detect_json_storage_type(column_info):
                # For JSON storage types, decode the LOB content
                content = self.handle_large_lob(value)
                content_str = content.decode("utf-8") if isinstance(content, bytes) else content
                # Try to parse as JSON
                detected_type = self.detect_type(content_str)
                if detected_type == "json":
                    return self.convert_value(content_str, detected_type)
                return content_str
            # For other LOB types, return raw content
            return self.handle_large_lob(value)

        # Use base type detection for non-LOB values
        if isinstance(value, str):
            detected_type = self.detect_type(value)
            if detected_type:
                return self.convert_value(value, detected_type)

        return value


__all__ = ("ORACLE_JSON_STORAGE_REGEX", "OracleTypeConverter")
