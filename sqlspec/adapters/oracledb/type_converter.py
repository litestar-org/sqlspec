"""Oracle-specific type conversion with LOB optimization.

Provides specialized type handling for Oracle databases, including
efficient LOB (Large Object) processing and JSON storage detection.
"""

import array
import re
from datetime import datetime
from functools import lru_cache
from typing import Any, Final

from sqlspec.core.type_conversion import BaseTypeConverter
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.sync_tools import ensure_async_

ORACLE_JSON_STORAGE_REGEX: Final[re.Pattern[str]] = re.compile(
    r"^(?:"
    r"(?P<json_type>JSON)|"
    r"(?P<blob_oson>BLOB.*OSON)|"
    r"(?P<blob_json>BLOB.*JSON)|"
    r"(?P<clob_json>CLOB.*JSON)"
    r")$",
    re.IGNORECASE,
)

ORACLE_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})


class OracleTypeConverter(BaseTypeConverter):
    """Oracle-specific type conversion with LOB optimization.

    Extends the base TypeDetector with Oracle-specific functionality
    including streaming LOB support and JSON storage type detection.
    Includes per-instance LRU cache for improved performance.
    """

    __slots__ = ("_convert_cache",)

    def __init__(self, cache_size: int = 5000) -> None:
        """Initialize converter with per-instance conversion cache.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
        """
        super().__init__()

        @lru_cache(maxsize=cache_size)
        def _cached_convert(value: str) -> Any:
            if not value or not any(c in value for c in ORACLE_SPECIAL_CHARS):
                return value
            detected_type = self.detect_type(value)
            if detected_type:
                return self.convert_value(value, detected_type)
            return value

        self._convert_cache = _cached_convert

    def convert_if_detected(self, value: Any) -> Any:
        """Convert string if special type detected (cached).

        Args:
            value: Value to potentially convert

        Returns:
            Converted value or original value
        """
        if not isinstance(value, str):
            return value
        return self._convert_cache(value)

    async def process_lob(self, value: Any) -> Any:
        """Process Oracle LOB objects efficiently.

        Args:
            value: Potential LOB object or regular value.

        Returns:
            LOB content if value is a LOB, original value otherwise.
        """
        if not hasattr(value, "read"):
            return value

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
        if hasattr(value, "read"):
            if self.detect_json_storage_type(column_info):
                content = self.handle_large_lob(value)
                content_str = content.decode("utf-8") if isinstance(content, bytes) else content
                return self.convert_if_detected(content_str)
            return self.handle_large_lob(value)

        if isinstance(value, str):
            return self.convert_if_detected(value)

        return value

    def convert_vector_to_numpy(self, value: Any) -> Any:
        """Convert Oracle VECTOR to NumPy array.

        Provides manual conversion API for users who need explicit control
        over vector transformations or have disabled automatic handlers.

        Args:
            value: Oracle VECTOR value (array.array) or other value.

        Returns:
            NumPy ndarray if value is array.array and NumPy is installed,
            otherwise original value.
        """
        if not NUMPY_INSTALLED:
            return value

        if isinstance(value, array.array):
            from sqlspec.adapters.oracledb._numpy_handlers import numpy_converter_out

            return numpy_converter_out(value)

        return value

    def convert_numpy_to_vector(self, value: Any) -> Any:
        """Convert NumPy array to Oracle VECTOR format.

        Provides manual conversion API for users who need explicit control
        over vector transformations or have disabled automatic handlers.

        Args:
            value: NumPy ndarray or other value.

        Returns:
            array.array compatible with Oracle VECTOR if value is ndarray,
            otherwise original value.

        """
        if not NUMPY_INSTALLED:
            return value

        import numpy as np

        if isinstance(value, np.ndarray):
            from sqlspec.adapters.oracledb._numpy_handlers import numpy_converter_in

            return numpy_converter_in(value)

        return value


__all__ = ("ORACLE_JSON_STORAGE_REGEX", "ORACLE_SPECIAL_CHARS", "OracleTypeConverter")
