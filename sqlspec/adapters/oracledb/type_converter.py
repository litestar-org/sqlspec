"""Oracle-specific type conversion with LOB optimization.

Provides specialized type handling for Oracle databases, including
efficient LOB (Large Object) processing and JSON storage detection.
"""

import array
import re
from datetime import datetime
from typing import Any, Final

from sqlspec.core.type_converter import CachedOutputConverter
from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.sync_tools import ensure_async_
from sqlspec.utils.type_guards import is_readable

__all__ = ("ORACLE_JSON_STORAGE_REGEX", "ORACLE_SPECIAL_CHARS", "OracleOutputConverter")

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


class OracleOutputConverter(CachedOutputConverter):
    """Oracle-specific output conversion with LOB optimization.

    Extends CachedOutputConverter with Oracle-specific functionality
    including streaming LOB support and JSON storage type detection.
    """

    __slots__ = ()

    def __init__(self, cache_size: int = 5000) -> None:
        """Initialize converter with Oracle-specific options.

        Args:
            cache_size: Maximum number of string values to cache (default: 5000)
        """
        super().__init__(special_chars=ORACLE_SPECIAL_CHARS, cache_size=cache_size)

    def _convert_detected(self, value: str, detected_type: str) -> Any:
        """Convert value with Oracle-specific handling.

        Args:
            value: String value to convert.
            detected_type: Detected type name.

        Returns:
            Converted value, or original on failure.
        """
        try:
            return self.convert_value(value, detected_type)
        except Exception:
            return value

    async def process_lob(self, value: Any) -> Any:
        """Process Oracle LOB objects efficiently.

        Args:
            value: Potential LOB object or regular value.

        Returns:
            LOB content if value is a LOB, original value otherwise.
        """
        if not is_readable(value):
            return value

        read_func = ensure_async_(value.read)
        return await read_func()

    def detect_json_storage_type(self, column_info: "dict[str, Any]") -> bool:
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
        if not is_readable(lob_obj):
            return lob_obj if isinstance(lob_obj, bytes) else str(lob_obj).encode("utf-8")

        first_chunk = lob_obj.read(chunk_size)
        if not first_chunk:
            return b""

        if isinstance(first_chunk, bytes):
            chunks: list[bytes] = [first_chunk]
            while True:
                chunk = lob_obj.read(chunk_size)
                if not chunk:
                    break
                if isinstance(chunk, bytes):
                    chunks.append(chunk)
                else:
                    chunks.append(str(chunk).encode("utf-8"))
            return b"".join(chunks)

        text_chunks: list[str] = [str(first_chunk)]
        while True:
            chunk = lob_obj.read(chunk_size)
            if not chunk:
                break
            text_chunks.append(str(chunk))
        return "".join(text_chunks).encode("utf-8")

    def convert_oracle_value(self, value: Any, column_info: "dict[str, Any]") -> Any:
        """Convert Oracle-specific value with column context.

        Args:
            value: Value to convert.
            column_info: Column metadata for context.

        Returns:
            Converted value appropriate for the column type.
        """
        if is_readable(value):
            if self.detect_json_storage_type(column_info):
                content = self.handle_large_lob(value)
                content_str = content.decode("utf-8") if isinstance(content, bytes) else content
                return self.convert(content_str)
            return self.handle_large_lob(value)

        if isinstance(value, str):
            return self.convert(value)

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
            from sqlspec.adapters.oracledb._numpy_handlers import (  # pyright: ignore[reportPrivateUsage]
                numpy_converter_out,
            )

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
            from sqlspec.adapters.oracledb._numpy_handlers import (  # pyright: ignore[reportPrivateUsage]
                numpy_converter_in,
            )

            return numpy_converter_in(value)

        return value
