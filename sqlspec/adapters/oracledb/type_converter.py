"""Oracle-specific type conversion with LOB optimization.

Provides specialized type handling for Oracle databases, including
efficient LOB (Large Object) processing and JSON storage detection.
"""

import array
import re
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
            from sqlspec.adapters.oracledb._vector_handlers import (  # pyright: ignore[reportPrivateUsage]
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
            from sqlspec.adapters.oracledb._vector_handlers import (  # pyright: ignore[reportPrivateUsage]
                numpy_converter_in,
            )

            return numpy_converter_in(value)

        return value
