"""Base classes for adapter type conversion (input and output).

Provides unified base classes for all adapter-specific type converters:
- CachedOutputConverter: For converting database results to Python types (OUTPUT)
- BaseInputConverter: For converting Python params to database format (INPUT)

All classes are designed for mypyc compilation with proper __slots__ and
module-level functions instead of nested closures.
"""

from functools import lru_cache
from typing import TYPE_CHECKING, Any, Final

from mypy_extensions import mypyc_attr

from sqlspec.core.type_conversion import BaseTypeConverter

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ("DEFAULT_CACHE_SIZE", "DEFAULT_SPECIAL_CHARS", "BaseInputConverter", "CachedOutputConverter")

DEFAULT_SPECIAL_CHARS: Final[frozenset[str]] = frozenset({"{", "[", "-", ":", "T", "."})
DEFAULT_CACHE_SIZE: Final[int] = 5000


def _make_cached_converter(
    converter: "CachedOutputConverter", special_chars: frozenset[str], cache_size: int
) -> "Callable[[str], Any]":
    """Create a cached conversion function for an output converter.

    This is a module-level factory function to avoid nested function definitions
    which are problematic for mypyc compilation.

    Args:
        converter: The output converter instance to use for type detection/conversion.
        special_chars: Characters that trigger type detection.
        cache_size: Maximum entries in the LRU cache.

    Returns:
        A cached function that converts string values.
    """

    @lru_cache(maxsize=cache_size)
    def _cached_convert(value: str) -> Any:
        if not value or not any(c in value for c in special_chars):
            return value
        detected_type = converter.detect_type(value)
        if detected_type:
            return converter._convert_detected(value, detected_type)  # pyright: ignore[reportPrivateUsage]
        return value

    return _cached_convert


@mypyc_attr(allow_interpreted_subclasses=True)
class CachedOutputConverter(BaseTypeConverter):
    """Base class for converting database results to Python types.

    Provides LRU-cached string type detection. Subclasses can override
    _convert_detected() for adapter-specific conversion logic.

    This class handles OUTPUT conversion (database -> Python).
    """

    __slots__ = ("_convert_cache", "_special_chars")

    def __init__(self, special_chars: "frozenset[str] | None" = None, cache_size: int = DEFAULT_CACHE_SIZE) -> None:
        """Initialize converter with caching.

        Args:
            special_chars: Characters that trigger type detection.
                          Defaults to DEFAULT_SPECIAL_CHARS.
            cache_size: Maximum entries in LRU cache. Defaults to 5000.
        """
        super().__init__()
        self._special_chars = special_chars if special_chars is not None else DEFAULT_SPECIAL_CHARS
        self._convert_cache = _make_cached_converter(self, self._special_chars, cache_size)

    def _convert_detected(self, value: str, detected_type: str) -> Any:
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

    def convert(self, value: Any) -> Any:
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
    """Base class for converting Python params to database format.

    Subclasses implement adapter-specific parameter coercion.
    This class handles INPUT conversion (Python -> database).
    """

    __slots__ = ()

    def convert_params(self, params: "dict[str, Any] | None") -> "dict[str, Any] | None":
        """Convert parameters for database execution.

        Override in subclasses for adapter-specific coercion.

        Args:
            params: Dictionary of parameters to convert.

        Returns:
            Converted parameters dictionary, or None if input was None.
        """
        return params

    def convert_value(self, value: Any) -> Any:
        """Convert a single parameter value.

        Override in subclasses for adapter-specific value coercion.

        Args:
            value: Value to convert.

        Returns:
            Converted value.
        """
        return value
