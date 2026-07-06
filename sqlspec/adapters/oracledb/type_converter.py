"""Oracle-specific type conversion with LOB optimization.

Provides specialized type handling for Oracle databases, including
efficient LOB (Large Object) processing and vector helpers.
"""

import array
from typing import Any

from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.sync_tools import ensure_async_
from sqlspec.utils.type_guards import is_readable

__all__ = ("OracleOutputConverter",)


class OracleOutputConverter:
    """Oracle-specific output conversion with LOB optimization.

    Provides streaming LOB support and NumPy vector helpers.
    """

    __slots__ = ()

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
