"""Oracle vector type handlers for the DB_TYPE_VECTOR data type.

Provides automatic conversion between Python sequence-of-numbers
(``numpy.ndarray``, ``array.array``, ``list``, ``tuple``) and Oracle VECTOR
columns. Requires Oracle Database 23ai or higher.

Public symbols keep the historical ``numpy_*`` prefix for backwards-compat with
sqlspec ``__all__`` consumers; the user-facing rename to ``vector_*`` is tracked
as a follow-up.
"""

import array
import sys
from typing import TYPE_CHECKING, Any

from sqlspec.typing import NUMPY_INSTALLED
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor

__all__ = (
    "DTYPE_TO_ARRAY_CODE",
    "numpy_converter_in",
    "numpy_converter_out",
    "numpy_input_type_handler",
    "numpy_output_type_handler",
    "register_numpy_handlers",
)


logger = get_logger(__name__)


_TYPECODE_FLOAT64 = "d"
_TYPECODE_FLOAT32 = "f"
_TYPECODE_FLOAT16 = "e"
_TYPECODE_UINT8 = "B"
_TYPECODE_INT8 = "b"
_TYPECODE_INT16 = "h"
_TYPECODE_INT32 = "i"

DTYPE_TO_ARRAY_CODE: "dict[str, str]" = {
    "float64": _TYPECODE_FLOAT64,
    "float32": _TYPECODE_FLOAT32,
    "uint8": _TYPECODE_UINT8,
    "int8": _TYPECODE_INT8,
    "int16": _TYPECODE_INT16,
    "int32": _TYPECODE_INT32,
}
if sys.version_info >= (3, 13):
    DTYPE_TO_ARRAY_CODE["float16"] = _TYPECODE_FLOAT16


def numpy_converter_in(value: Any) -> "array.array[Any]":
    """Convert NumPy array to Oracle array for VECTOR insertion.

    Args:
        value: NumPy ndarray to convert.

    Returns:
        Python array.array compatible with Oracle VECTOR type.

    Raises:
        ImportError: If NumPy is not installed.
        TypeError: If NumPy dtype is not supported for Oracle VECTOR.
    """
    if not NUMPY_INSTALLED:
        msg = "NumPy is not installed - cannot convert vectors"
        raise ImportError(msg)

    dtype_name = value.dtype.name
    array_code = DTYPE_TO_ARRAY_CODE.get(dtype_name)

    if not array_code:
        supported = ", ".join(DTYPE_TO_ARRAY_CODE.keys())
        msg = f"Unsupported NumPy dtype for Oracle VECTOR: {dtype_name}. Supported: {supported}"
        raise TypeError(msg)

    return array.array(array_code, value)


def numpy_converter_out(value: "array.array[Any]") -> Any:
    """Convert Oracle array to NumPy array for VECTOR retrieval.

    Args:
        value: Oracle array.array from VECTOR column.

    Returns:
        NumPy ndarray with appropriate dtype, or original value if NumPy not installed.
    """
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    return np.array(value, copy=True, dtype=value.typecode)


def _input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Oracle input type handler for NumPy arrays.

    Args:
        cursor: Oracle cursor (sync or async).
        value: Value being inserted.
        arraysize: Array size for the cursor variable.

    Returns:
        Cursor variable with NumPy converter if value is ndarray, None otherwise.
    """
    if not NUMPY_INSTALLED:
        return None

    import numpy as np
    import oracledb

    if isinstance(value, np.ndarray):
        return cursor.var(oracledb.DB_TYPE_VECTOR, arraysize=arraysize, inconverter=numpy_converter_in)
    return None


def _output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Oracle output type handler for VECTOR columns.

    Args:
        cursor: Oracle cursor (sync or async).
        metadata: Column metadata from Oracle.

    Returns:
        Cursor variable with NumPy converter if column is VECTOR, None otherwise.
    """
    if not NUMPY_INSTALLED:
        return None

    import oracledb

    if metadata.type_code is oracledb.DB_TYPE_VECTOR:
        return cursor.var(metadata.type_code, arraysize=cursor.arraysize, outconverter=numpy_converter_out)
    return None


def numpy_input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Public input type handler for NumPy arrays."""
    return _input_type_handler(cursor, value, arraysize)


def numpy_output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Public output type handler for NumPy VECTOR columns."""
    return _output_type_handler(cursor, metadata)


def register_numpy_handlers(connection: "Connection | AsyncConnection") -> None:
    """Register NumPy type handlers on Oracle connection.

    Enables automatic conversion between NumPy arrays and Oracle VECTOR types.
    Works for both sync and async connections.

    Args:
        connection: Oracle connection (sync or async).
    """
    if not NUMPY_INSTALLED:
        return

    connection.inputtypehandler = numpy_input_type_handler
    connection.outputtypehandler = numpy_output_type_handler
