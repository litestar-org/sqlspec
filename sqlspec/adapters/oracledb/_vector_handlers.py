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

from sqlspec.adapters.oracledb._typing import DB_TYPE_VECTOR
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

_INT8_MIN = -128
_INT8_MAX = 127

_VECTOR_RETURN_NUMPY = "numpy"
_VECTOR_RETURN_LIST = "list"
_VECTOR_RETURN_ARRAY = "array"
_VECTOR_RETURN_FORMATS = frozenset({_VECTOR_RETURN_NUMPY, _VECTOR_RETURN_LIST, _VECTOR_RETURN_ARRAY})

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


def _is_vector_payload(value: Any) -> bool:
    """Return True if the value should be claimed by the vector input handler.

    Mirrors the C1 ``_is_json_payload`` predicate but inverted: claims sequences
    of numbers (vector embeddings); rejects ``dict`` / ``list[dict]`` which the
    JSON handler owns. ``bool`` is excluded explicitly because it is a subclass
    of ``int`` but is owned by the JSON path.
    """
    if isinstance(value, array.array):
        return True
    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(value, np.ndarray):
            return True
    if isinstance(value, (list, tuple)) and value:
        first = value[0]
        if isinstance(first, bool):
            return False
        return isinstance(first, (int, float))
    return False


def _pack_python_sequence(value: "list[Any] | tuple[Any, ...]") -> "array.array[Any]":
    """Pack a Python sequence into an ``array.array`` for VECTOR binding.

    Integer sequences entirely within ``[-128, 127]`` use the int8 typecode for
    a cheaper bind; everything else falls back to float32 (the 23ai default and
    the most common LLM embedding dtype).
    """
    if all(isinstance(v, int) and not isinstance(v, bool) and _INT8_MIN <= v <= _INT8_MAX for v in value):
        return array.array(_TYPECODE_INT8, value)
    return array.array(_TYPECODE_FLOAT32, [float(v) for v in value])


def _input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Oracle input type handler for vector payloads.

    Args:
        cursor: Oracle cursor (sync or async).
        value: Value being inserted.
        arraysize: Array size for the cursor variable.

    Returns:
        Cursor variable for VECTOR binding when ``value`` is a vector payload,
        otherwise ``None`` so the next handler in the chain can claim it.
    """
    if not _is_vector_payload(value):
        return None

    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(value, np.ndarray):
            return cursor.var(DB_TYPE_VECTOR, arraysize=arraysize, inconverter=numpy_converter_in)

    if isinstance(value, array.array):
        return cursor.var(DB_TYPE_VECTOR, arraysize=arraysize)

    packed = _pack_python_sequence(value)
    return cursor.var(DB_TYPE_VECTOR, arraysize=arraysize, inconverter=lambda _v: packed)


def _output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Oracle output type handler for VECTOR columns.

    Reads ``connection._sqlspec_vector_return_format`` (set by the session
    callback in ``config._init_connection``) to dispatch to the requested
    return type. Falls back to ``"numpy"`` when NumPy is installed and
    ``"list"`` otherwise so consumers without the connection-level setting
    still get sensible behavior.
    """
    if metadata.type_code is not DB_TYPE_VECTOR:
        return None

    fmt = getattr(cursor.connection, "_sqlspec_vector_return_format", None)
    if fmt is None:
        fmt = _VECTOR_RETURN_NUMPY if NUMPY_INSTALLED else _VECTOR_RETURN_LIST

    if fmt == _VECTOR_RETURN_NUMPY:
        if not NUMPY_INSTALLED:
            msg = (
                "vector_return_format='numpy' requires numpy; install with "
                "`pip install sqlspec[oracle,numpy]` or set vector_return_format='list'."
            )
            raise RuntimeError(msg)
        return cursor.var(metadata.type_code, arraysize=cursor.arraysize, outconverter=numpy_converter_out)
    if fmt == _VECTOR_RETURN_LIST:
        return cursor.var(metadata.type_code, arraysize=cursor.arraysize, outconverter=list)
    if fmt == _VECTOR_RETURN_ARRAY:
        return None

    msg = f"Invalid vector_return_format: {fmt!r}; expected one of {sorted(_VECTOR_RETURN_FORMATS)}"
    raise ValueError(msg)


def numpy_input_type_handler(cursor: "Cursor | AsyncCursor", value: Any, arraysize: int) -> Any:
    """Public input type handler for vector payloads."""
    return _input_type_handler(cursor, value, arraysize)


def numpy_output_type_handler(cursor: "Cursor | AsyncCursor", metadata: Any) -> Any:
    """Public output type handler for VECTOR columns."""
    return _output_type_handler(cursor, metadata)


def register_numpy_handlers(connection: "Connection | AsyncConnection") -> None:
    """Register vector type handlers on an Oracle connection.

    Enables automatic conversion between Python sequence types and Oracle
    VECTOR columns. Works for both sync and async connections.

    Args:
        connection: Oracle connection (sync or async).
    """
    connection.inputtypehandler = numpy_input_type_handler
    connection.outputtypehandler = numpy_output_type_handler
