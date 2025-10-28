"""JSON serialization utilities for SQLSpec.

Re-exports common JSON encoding and decoding functions from the core
serialization module for convenient access.

Provides NumPy array serialization hooks for framework integrations
that support custom type encoders and decoders (e.g., Litestar).
"""

from typing import Any, Literal, overload

from sqlspec._serialization import decode_json, encode_json
from sqlspec.typing import NUMPY_INSTALLED


@overload
def to_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...


@overload
def to_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...


def to_json(data: Any, *, as_bytes: bool = False) -> str | bytes:
    """Encode data to JSON string or bytes.

    Args:
        data: Data to encode.
        as_bytes: Whether to return bytes instead of string for optimal performance.

    Returns:
        JSON string or bytes representation based on as_bytes parameter.
    """
    if as_bytes:
        return encode_json(data, as_bytes=True)
    return encode_json(data, as_bytes=False)


@overload
def from_json(data: str) -> Any: ...


@overload
def from_json(data: bytes, *, decode_bytes: bool = ...) -> Any: ...


def from_json(data: str | bytes, *, decode_bytes: bool = True) -> Any:
    """Decode JSON string or bytes to Python object.

    Args:
        data: JSON string or bytes to decode.
        decode_bytes: Whether to decode bytes input (vs passing through).

    Returns:
        Decoded Python object.
    """
    if isinstance(data, bytes):
        return decode_json(data, decode_bytes=decode_bytes)
    return decode_json(data)


def numpy_array_enc_hook(value: Any) -> Any:
    """Encode NumPy array to JSON-compatible list.

    Converts NumPy ndarrays to Python lists for JSON serialization.
    Gracefully handles cases where NumPy is not installed by returning
    the original value unchanged.

    Args:
        value: Value to encode (checked for ndarray type).

    Returns:
        List representation if value is ndarray, original value otherwise.

    Example:
        >>> import numpy as np
        >>> arr = np.array([1.0, 2.0, 3.0])
        >>> numpy_array_enc_hook(arr)
        [1.0, 2.0, 3.0]

        >>> # Multi-dimensional arrays work automatically
        >>> arr_2d = np.array([[1, 2], [3, 4]])
        >>> numpy_array_enc_hook(arr_2d)
        [[1, 2], [3, 4]]
    """
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def numpy_array_dec_hook(value: Any) -> "Any":
    """Decode list to NumPy array.

    Converts Python lists to NumPy arrays when appropriate.
    Works best with typed schemas (Pydantic, msgspec) that expect ndarray.

    Args:
        value: List to potentially convert to ndarray.

    Returns:
        NumPy array if conversion successful, original value otherwise.

    Note:
        Dtype is inferred by NumPy and may differ from original array.
        For explicit dtype control, construct arrays manually in application code.

    Example:
        >>> numpy_array_dec_hook([1.0, 2.0, 3.0])
        array([1., 2., 3.])

        >>> # Returns original value if NumPy not installed
        >>> # (when NUMPY_INSTALLED is False)
        >>> numpy_array_dec_hook([1, 2, 3])
        [1, 2, 3]
    """
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    if isinstance(value, list):
        try:
            return np.array(value)
        except Exception:
            return value
    return value


def numpy_array_predicate(value: Any) -> bool:
    """Check if value is NumPy array instance.

    Type checker for decoder registration in framework plugins.
    Returns False when NumPy is not installed.

    Args:
        value: Value to type-check.

    Returns:
        True if value is ndarray, False otherwise.

    Example:
        >>> import numpy as np
        >>> numpy_array_predicate(np.array([1, 2, 3]))
        True

        >>> numpy_array_predicate([1, 2, 3])
        False

        >>> # Returns False when NumPy not installed
        >>> # (when NUMPY_INSTALLED is False)
        >>> numpy_array_predicate([1, 2, 3])
        False
    """
    if not NUMPY_INSTALLED:
        return False

    import numpy as np

    return isinstance(value, np.ndarray)


__all__ = ("from_json", "numpy_array_dec_hook", "numpy_array_enc_hook", "numpy_array_predicate", "to_json")
