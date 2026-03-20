"""NumPy serialization helpers for ``sqlspec.utils.serializers``."""

from typing import Any, Final

from sqlspec.typing import NUMPY_INSTALLED

_NUMPY_DECODER_SENTINEL: Final[object] = object()


def numpy_array_enc_hook(value: Any) -> Any:
    """Encode NumPy arrays and scalars to JSON-compatible values."""
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    return value


def numpy_array_dec_hook(target_or_value: Any, value: Any = _NUMPY_DECODER_SENTINEL) -> Any:
    """Decode JSON list payloads into NumPy arrays.

    Supports both direct one-argument usage and Litestar's
    ``(target_type, value)`` decoder contract.
    """
    if value is _NUMPY_DECODER_SENTINEL:
        raw_value = target_or_value
        should_decode = True
    else:
        raw_value = value
        should_decode = numpy_array_predicate(target_or_value)

    if not NUMPY_INSTALLED:
        return raw_value
    if not should_decode or not isinstance(raw_value, list):
        return raw_value

    import numpy as np

    try:
        return np.array(raw_value)
    except Exception:
        return raw_value


def numpy_array_predicate(value_or_target: Any) -> bool:
    """Check whether a value or target type represents a NumPy array."""
    if not NUMPY_INSTALLED:
        return False

    import numpy as np

    if isinstance(value_or_target, type):
        try:
            return issubclass(value_or_target, np.ndarray)
        except TypeError:
            return False
    return isinstance(value_or_target, np.ndarray)
