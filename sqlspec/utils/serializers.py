"""Serialization utilities for SQLSpec.

Provides JSON helpers, serializer pipelines, optional dependency hooks,
and cache instrumentation aligned with the core pipeline counters.
"""

import os
from functools import partial
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, Literal, cast, overload

from sqlspec._serialization import decode_json, encode_json
from sqlspec.typing import NUMPY_INSTALLED, UNSET, ArrowReturnFormat, attrs_asdict
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
from sqlspec.utils.type_guards import (
    dataclass_to_dict,
    has_dict_attribute,
    is_attrs_instance,
    is_dataclass_instance,
    is_dict,
    is_msgspec_struct,
    is_pydantic_model,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

__all__ = (
    "SchemaSerializer",
    "from_json",
    "get_collection_serializer",
    "get_serializer_metrics",
    "numpy_array_dec_hook",
    "numpy_array_enc_hook",
    "numpy_array_predicate",
    "reset_serializer_cache",
    "schema_dump",
    "serialize_collection",
    "to_json",
)

DEBUG_ENV_FLAG: Final[str] = "SQLSPEC_DEBUG_PIPELINE_CACHE"
_PRIMITIVE_TYPES: Final[tuple[type[Any], ...]] = (str, bytes, int, float, bool)


def _is_truthy(value: "str | None") -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def _metrics_enabled() -> bool:
    return _is_truthy(os.getenv(DEBUG_ENV_FLAG))


class _SerializerCacheMetrics:
    __slots__ = ("hits", "max_size", "misses", "size")

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.size = 0
        self.max_size = 0

    def record_hit(self, cache_size: int) -> None:
        if not _metrics_enabled():
            return
        self.hits += 1
        self.size = cache_size
        self.max_size = max(self.max_size, cache_size)

    def record_miss(self, cache_size: int) -> None:
        if not _metrics_enabled():
            return
        self.misses += 1
        self.size = cache_size
        self.max_size = max(self.max_size, cache_size)

    def reset(self) -> None:
        self.hits = 0
        self.misses = 0
        self.size = 0
        self.max_size = 0

    def snapshot(self) -> "dict[str, int]":
        return {
            "hits": self.hits if _metrics_enabled() else 0,
            "misses": self.misses if _metrics_enabled() else 0,
            "max_size": self.max_size if _metrics_enabled() else 0,
            "size": self.size if _metrics_enabled() else 0,
        }


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


def numpy_array_dec_hook(value: Any) -> Any:
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


class SchemaSerializer:
    """Serializer pipeline that caches conversions for repeated schema dumps."""

    __slots__ = ("_dump", "_key")

    def __init__(self, key: "tuple[type[Any] | None, bool]", dump: "Callable[[Any], dict[str, Any]]") -> None:
        self._key = key
        self._dump = dump

    @property
    def key(self) -> "tuple[type[Any] | None, bool]":
        return self._key

    def dump_one(self, item: Any) -> "dict[str, Any]":
        return self._dump(item)

    def dump_many(self, items: "Iterable[Any]") -> "list[dict[str, Any]]":
        return [self._dump(item) for item in items]

    def to_arrow(
        self, items: "Iterable[Any]", *, return_format: "ArrowReturnFormat" = "table", batch_size: int | None = None
    ) -> Any:
        payload = self.dump_many(items)
        return convert_dict_to_arrow(payload, return_format=return_format, batch_size=batch_size)


_SERIALIZER_LOCK: RLock = RLock()
_SCHEMA_SERIALIZERS: dict[tuple[type[Any] | None, bool], SchemaSerializer] = {}
_SERIALIZER_METRICS = _SerializerCacheMetrics()


def _make_serializer_key(sample: Any, exclude_unset: bool) -> "tuple[type[Any] | None, bool]":
    if sample is None or isinstance(sample, dict):
        return (None, exclude_unset)
    return (type(sample), exclude_unset)


def _dump_identity_dict(value: Any) -> "dict[str, Any]":
    return cast("dict[str, Any]", value)


def _dump_msgspec_fields(value: Any) -> "dict[str, Any]":
    return {f: value.__getattribute__(f) for f in value.__struct_fields__}


def _dump_msgspec_excluding_unset(value: Any) -> "dict[str, Any]":
    return {f: field_value for f in value.__struct_fields__ if (field_value := value.__getattribute__(f)) != UNSET}


def _dump_dataclass(value: Any, *, exclude_unset: bool) -> "dict[str, Any]":
    return dataclass_to_dict(value, exclude_empty=exclude_unset)


def _dump_pydantic(value: Any, *, exclude_unset: bool) -> "dict[str, Any]":
    return cast("dict[str, Any]", value.model_dump(exclude_unset=exclude_unset))


def _dump_attrs(value: Any) -> "dict[str, Any]":
    return attrs_asdict(value, recurse=True)


def _dump_dict_attr(value: Any) -> "dict[str, Any]":
    return dict(value.__dict__)


def _dump_mapping(value: Any) -> "dict[str, Any]":
    return dict(value)


def _build_dump_function(sample: Any, exclude_unset: bool) -> "Callable[[Any], dict[str, Any]]":
    if sample is None or isinstance(sample, dict):
        return _dump_identity_dict

    if is_dataclass_instance(sample):
        return cast("Callable[[Any], dict[str, Any]]", partial(_dump_dataclass, exclude_unset=exclude_unset))
    if is_pydantic_model(sample):
        return cast("Callable[[Any], dict[str, Any]]", partial(_dump_pydantic, exclude_unset=exclude_unset))
    if is_msgspec_struct(sample):
        if exclude_unset:
            return _dump_msgspec_excluding_unset
        return _dump_msgspec_fields

    if is_attrs_instance(sample):
        return _dump_attrs

    if has_dict_attribute(sample):
        return _dump_dict_attr

    return _dump_mapping


def get_collection_serializer(sample: Any, *, exclude_unset: bool = True) -> "SchemaSerializer":
    """Return cached serializer pipeline for the provided sample object."""

    key = _make_serializer_key(sample, exclude_unset)
    with _SERIALIZER_LOCK:
        pipeline = _SCHEMA_SERIALIZERS.get(key)
        if pipeline is not None:
            _SERIALIZER_METRICS.record_hit(len(_SCHEMA_SERIALIZERS))
            return pipeline

        dump = _build_dump_function(sample, exclude_unset)
        pipeline = SchemaSerializer(key, dump)
        _SCHEMA_SERIALIZERS[key] = pipeline
        _SERIALIZER_METRICS.record_miss(len(_SCHEMA_SERIALIZERS))
        return pipeline


def serialize_collection(items: "Iterable[Any]", *, exclude_unset: bool = True) -> "list[Any]":
    """Serialize a collection using cached pipelines keyed by item type."""

    serialized: list[Any] = []
    cache: dict[tuple[type[Any] | None, bool], SchemaSerializer] = {}

    for item in items:
        if isinstance(item, _PRIMITIVE_TYPES) or item is None or isinstance(item, dict):
            serialized.append(item)
            continue

        key = _make_serializer_key(item, exclude_unset)
        pipeline = cache.get(key)
        if pipeline is None:
            pipeline = get_collection_serializer(item, exclude_unset=exclude_unset)
            cache[key] = pipeline
        serialized.append(pipeline.dump_one(item))
    return serialized


def reset_serializer_cache() -> None:
    """Clear cached serializer pipelines."""

    with _SERIALIZER_LOCK:
        _SCHEMA_SERIALIZERS.clear()
        _SERIALIZER_METRICS.reset()


def get_serializer_metrics() -> "dict[str, int]":
    """Return cache metrics aligned with the core pipeline counters."""

    with _SERIALIZER_LOCK:
        metrics = _SERIALIZER_METRICS.snapshot()
        metrics["size"] = len(_SCHEMA_SERIALIZERS)
        return metrics


def schema_dump(data: Any, *, exclude_unset: bool = True) -> Any:
    """Dump a schema model or dict to a plain representation.

    Args:
        data: Schema model instance or dictionary to dump.
        exclude_unset: Whether to exclude unset fields (for models that support it).

    Returns:
        A plain representation of the schema model or value.
    """
    if is_dict(data):
        return data

    if isinstance(data, _PRIMITIVE_TYPES) or data is None:
        return data

    serializer = get_collection_serializer(data, exclude_unset=exclude_unset)
    return serializer.dump_one(data)
