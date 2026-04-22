"""Schema dumping and cache helpers for ``sqlspec.utils.serializers``."""

import os
from functools import partial
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.typing import UNSET, ArrowReturnFormat, attrs_asdict
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
    "get_collection_serializer",
    "get_serializer_metrics",
    "reset_serializer_cache",
    "schema_dump",
    "serialize_collection",
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
    from msgspec import structs

    return {field.encode_name: value.__getattribute__(field.name) for field in structs.fields(type(value))}


def _dump_msgspec_excluding_unset(value: Any) -> "dict[str, Any]":
    from msgspec import structs

    return {
        field.encode_name: field_value
        for field in structs.fields(type(value))
        if (field_value := value.__getattribute__(field.name)) != UNSET
    }


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
    """Dump a schema model or dict to a plain representation."""
    if is_dict(data):
        return data
    if isinstance(data, _PRIMITIVE_TYPES) or data is None:
        return data

    serializer = get_collection_serializer(data, exclude_unset=exclude_unset)
    return serializer.dump_one(data)
