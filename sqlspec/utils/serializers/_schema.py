"""Schema dumping and cache helpers for ``sqlspec.utils.serializers``."""

import os
from collections import OrderedDict
from dataclasses import Field
from dataclasses import fields as dataclasses_fields
from functools import partial
from threading import RLock
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.typing import MSGSPEC_INSTALLED, UNSET, ArrowReturnFormat, Empty, msgspec_fields
from sqlspec.utils.arrow_helpers import convert_dict_to_arrow
from sqlspec.utils.module_loader import import_optional_attr
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
_PRIMITIVE_TYPES_SET: Final[frozenset[type[Any]]] = frozenset(_PRIMITIVE_TYPES)
_SCHEMA_SERIALIZER_CACHE_MAX_SIZE: Final[int] = 1000


def _is_truthy(value: "str | None") -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return normalized in {"1", "true", "yes", "on"}


_METRICS_ENABLED: Final[bool] = _is_truthy(os.getenv(DEBUG_ENV_FLAG))


class _SerializerCacheMetrics:
    __slots__ = ("hits", "max_size", "misses", "size")

    def __init__(self) -> None:
        self.hits = 0
        self.misses = 0
        self.size = 0
        self.max_size = 0

    def record_hit(self, cache_size: int) -> None:
        if not _METRICS_ENABLED:
            return
        self.hits += 1
        self.size = cache_size
        self.max_size = max(self.max_size, cache_size)

    def record_miss(self, cache_size: int) -> None:
        if not _METRICS_ENABLED:
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
            "hits": self.hits if _METRICS_ENABLED else 0,
            "misses": self.misses if _METRICS_ENABLED else 0,
            "max_size": self.max_size if _METRICS_ENABLED else 0,
            "size": self.size if _METRICS_ENABLED else 0,
        }


class SchemaSerializer:
    """Serializer pipeline that caches conversions for repeated schema dumps."""

    __slots__ = ("_dump", "_key")

    def __init__(self, key: "tuple[type[Any] | None, bool, bool]", dump: "Callable[[Any], dict[str, Any]]") -> None:
        self._key = key
        self._dump = dump

    @property
    def key(self) -> "tuple[type[Any] | None, bool, bool]":
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
_SCHEMA_SERIALIZERS: "OrderedDict[tuple[type[Any] | None, bool, bool], SchemaSerializer]" = OrderedDict()
_SERIALIZER_METRICS = _SerializerCacheMetrics()
_DATACLASS_FIELDS_CACHE: "dict[type[Any], tuple[Field[Any], ...]]" = {}


def _make_serializer_key(sample: Any, exclude_unset: bool, wire_format: bool) -> "tuple[type[Any] | None, bool, bool]":
    if sample is None or isinstance(sample, dict):
        return (None, exclude_unset, wire_format)
    return (type(sample), exclude_unset, wire_format)


def _dump_identity_dict(value: Any) -> "dict[str, Any]":
    return cast("dict[str, Any]", value)


def _dataclass_fields(schema_type: type[Any]) -> "tuple[Field[Any], ...]":
    cached = _DATACLASS_FIELDS_CACHE.get(schema_type)
    if cached is not None:
        return cached
    fields = dataclasses_fields(schema_type)
    _DATACLASS_FIELDS_CACHE[schema_type] = fields
    return fields


def _msgspec_field_pairs(schema_type: type[Any], *, wire_format: bool) -> "tuple[tuple[str, str], ...]":
    if not MSGSPEC_INSTALLED:
        msg = "msgspec is required to serialize msgspec.Struct values"
        raise RuntimeError(msg)

    if wire_format:
        return tuple((field.encode_name, field.name) for field in msgspec_fields(schema_type))
    return tuple((field.name, field.name) for field in msgspec_fields(schema_type))


def _dump_msgspec_struct(
    value: Any, *, field_pairs: "tuple[tuple[str, str], ...]", exclude_unset: bool
) -> "dict[str, Any]":
    if not exclude_unset:
        return {output_name: value.__getattribute__(field_name) for output_name, field_name in field_pairs}
    return {
        output_name: field_value
        for output_name, field_name in field_pairs
        if (field_value := value.__getattribute__(field_name)) != UNSET
    }


def _dump_dataclass(value: Any, *, dataclass_fields: "tuple[Field[Any], ...]", exclude_unset: bool) -> "dict[str, Any]":
    result: dict[str, Any] = {}
    for field in dataclass_fields:
        field_value = value.__getattribute__(field.name)
        if exclude_unset and field_value is Empty:
            continue
        if is_dataclass_instance(field_value):
            result[field.name] = dataclass_to_dict(field_value, exclude_empty=exclude_unset)
        else:
            result[field.name] = field_value
    return result


def _dump_pydantic(value: Any, *, exclude_unset: bool) -> "dict[str, Any]":
    return cast("dict[str, Any]", value.model_dump(exclude_unset=exclude_unset))


def _dump_attrs(value: Any) -> "dict[str, Any]":
    attrs_asdict = import_optional_attr("attrs", "asdict")
    return cast("dict[str, Any]", attrs_asdict(value, recurse=True))


def _dump_dict_attr(value: Any) -> "dict[str, Any]":
    return dict(value.__dict__)


def _dump_mapping(value: Any) -> "dict[str, Any]":
    return dict(value)


def _msgspec_dump_function(sample: Any, *, exclude_unset: bool, wire_format: bool) -> "Callable[[Any], dict[str, Any]]":
    field_pairs = _msgspec_field_pairs(type(sample), wire_format=wire_format)
    return cast(
        "Callable[[Any], dict[str, Any]]",
        partial(_dump_msgspec_struct, field_pairs=field_pairs, exclude_unset=exclude_unset),
    )


def _dataclass_dump_function(sample: Any, *, exclude_unset: bool) -> "Callable[[Any], dict[str, Any]]":
    field_tuple = _dataclass_fields(type(sample))
    return cast(
        "Callable[[Any], dict[str, Any]]",
        partial(_dump_dataclass, dataclass_fields=field_tuple, exclude_unset=exclude_unset),
    )


def _dump_function(sample: Any, exclude_unset: bool, wire_format: bool) -> "Callable[[Any], dict[str, Any]]":
    if sample is None or isinstance(sample, dict):
        return _dump_identity_dict
    if is_dataclass_instance(sample):
        return _dataclass_dump_function(sample, exclude_unset=exclude_unset)
    if is_pydantic_model(sample):
        return cast("Callable[[Any], dict[str, Any]]", partial(_dump_pydantic, exclude_unset=exclude_unset))
    if is_msgspec_struct(sample):
        return _msgspec_dump_function(sample, exclude_unset=exclude_unset, wire_format=wire_format)
    if is_attrs_instance(sample):
        return _dump_attrs
    if has_dict_attribute(sample):
        return _dump_dict_attr
    return _dump_mapping


def get_collection_serializer(
    sample: Any, *, exclude_unset: bool = True, wire_format: bool = False
) -> "SchemaSerializer":
    """Return cached serializer pipeline for the provided sample object."""
    key = _make_serializer_key(sample, exclude_unset, wire_format)
    pipeline = _SCHEMA_SERIALIZERS.get(key)
    if pipeline is not None:
        try:
            _SCHEMA_SERIALIZERS.move_to_end(key)
        except KeyError:
            pipeline = None
        else:
            _SERIALIZER_METRICS.record_hit(len(_SCHEMA_SERIALIZERS))
            return pipeline

    with _SERIALIZER_LOCK:
        pipeline = _SCHEMA_SERIALIZERS.get(key)
        if pipeline is not None:
            _SCHEMA_SERIALIZERS.move_to_end(key)
            _SERIALIZER_METRICS.record_hit(len(_SCHEMA_SERIALIZERS))
            return pipeline

        dump = _dump_function(sample, exclude_unset, wire_format)
        pipeline = SchemaSerializer(key, dump)
        _SCHEMA_SERIALIZERS[key] = pipeline
        if len(_SCHEMA_SERIALIZERS) > _SCHEMA_SERIALIZER_CACHE_MAX_SIZE:
            _SCHEMA_SERIALIZERS.popitem(last=False)
        _SERIALIZER_METRICS.record_miss(len(_SCHEMA_SERIALIZERS))
        return pipeline


def serialize_collection(
    items: "Iterable[Any]", *, exclude_unset: bool = True, wire_format: bool = False
) -> "list[Any]":
    """Serialize a collection using cached pipelines keyed by item type."""
    serialized: list[Any] = []
    cache: dict[tuple[type[Any] | None, bool, bool], SchemaSerializer] = {}

    for item in items:
        if type(item) in _PRIMITIVE_TYPES_SET or item is None or isinstance(item, dict):
            serialized.append(item)
            continue

        key = _make_serializer_key(item, exclude_unset, wire_format)
        pipeline = cache.get(key)
        if pipeline is None:
            pipeline = get_collection_serializer(item, exclude_unset=exclude_unset, wire_format=wire_format)
            cache[key] = pipeline
        serialized.append(pipeline.dump_one(item))
    return serialized


def reset_serializer_cache() -> None:
    """Clear cached serializer pipelines."""
    with _SERIALIZER_LOCK:
        _SCHEMA_SERIALIZERS.clear()
        _DATACLASS_FIELDS_CACHE.clear()
        _SERIALIZER_METRICS.reset()


def get_serializer_metrics() -> "dict[str, int]":
    """Return cache metrics aligned with the core pipeline counters."""
    with _SERIALIZER_LOCK:
        metrics = _SERIALIZER_METRICS.snapshot()
        metrics["size"] = len(_SCHEMA_SERIALIZERS)
        return metrics


def schema_dump(data: Any, *, exclude_unset: bool = True, wire_format: bool = False) -> Any:
    """Dump a schema model or dict to a plain representation.

    Args:
        data: A schema instance (msgspec.Struct, Pydantic BaseModel, dataclass, attrs class)
            or plain dict / primitive.
        exclude_unset: If True, exclude fields that were never set (msgspec UNSET, Pydantic
            model_fields_set semantics). No-op for attrs (attrs has no unset concept).
        wire_format: msgspec-only knob. Default ``False`` emits Python attribute names
            (``field.name``) for cross-library consistency — keys match Pydantic, dataclass,
            and attrs output regardless of the Struct's ``rename=`` meta. Pass
            ``wire_format=True`` to emit ``field.encode_name`` (honours ``rename=`` on the
            Struct) for wire-aligned JSON / API payloads. Pydantic, dataclass, and attrs
            branches always use Python attribute names regardless of this flag.
    """
    if is_dict(data):
        return data
    if type(data) in _PRIMITIVE_TYPES_SET or data is None:
        return data

    serializer = get_collection_serializer(data, exclude_unset=exclude_unset, wire_format=wire_format)
    return serializer.dump_one(data)
