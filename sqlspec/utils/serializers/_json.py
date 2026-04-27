"""Private JSON serialization engine for ``sqlspec.utils.serializers``."""

import contextlib
import datetime
import enum
import json
import uuid as uuid_mod
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from decimal import Decimal
from ipaddress import IPv4Address, IPv4Interface, IPv4Network, IPv6Address, IPv6Interface, IPv6Network
from pathlib import Path, PurePath
from typing import Any, Final, Literal, Protocol, overload

from sqlspec.typing import (
    MSGSPEC_INSTALLED,
    NUMPY_INSTALLED,
    ORJSON_INSTALLED,
    PYDANTIC_INSTALLED,
    BaseModel,
    attrs_asdict,
)
from sqlspec.utils.type_guards import dataclass_to_dict, is_attrs_instance, is_dataclass_instance, is_msgspec_struct
from sqlspec.utils.uuids import UUID_UTILS_INSTALLED, _load_uuid_utils

__all__ = (
    "DEFAULT_TYPE_ENCODERS",
    "BaseJSONSerializer",
    "JSONSerializer",
    "MsgspecSerializer",
    "OrjsonSerializer",
    "StandardLibSerializer",
    "TypeEncodersMap",
    "convert_date_to_iso",
    "convert_datetime_to_gmt_iso",
    "decode_json",
    "encode_json",
    "get_default_serializer",
)


TypeEncodersMap = Mapping[type, Callable[[Any], Any]]


def _get_uuid_utils_type() -> "type[Any] | None":
    if not UUID_UTILS_INSTALLED:
        return None
    module = _load_uuid_utils()
    if module is None:
        return None
    return module.UUID  # type: ignore[no-any-return]


_UUID_UTILS_TYPE: "type[Any] | None" = _get_uuid_utils_type()


def convert_datetime_to_gmt_iso(value: datetime.datetime) -> str:
    """Normalize datetime values to ISO 8601 strings."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def convert_date_to_iso(value: datetime.date) -> str:
    """Normalize date values to ISO 8601 strings."""
    return value.isoformat()


def _dump_pydantic_model(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value.dict()


def _dump_msgspec_struct(value: Any) -> "dict[str, Any]":
    return {field_name: value.__getattribute__(field_name) for field_name in value.__struct_fields__}


def _build_default_type_encoders() -> "dict[type, Callable[[Any], Any]]":
    """Construct the default type encoder registry.

    Mirrors ``advanced_alchemy.utils.serialization.DEFAULT_TYPE_ENCODERS`` in
    shape, with one deliberate divergence: ``Decimal`` encodes to ``float`` to
    preserve sqlspec's prior behaviour from ``_normalize_supported_value``.
    """
    encoders: dict[type, Callable[[Any], Any]] = {
        datetime.datetime: convert_datetime_to_gmt_iso,
        datetime.date: convert_date_to_iso,
        datetime.time: lambda v: v.isoformat(),
        datetime.timedelta: lambda v: v.total_seconds(),
        # NOTE: Diverges from advanced_alchemy (str): sqlspec keeps Decimal -> float
        # to match the legacy _normalize_supported_value behaviour.
        Decimal: float,
        uuid_mod.UUID: str,
        Path: str,
        PurePath: str,
        IPv4Address: str,
        IPv4Interface: str,
        IPv4Network: str,
        IPv6Address: str,
        IPv6Interface: str,
        IPv6Network: str,
        frozenset: list,
        set: list,
        bytes: lambda v: v.decode("utf-8", errors="replace"),
        enum.Enum: lambda v: v.value,
    }

    if _UUID_UTILS_TYPE is not None:
        encoders[_UUID_UTILS_TYPE] = str

    if NUMPY_INSTALLED:
        import numpy as np

        encoders[np.ndarray] = lambda v: v.tolist()
        encoders[np.generic] = lambda v: v.item()

    if PYDANTIC_INSTALLED:
        encoders[BaseModel] = _dump_pydantic_model

    return encoders


DEFAULT_TYPE_ENCODERS: Final["dict[type, Callable[[Any], Any]]"] = _build_default_type_encoders()


def _resolve_structural_encoder(value: Any) -> Any:
    """Probe-based fallback for types that aren't keyed by class.

    ``dataclass``/``attrs``/``msgspec.Struct`` rely on ``is_*`` runtime probes
    rather than ``isinstance`` against a single base type, so they can't live
    in the type-keyed registry.
    """
    if is_dataclass_instance(value):
        return dataclass_to_dict(value)
    if is_attrs_instance(value):
        return attrs_asdict(value, recurse=True)
    if is_msgspec_struct(value):
        return _dump_msgspec_struct(value)
    return None


def _create_enc_hook(type_encoders: "Mapping[type, Callable[[Any], Any]]") -> "Callable[[Any], Any]":
    """Build an MRO-walking enc_hook bound to the supplied registry.

    The hook walks ``value.__class__.__mro__[:-1]`` (skipping ``object``) and
    returns the first matching encoder. Unmatched values fall through to
    structural probes (dataclass/attrs/msgspec.Struct) before raising
    ``TypeError`` — this preserves sqlspec's strict semantics rather than
    advanced-alchemy's ``str()`` fallback.
    """

    def enc_hook(value: Any) -> Any:
        for base in value.__class__.__mro__[:-1]:
            encoder = type_encoders.get(base)
            if encoder is not None:
                return encoder(value)
        structural = _resolve_structural_encoder(value)
        if structural is not None:
            return structural
        msg = f"unsupported JSON value: {type(value).__name__}"
        raise TypeError(msg)

    return enc_hook


_DEFAULT_ENC_HOOK: Final["Callable[[Any], Any]"] = _create_enc_hook(DEFAULT_TYPE_ENCODERS)


def _normalize_supported_value(value: Any) -> Any:
    """Convert supported non-native values into JSON-compatible objects.

    Thin shim over the default enc_hook for backwards compatibility with any
    callers that imported this private helper.
    """
    return _DEFAULT_ENC_HOOK(value)


def _is_explicit_unsupported_error(exc: Exception) -> bool:
    return "unsupported json value" in str(exc).lower()


class JSONSerializer(Protocol):
    """Protocol for JSON serializer implementations."""

    def encode(self, data: Any, *, as_bytes: bool = False) -> "str | bytes":
        """Encode Python data into JSON."""
        ...

    def decode(self, data: "str | bytes", *, decode_bytes: bool = True) -> Any:
        """Decode JSON into Python data."""
        ...


class BaseJSONSerializer(ABC):
    """Base class shared by JSON serializer implementations."""

    __slots__ = ()

    @abstractmethod
    def encode(self, data: Any, *, as_bytes: bool = False) -> "str | bytes":
        """Encode Python data into JSON."""
        ...

    @abstractmethod
    def decode(self, data: "str | bytes", *, decode_bytes: bool = True) -> Any:
        """Decode JSON into Python data."""
        ...


_orjson_fallback: "OrjsonSerializer | None" = None
_stdlib_fallback: "StandardLibSerializer | None" = None
_default_serializer: "JSONSerializer | None" = None


def _get_orjson_fallback() -> "OrjsonSerializer":
    global _orjson_fallback
    if _orjson_fallback is None:
        _orjson_fallback = OrjsonSerializer()
    return _orjson_fallback


def _get_stdlib_fallback() -> "StandardLibSerializer":
    global _stdlib_fallback
    if _stdlib_fallback is None:
        _stdlib_fallback = StandardLibSerializer()
    return _stdlib_fallback


def _merge_type_encoders(type_encoders: "TypeEncodersMap | None") -> "Callable[[Any], Any]":
    """Return an enc_hook bound to defaults merged with caller overrides."""
    if not type_encoders:
        return _DEFAULT_ENC_HOOK
    merged: dict[type, Callable[[Any], Any]] = {**DEFAULT_TYPE_ENCODERS, **type_encoders}
    return _create_enc_hook(merged)


class MsgspecSerializer(BaseJSONSerializer):
    """Msgspec-based JSON serializer."""

    __slots__ = ("_decoder", "_enc_hook", "_encoder")

    def __init__(self, type_encoders: "TypeEncodersMap | None" = None) -> None:
        from msgspec.json import Decoder, Encoder

        self._enc_hook: Callable[[Any], Any] = _merge_type_encoders(type_encoders)
        self._encoder: Final[Encoder] = Encoder(enc_hook=self._enc_hook)
        self._decoder: Final[Decoder] = Decoder()

    def encode(self, data: Any, *, as_bytes: bool = False) -> "str | bytes":
        try:
            encoded = self._encoder.encode(data)
        except TypeError as exc:
            if _is_explicit_unsupported_error(exc):
                raise
            if ORJSON_INSTALLED:
                return _get_orjson_fallback().encode(data, as_bytes=as_bytes)
            return _get_stdlib_fallback().encode(data, as_bytes=as_bytes)
        except ValueError:
            if ORJSON_INSTALLED:
                return _get_orjson_fallback().encode(data, as_bytes=as_bytes)
            return _get_stdlib_fallback().encode(data, as_bytes=as_bytes)
        return encoded if as_bytes else encoded.decode("utf-8")

    def decode(self, data: "str | bytes", *, decode_bytes: bool = True) -> Any:
        if isinstance(data, bytes):
            if not decode_bytes:
                return data
            try:
                return self._decoder.decode(data)
            except (TypeError, ValueError):
                if ORJSON_INSTALLED:
                    return _get_orjson_fallback().decode(data, decode_bytes=decode_bytes)
                return _get_stdlib_fallback().decode(data, decode_bytes=decode_bytes)
        try:
            return self._decoder.decode(data.encode("utf-8"))
        except (TypeError, ValueError):
            if ORJSON_INSTALLED:
                return _get_orjson_fallback().decode(data, decode_bytes=decode_bytes)
            return _get_stdlib_fallback().decode(data, decode_bytes=decode_bytes)


class OrjsonSerializer(BaseJSONSerializer):
    """Orjson-based JSON serializer."""

    __slots__ = ("_enc_hook",)

    def __init__(self, type_encoders: "TypeEncodersMap | None" = None) -> None:
        self._enc_hook: Callable[[Any], Any] = _merge_type_encoders(type_encoders)

    def encode(self, data: Any, *, as_bytes: bool = False) -> "str | bytes":
        from orjson import OPT_NAIVE_UTC, OPT_SERIALIZE_UUID
        from orjson import dumps as orjson_dumps  # pyright: ignore[reportMissingImports]

        options = OPT_NAIVE_UTC | OPT_SERIALIZE_UUID
        if NUMPY_INSTALLED:
            from orjson import OPT_SERIALIZE_NUMPY

            options |= OPT_SERIALIZE_NUMPY

        try:
            encoded = orjson_dumps(data, default=self._enc_hook, option=options)
        except TypeError as exc:
            if _is_explicit_unsupported_error(exc):
                raise
            if "type is not json serializable" in str(exc).lower():
                unsupported_msg = "unsupported JSON value"
                raise TypeError(unsupported_msg) from exc
            raise
        return encoded if as_bytes else encoded.decode("utf-8")

    def decode(self, data: "str | bytes", *, decode_bytes: bool = True) -> Any:
        from orjson import loads as orjson_loads  # pyright: ignore[reportMissingImports]

        if isinstance(data, bytes):
            if not decode_bytes:
                return data
            return orjson_loads(data)
        return orjson_loads(data)


class StandardLibSerializer(BaseJSONSerializer):
    """Standard library JSON serializer fallback."""

    __slots__ = ("_enc_hook",)

    def __init__(self, type_encoders: "TypeEncodersMap | None" = None) -> None:
        self._enc_hook: Callable[[Any], Any] = _merge_type_encoders(type_encoders)

    def encode(self, data: Any, *, as_bytes: bool = False) -> "str | bytes":
        encoded = json.dumps(data, default=self._enc_hook)
        return encoded.encode("utf-8") if as_bytes else encoded

    def decode(self, data: "str | bytes", *, decode_bytes: bool = True) -> Any:
        if isinstance(data, bytes):
            if not decode_bytes:
                return data
            return json.loads(data.decode("utf-8"))
        return json.loads(data)


def get_default_serializer() -> JSONSerializer:
    """Return the best available JSON serializer."""
    global _default_serializer

    if _default_serializer is None:
        if MSGSPEC_INSTALLED:
            with contextlib.suppress(ImportError):
                _default_serializer = MsgspecSerializer()
        if _default_serializer is None and ORJSON_INSTALLED:
            with contextlib.suppress(ImportError):
                _default_serializer = OrjsonSerializer()
        if _default_serializer is None:
            _default_serializer = StandardLibSerializer()

    assert _default_serializer is not None
    return _default_serializer


@overload
def encode_json(data: Any, *, as_bytes: Literal[False] = ...) -> str: ...


@overload
def encode_json(data: Any, *, as_bytes: Literal[True]) -> bytes: ...


def encode_json(data: Any, *, as_bytes: bool = False) -> "str | bytes":
    """Encode Python data into JSON."""
    return get_default_serializer().encode(data, as_bytes=as_bytes)


def decode_json(data: "str | bytes", *, decode_bytes: bool = True) -> Any:
    """Decode JSON input into Python data."""
    return get_default_serializer().decode(data, decode_bytes=decode_bytes)
