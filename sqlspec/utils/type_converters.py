"""Reusable converter builders for parameter configuration."""

import decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import datetime
    from collections.abc import Callable, Sequence

__all__ = (
    "DEFAULT_DECIMAL_MODE",
    "build_decimal_converter",
    "build_json_list_converter",
    "build_json_tuple_converter",
    "build_nested_decimal_normalizer",
    "build_time_iso_converter",
    "should_json_encode_sequence",
)

JSON_NESTED_TYPES: "tuple[type[Any], ...]" = (dict, list, tuple)
DEFAULT_DECIMAL_MODE: str = "preserve"


def _decimal_identity(value: "decimal.Decimal") -> "decimal.Decimal":
    return value


def _decimal_to_string(value: "decimal.Decimal") -> str:
    return str(value)


def _decimal_to_float(value: "decimal.Decimal") -> float:
    return float(value)


class _JsonListConverter:
    __slots__ = ("_preserve_arrays", "_serializer")

    def __init__(self, serializer: "Callable[[Any], str]", preserve_arrays: bool) -> None:
        self._serializer = serializer
        self._preserve_arrays = preserve_arrays

    def __call__(self, value: "list[Any]") -> Any:
        if not value:
            return value
        if self._preserve_arrays and not should_json_encode_sequence(value):
            return value
        return self._serializer(value)


class _JsonTupleConverter:
    __slots__ = ("_list_converter",)

    def __init__(self, list_converter: _JsonListConverter) -> None:
        self._list_converter = list_converter

    def __call__(self, value: "tuple[Any, ...]") -> Any:
        if not value:
            return value
        return self._list_converter(list(value))


class _DecimalNormalizer:
    __slots__ = ("_decimal_converter",)

    def __init__(self, decimal_converter: "Callable[[decimal.Decimal], Any]") -> None:
        self._decimal_converter = decimal_converter

    def __call__(self, value: Any) -> Any:
        if isinstance(value, decimal.Decimal):
            return self._decimal_converter(value)
        if isinstance(value, list):
            return [self(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self(item) for item in value)
        if isinstance(value, dict):
            return {key: self(item) for key, item in value.items()}
        return value


def _time_iso_convert(value: "datetime.date | datetime.datetime | datetime.time") -> str:
    return value.isoformat()


def should_json_encode_sequence(sequence: "Sequence[Any]") -> bool:
    """Return ``True`` when a sequence should be JSON serialized."""

    return any(isinstance(item, JSON_NESTED_TYPES) for item in sequence if item is not None)


def build_json_list_converter(
    serializer: "Callable[[Any], str]", *, preserve_arrays: bool = True
) -> "Callable[[list[Any]], Any]":
    """Create a converter that serializes lists containing nested structures."""
    return _JsonListConverter(serializer, preserve_arrays)


def build_json_tuple_converter(
    serializer: "Callable[[Any], str]", *, preserve_arrays: bool = True
) -> "Callable[[tuple[Any, ...]], Any]":
    """Create a converter that mirrors list handling for tuples."""
    list_converter = _JsonListConverter(serializer, preserve_arrays)
    return _JsonTupleConverter(list_converter)


def build_decimal_converter(*, mode: str = DEFAULT_DECIMAL_MODE) -> "Callable[[decimal.Decimal], Any]":
    """Create a Decimal converter according to the desired mode."""

    if mode == "preserve":
        return _decimal_identity
    if mode == "string":
        return _decimal_to_string
    if mode == "float":
        return _decimal_to_float

    msg = f"Unsupported decimal converter mode: {mode}"
    raise ValueError(msg)


def build_nested_decimal_normalizer(*, mode: str = DEFAULT_DECIMAL_MODE) -> "Callable[[Any], Any]":
    """Return a callable that coerces ``Decimal`` values within nested structures."""
    decimal_converter = build_decimal_converter(mode=mode)
    return _DecimalNormalizer(decimal_converter)


def build_time_iso_converter() -> "Callable[[datetime.date | datetime.datetime | datetime.time], str]":
    """Return a converter that formats temporal values using ISO 8601."""
    return _time_iso_convert
