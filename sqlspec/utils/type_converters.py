"""Reusable converter builders for parameter configuration."""

import decimal
from typing import TYPE_CHECKING, Any, Final

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

JSON_NESTED_TYPES: Final[tuple[type[Any], ...]] = (dict, list, tuple)
DEFAULT_DECIMAL_MODE: Final[str] = "preserve"


def _decimal_identity(value: "decimal.Decimal") -> "decimal.Decimal":
    return value


def _decimal_to_string(value: "decimal.Decimal") -> str:
    return str(value)


def _decimal_to_float(value: "decimal.Decimal") -> float:
    return float(value)


def should_json_encode_sequence(sequence: "Sequence[Any]") -> bool:
    """Return ``True`` when a sequence should be JSON serialized."""

    return any(isinstance(item, JSON_NESTED_TYPES) for item in sequence if item is not None)


def build_json_list_converter(
    serializer: "Callable[[Any], str]", *, preserve_arrays: bool = True
) -> "Callable[[list[Any]], Any]":
    """Create a converter that serializes lists containing nested structures."""

    def convert(value: "list[Any]") -> Any:
        if not value:
            return value
        if preserve_arrays and not should_json_encode_sequence(value):
            return value
        return serializer(value)

    return convert


def build_json_tuple_converter(
    serializer: "Callable[[Any], str]", *, preserve_arrays: bool = True
) -> "Callable[[tuple[Any, ...]], Any]":
    """Create a converter that mirrors list handling for tuples."""

    list_converter = build_json_list_converter(serializer, preserve_arrays=preserve_arrays)

    def convert(value: "tuple[Any, ...]") -> Any:
        if not value:
            return value
        return list_converter(list(value))

    return convert


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

    def normalize(value: Any) -> Any:
        if isinstance(value, decimal.Decimal):
            return decimal_converter(value)
        if isinstance(value, list):
            return [normalize(item) for item in value]
        if isinstance(value, tuple):
            return tuple(normalize(item) for item in value)
        if isinstance(value, dict):
            return {key: normalize(item) for key, item in value.items()}
        return value

    return normalize


def build_time_iso_converter() -> "Callable[[datetime.date | datetime.datetime | datetime.time], str]":
    """Return a converter that formats temporal values using ISO 8601."""

    def convert(value: "datetime.date | datetime.datetime | datetime.time") -> str:
        return value.isoformat()

    return convert
