"""Schema transformation utilities for converting data to various schema types."""

import datetime
from collections.abc import Callable, Sequence
from decimal import Decimal, InvalidOperation
from enum import Enum
from functools import lru_cache, partial
from pathlib import Path, PurePath
from typing import Any, Final, TypeGuard, cast, overload
from uuid import UUID

from typing_extensions import TypeVar

from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import (
    CATTRS_INSTALLED,
    NUMPY_INSTALLED,
    SchemaT,
    attrs_asdict,
    cattrs_structure,
    cattrs_unstructure,
    convert,
    get_type_adapter,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json
from sqlspec.utils.text import camelize, kebabize, pascalize
from sqlspec.utils.type_guards import (
    get_msgspec_rename_config,
    is_attrs_instance,
    is_attrs_schema,
    is_dataclass,
    is_dict,
    is_msgspec_struct,
    is_pydantic_model,
    is_typed_dict,
)

__all__ = (
    "_DEFAULT_TYPE_DECODERS",
    "DataT",
    "ValueT",
    "_convert_numpy_recursive",
    "_convert_numpy_to_list",
    "_default_msgspec_deserializer",
    "_is_list_type_target",
    "to_schema",
    "to_value_type",
    "transform_dict_keys",
)

DataT = TypeVar("DataT", default=dict[str, Any])
ValueT = TypeVar("ValueT")

logger = get_logger(__name__)

_DATETIME_TYPES: Final[set[type]] = {datetime.datetime, datetime.date, datetime.time}
_DATETIME_TYPE_TUPLE: Final[tuple[type, ...]] = (datetime.datetime, datetime.date, datetime.time)


# =============================================================================
# Dict Key Transformation
# =============================================================================


def _safe_convert_key(key: Any, converter: Callable[[str], str]) -> Any:
    """Safely convert a key using the converter function.

    Args:
        key: Key to convert (may not be a string).
        converter: Function to convert string keys.

    Returns:
        Converted key if conversion succeeds, original key otherwise.
    """
    if not isinstance(key, str):
        return key

    try:
        return converter(key)
    except (TypeError, ValueError, AttributeError):
        return key


def transform_dict_keys(data: dict | list | Any, converter: Callable[[str], str]) -> dict | list | Any:
    """Transform dictionary keys using the provided converter function.

    Recursively transforms all dictionary keys in a data structure using
    the provided converter function. Handles nested dictionaries, lists
    of dictionaries, and preserves non-dict values unchanged.

    Args:
        data: The data structure to transform. Can be a dict, list, or any other type.
        converter: Function to convert string keys (e.g., camelize, kebabize).

    Returns:
        The transformed data structure with converted keys. Non-dict values
        are returned unchanged.

    Examples:
        Transform snake_case keys to camelCase:

        >>> from sqlspec.utils.text import camelize
        >>> data = {"user_id": 123, "created_at": "2024-01-01"}
        >>> transform_dict_keys(data, camelize)
        {"userId": 123, "createdAt": "2024-01-01"}

        Transform nested structures:

        >>> nested = {
        ...     "user_data": {"first_name": "John", "last_name": "Doe"},
        ...     "order_items": [
        ...         {"item_id": 1, "item_name": "Product A"},
        ...         {"item_id": 2, "item_name": "Product B"},
        ...     ],
        ... }
        >>> transform_dict_keys(nested, camelize)
        {
            "userData": {
                "firstName": "John",
                "lastName": "Doe"
            },
            "orderItems": [
                {"itemId": 1, "itemName": "Product A"},
                {"itemId": 2, "itemName": "Product B"}
            ]
        }
    """
    if isinstance(data, dict):
        return _transform_dict(data, converter)
    if isinstance(data, list):
        return _transform_list(data, converter)
    return data


def _transform_dict(data: dict, converter: Callable[[str], str]) -> dict:
    """Transform a dictionary's keys recursively.

    Args:
        data: Dictionary to transform.
        converter: Function to convert string keys.

    Returns:
        Dictionary with transformed keys and recursively transformed values.
    """
    transformed = {}

    for key, value in data.items():
        converted_key = _safe_convert_key(key, converter)
        transformed_value = transform_dict_keys(value, converter)
        transformed[converted_key] = transformed_value

    return transformed


def _transform_list(data: list, converter: Callable[[str], str]) -> list:
    """Transform a list's elements recursively.

    Args:
        data: List to transform.
        converter: Function to convert string keys in nested structures.

    Returns:
        List with recursively transformed elements.
    """
    return [transform_dict_keys(item, converter) for item in data]


# =============================================================================
# Schema Type Detection
# =============================================================================


def _is_list_type_target(target_type: Any) -> "TypeGuard[list[object]]":
    """Check if target type is a list type (e.g., list[float])."""
    try:
        origin = target_type.__origin__
    except (AttributeError, TypeError):
        return False
    return origin is list


def _convert_numpy_to_list(target_type: Any, value: Any) -> Any:
    """Convert numpy array to list if target is a list type."""
    if not NUMPY_INSTALLED:
        return value

    import numpy as np

    if isinstance(value, np.ndarray) and _is_list_type_target(target_type):
        return value.tolist()

    return value


@lru_cache(maxsize=128)
def _detect_schema_type(schema_type: type) -> "str | None":
    """Detect schema type with LRU caching.

    Args:
        schema_type: Type to detect

    Returns:
        Type identifier string or None if unsupported
    """
    return (
        "typed_dict"
        if is_typed_dict(schema_type)
        else "dataclass"
        if is_dataclass(schema_type)
        else "msgspec"
        if is_msgspec_struct(schema_type)
        else "pydantic"
        if is_pydantic_model(schema_type)
        else "attrs"
        if is_attrs_schema(schema_type)
        else None
    )


def _is_foreign_key_metadata_type(schema_type: type) -> bool:
    if schema_type.__name__ != "ForeignKeyMetadata":
        return False

    # Check module for stronger guarantee without importing
    module = getattr(schema_type, "__module__", "")
    if "sqlspec" in module and ("driver" in module or "data_dictionary" in module):
        return True

    slots = getattr(schema_type, "__slots__", None)
    if not slots:
        return False
    return {"table_name", "column_name", "referenced_table", "referenced_column"}.issubset(set(slots))


def _convert_foreign_key_metadata(data: Any, schema_type: Any) -> Any:
    if not is_dict(data):
        return data
    payload = {
        "table_name": data.get("table_name") or data.get("table"),
        "column_name": data.get("column_name") or data.get("column"),
        "referenced_table": data.get("referenced_table") or data.get("referenced_table_name"),
        "referenced_column": data.get("referenced_column") or data.get("referenced_column_name"),
        "constraint_name": data.get("constraint_name"),
        "schema": data.get("schema") or data.get("table_schema"),
        "referenced_schema": data.get("referenced_schema") or data.get("referenced_table_schema"),
    }
    return schema_type(**payload)


def _convert_typed_dict(data: Any, schema_type: Any) -> Any:
    """Convert data to TypedDict."""
    return [item for item in data if is_dict(item)] if isinstance(data, list) else data


def _convert_dataclass(data: Any, schema_type: Any) -> Any:
    """Convert data to dataclass."""
    if isinstance(data, list):
        return [schema_type(**dict(item)) if is_dict(item) else item for item in data]
    return schema_type(**dict(data)) if is_dict(data) else (schema_type(**data) if isinstance(data, dict) else data)


class _IsTypePredicate:
    """Callable predicate to check if a type matches a target type."""

    __slots__ = ("_type",)

    def __init__(self, target_type: type) -> None:
        self._type = target_type

    def __call__(self, x: Any) -> bool:
        return x is self._type


class _UUIDDecoder:
    """Decoder for UUID types."""

    __slots__ = ()

    def __call__(self, t: type, v: Any) -> Any:
        return t(v.hex)


class _ISOFormatDecoder:
    """Decoder for types with isoformat() method (datetime, date, time)."""

    __slots__ = ()

    def __call__(self, t: type, v: Any) -> Any:
        return t(v.isoformat())


class _EnumDecoder:
    """Decoder for Enum types."""

    __slots__ = ()

    def __call__(self, t: type, v: Any) -> Any:
        return t(v.value)


_DEFAULT_TYPE_DECODERS: Final[list[tuple[Callable[[Any], bool], Callable[[Any, Any], Any]]]] = [
    (_IsTypePredicate(UUID), _UUIDDecoder()),
    (_IsTypePredicate(datetime.datetime), _ISOFormatDecoder()),
    (_IsTypePredicate(datetime.date), _ISOFormatDecoder()),
    (_IsTypePredicate(datetime.time), _ISOFormatDecoder()),
    (_IsTypePredicate(Enum), _EnumDecoder()),
    (_is_list_type_target, _convert_numpy_to_list),
]


def _default_msgspec_deserializer(
    target_type: Any, value: Any, type_decoders: "Sequence[tuple[Any, Any]] | None" = None
) -> Any:
    """Convert msgspec types with type decoder support.

    Args:
        target_type: Type to convert to
        value: Value to convert
        type_decoders: Optional sequence of (predicate, decoder) pairs

    Returns:
        Converted value or original value if conversion not applicable
    """
    if NUMPY_INSTALLED:
        import numpy as np

        if isinstance(value, np.ndarray) and _is_list_type_target(target_type):
            return value.tolist()

    if type_decoders:
        for predicate, decoder in type_decoders:
            if predicate(target_type):
                return decoder(target_type, value)

    if target_type is UUID and isinstance(value, UUID):
        return value.hex

    if target_type in _DATETIME_TYPES and isinstance(value, _DATETIME_TYPE_TUPLE):
        datetime_value = cast("datetime.datetime | datetime.date | datetime.time", value)
        return datetime_value.isoformat()

    if isinstance(target_type, type) and issubclass(target_type, Enum) and isinstance(value, Enum):
        return value.value

    try:
        if isinstance(target_type, type) and isinstance(value, target_type):
            return value
    except TypeError:
        pass

    if isinstance(target_type, type):
        try:
            if issubclass(target_type, (Path, PurePath)) or issubclass(target_type, UUID):
                return target_type(str(value))
        except (TypeError, ValueError):
            pass

    return value


def _convert_numpy_recursive(obj: Any) -> Any:
    """Recursively convert numpy arrays to lists.

    This is a module-level function to avoid nested function definitions
    which are problematic for mypyc compilation.

    Args:
        obj: Object to convert (may contain numpy arrays nested in dicts/lists)

    Returns:
        Object with all numpy arrays converted to lists
    """
    if not NUMPY_INSTALLED:
        return obj

    import numpy as np

    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: _convert_numpy_recursive(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        converted = [_convert_numpy_recursive(item) for item in obj]
        return type(obj)(converted)
    return obj


def _convert_msgspec(data: Any, schema_type: Any) -> Any:
    """Convert data to msgspec Struct."""
    rename_config = get_msgspec_rename_config(schema_type)
    deserializer = partial(_default_msgspec_deserializer, type_decoders=_DEFAULT_TYPE_DECODERS)

    transformed_data = data
    if (rename_config and is_dict(data)) or (isinstance(data, Sequence) and data and is_dict(data[0])):
        try:
            converter_map: dict[str, Callable[[str], str]] = {"camel": camelize, "kebab": kebabize, "pascal": pascalize}
            converter = converter_map.get(rename_config) if rename_config else None
            if converter:
                transformed_data = (
                    [transform_dict_keys(item, converter) if is_dict(item) else item for item in data]
                    if isinstance(data, Sequence)
                    else (transform_dict_keys(data, converter) if is_dict(data) else data)
                )
        except Exception as e:
            logger.debug("Field name transformation failed for msgspec schema: %s", e)

    if NUMPY_INSTALLED:
        transformed_data = _convert_numpy_recursive(transformed_data)

    return convert(
        obj=transformed_data,
        type=(list[schema_type] if isinstance(transformed_data, Sequence) else schema_type),
        from_attributes=True,
        dec_hook=deserializer,
    )


def _convert_pydantic(data: Any, schema_type: Any) -> Any:
    """Convert data to Pydantic model."""
    if isinstance(data, Sequence):
        return get_type_adapter(list[schema_type]).validate_python(data, from_attributes=True)
    return get_type_adapter(schema_type).validate_python(data, from_attributes=True)


def _convert_attrs(data: Any, schema_type: Any) -> Any:
    """Convert data to attrs class."""
    if CATTRS_INSTALLED:
        if isinstance(data, Sequence):
            return cattrs_structure(data, list[schema_type])
        structured = cattrs_unstructure(data) if is_attrs_instance(data) else data
        return cattrs_structure(structured, schema_type)

    if isinstance(data, list):
        return [schema_type(**dict(item)) if is_dict(item) else schema_type(**attrs_asdict(item)) for item in data]
    return schema_type(**dict(data)) if is_dict(data) else data


_SCHEMA_CONVERTERS: "dict[str, Callable[[Any, Any], Any]]" = {
    "typed_dict": _convert_typed_dict,
    "dataclass": _convert_dataclass,
    "msgspec": _convert_msgspec,
    "pydantic": _convert_pydantic,
    "attrs": _convert_attrs,
}


@overload
def to_schema(data: "list[DataT]", *, schema_type: "type[SchemaT]") -> "list[SchemaT]": ...
@overload
def to_schema(data: "list[DataT]", *, schema_type: None = None) -> "list[DataT]": ...
@overload
def to_schema(data: "DataT", *, schema_type: "type[SchemaT]") -> "SchemaT": ...
@overload
def to_schema(data: "DataT", *, schema_type: None = None) -> "DataT": ...


def to_schema(data: Any, *, schema_type: Any = None) -> Any:
    """Convert data to a specified schema type.

    Supports transformation to various schema types including:
    - TypedDict
    - dataclasses
    - msgspec Structs
    - Pydantic models
    - attrs classes

    Args:
        data: Input data to convert (dict, list of dicts, or other)
        schema_type: Target schema type for conversion. If None, returns data unchanged.

    Returns:
        Converted data in the specified schema type, or original data if schema_type is None

    Raises:
        SQLSpecError: If schema_type is not a supported type
    """
    if schema_type is None:
        return data

    schema_type_key = _detect_schema_type(schema_type)
    if schema_type_key is None:
        if _is_foreign_key_metadata_type(schema_type):
            if isinstance(data, list):
                return [_convert_foreign_key_metadata(item, schema_type) for item in data]
            return _convert_foreign_key_metadata(data, schema_type)
        msg = "`schema_type` should be a valid Dataclass, Pydantic model, Msgspec struct, Attrs class, or TypedDict"
        raise SQLSpecError(msg)

    return _SCHEMA_CONVERTERS[schema_type_key](data, schema_type)


# =============================================================================
# Scalar Type Conversion
# =============================================================================


def _ensure_json_parsed(value: Any) -> Any:
    """Parse JSON string if needed, otherwise return as-is.

    This helper is used when converting database values (potentially JSON strings
    from JSONB columns) to schema types like Pydantic models or dataclasses.

    Args:
        value: The value to potentially parse. If it's a string, attempts JSON parsing.

    Returns:
        Parsed JSON object if value was a valid JSON string, otherwise the original value.
    """
    if isinstance(value, str):
        try:
            return from_json(value)
        except Exception:
            return value
    return value


def _try_parse_json(value: str) -> Any:
    """Attempt to parse a JSON string, returning None on failure.

    Args:
        value: JSON string to parse.

    Returns:
        Parsed JSON value, or None if parsing fails.
    """
    try:
        return from_json(value)
    except Exception:
        return None


# Boolean true values for string conversion
_BOOL_TRUE_VALUES: Final[frozenset[str]] = frozenset({"true", "1", "yes", "y", "t", "on"})


def _convert_to_int(value: Any) -> int:
    """Convert a value to int.

    Args:
        value: Value to convert.

    Returns:
        Converted integer value.

    Raises:
        TypeError: If value cannot be converted to int.
    """
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, Decimal)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            # Try parsing as float first for values like "42.0"
            try:
                return int(float(value))
            except ValueError:
                pass
    msg = f"Cannot convert {type(value).__name__} to int"
    raise TypeError(msg)


def _convert_to_float(value: Any) -> float:
    """Convert a value to float.

    Args:
        value: Value to convert.

    Returns:
        Converted float value.

    Raises:
        TypeError: If value cannot be converted to float.
    """
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
    msg = f"Cannot convert {type(value).__name__} to float"
    raise TypeError(msg)


def _convert_to_bool(value: Any) -> bool:
    """Convert a value to bool.

    Args:
        value: Value to convert.

    Returns:
        Converted boolean value.

    Raises:
        TypeError: If value cannot be converted to bool.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.lower() in _BOOL_TRUE_VALUES
    msg = f"Cannot convert {type(value).__name__} to bool"
    raise TypeError(msg)


def _convert_to_datetime(value: Any) -> datetime.datetime:
    """Convert a value to datetime.

    Args:
        value: Value to convert.

    Returns:
        Converted datetime value.

    Raises:
        TypeError: If value cannot be converted to datetime.
    """
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.datetime.fromisoformat(value)
        except ValueError:
            pass
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return datetime.datetime.combine(value, datetime.time.min)
    msg = f"Cannot convert {type(value).__name__} to datetime"
    raise TypeError(msg)


def _convert_to_date(value: Any) -> datetime.date:
    """Convert a value to date.

    Args:
        value: Value to convert.

    Returns:
        Converted date value.

    Raises:
        TypeError: If value cannot be converted to date.
    """
    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        try:
            # Try ISO format first
            return datetime.date.fromisoformat(value)
        except ValueError:
            # Try parsing as datetime and extracting date
            try:
                return datetime.datetime.fromisoformat(value).date()
            except ValueError:
                pass
    msg = f"Cannot convert {type(value).__name__} to date"
    raise TypeError(msg)


def _convert_to_time(value: Any) -> datetime.time:
    """Convert a value to time.

    Args:
        value: Value to convert.

    Returns:
        Converted time value.

    Raises:
        TypeError: If value cannot be converted to time.
    """
    if isinstance(value, datetime.datetime):
        return value.time()
    if isinstance(value, datetime.time):
        return value
    if isinstance(value, str):
        try:
            return datetime.time.fromisoformat(value)
        except ValueError:
            pass
    msg = f"Cannot convert {type(value).__name__} to time"
    raise TypeError(msg)


def _convert_to_decimal(value: Any) -> Decimal:
    """Convert a value to Decimal.

    Args:
        value: Value to convert.

    Returns:
        Converted Decimal value.

    Raises:
        TypeError: If value cannot be converted to Decimal.
    """
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            pass
    msg = f"Cannot convert {type(value).__name__} to Decimal"
    raise TypeError(msg)


def _convert_to_uuid(value: Any) -> UUID:
    """Convert a value to UUID.

    Args:
        value: Value to convert.

    Returns:
        Converted UUID value.

    Raises:
        TypeError: If value cannot be converted to UUID.
    """
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            pass
    if isinstance(value, bytes):
        try:
            return UUID(bytes=value)
        except ValueError:
            pass
    msg = f"Cannot convert {type(value).__name__} to UUID"
    raise TypeError(msg)


def _convert_to_path(value: Any) -> Path:
    """Convert a value to Path.

    Args:
        value: Value to convert.

    Returns:
        Converted Path value.

    Raises:
        TypeError: If value cannot be converted to Path.
    """
    if isinstance(value, Path):
        return value
    if isinstance(value, (str, PurePath)):
        return Path(value)
    msg = f"Cannot convert {type(value).__name__} to Path"
    raise TypeError(msg)


def _convert_to_dict(value: Any) -> dict[str, Any]:
    """Convert a value to dict.

    This is useful for JSON/JSONB database columns where the driver may return
    either a dict (already parsed) or a string (needs parsing).

    Args:
        value: Value to convert.

    Returns:
        Converted dict value.

    Raises:
        TypeError: If value cannot be converted to dict.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        parsed = _try_parse_json(value)
        if parsed is not None:
            if isinstance(parsed, dict):
                return parsed
            msg = f"JSON string did not parse to dict, got {type(parsed).__name__}"
            raise TypeError(msg)
    msg = f"Cannot convert {type(value).__name__} to dict"
    raise TypeError(msg)


def _convert_to_list(value: Any) -> list[Any]:
    """Convert a value to list.

    This is useful for JSON array database columns where the driver may return
    either a list (already parsed) or a string (needs parsing).

    Args:
        value: Value to convert.

    Returns:
        Converted list value.

    Raises:
        TypeError: If value cannot be converted to list.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = _try_parse_json(value)
        if parsed is not None:
            if isinstance(parsed, list):
                return parsed
            msg = f"JSON string did not parse to list, got {type(parsed).__name__}"
            raise TypeError(msg)
    if isinstance(value, (tuple, set, frozenset)):
        return list(value)
    msg = f"Cannot convert {type(value).__name__} to list"
    raise TypeError(msg)


def to_value_type(value: Any, value_type: "type[ValueT]") -> "ValueT":
    """Convert a database value to the specified Python type.

    This function handles type conversion for common database return values,
    providing runtime type safety for scalar queries. When the value is already
    the correct type, it is returned as-is without conversion overhead.

    Also supports schema types (Pydantic models, dataclasses, msgspec Structs,
    attrs classes, and TypedDict). For schema types, JSON strings are automatically
    parsed before conversion.

    Args:
        value: The value to convert.
        value_type: The target Python type. Supported types include:
            - Primitives: int, float, str, bool
            - Temporal: datetime, date, time
            - Numeric: Decimal
            - Identifiers: UUID, Path
            - Collections: dict, list (for JSON/JSONB columns)
            - Schema types: Pydantic models, dataclasses, msgspec Structs,
              attrs classes, TypedDict (for JSONB columns)

    Returns:
        The converted value of the specified type.

    Raises:
        TypeError: If the value cannot be converted to the specified type.

    Examples:
        Convert string to int:

        >>> to_value_type("42", int)
        42

        Convert Decimal to float:

        >>> from decimal import Decimal
        >>> to_value_type(Decimal("3.14"), float)
        3.14

        Convert string to UUID:

        >>> from uuid import UUID
        >>> to_value_type("550e8400-e29b-41d4-a716-446655440000", UUID)
        UUID('550e8400-e29b-41d4-a716-446655440000')

        Convert JSON string to dict:

        >>> to_value_type('{"key": "value"}', dict)
        {'key': 'value'}

        Convert JSONB to Pydantic model:

        >>> from pydantic import BaseModel
        >>> class User(BaseModel):
        ...     name: str
        ...     email: str
        >>> to_value_type(
        ...     '{"name": "Alice", "email": "alice@example.com"}', User
        ... )
        User(name='Alice', email='alice@example.com')

        Identity conversion (no overhead when type matches exactly):

        >>> value = 42
        >>> result = to_value_type(value, int)
        >>> result is value
        True

        Bool to int conversion (bool is a subclass of int, but converts correctly):

        >>> to_value_type(True, int)
        1
        >>> to_value_type(False, int)
        0
    """
    # Check if value_type is a schema type (Pydantic, dataclass, msgspec, attrs, TypedDict)
    # This must come before the fast path to handle these types correctly
    schema_type_key = _detect_schema_type(value_type)  # type: ignore[arg-type]
    if schema_type_key is not None:
        parsed = _ensure_json_parsed(value)
        return cast("ValueT", to_schema(parsed, schema_type=value_type))

    # Fast path: already correct type
    # Use strict type check for types with problematic subclass relationships:
    # - bool is a subclass of int (isinstance(True, int) is True)
    # - datetime is a subclass of date (isinstance(datetime(...), date) is True)
    # For these types, we must use type() identity to ensure correct behavior
    if value_type in (int, bool, datetime.date, datetime.time):
        # Strict check: type must match exactly
        if type(value) is value_type:
            return value
    elif isinstance(value, value_type):
        # Safe to use isinstance for types without problematic subclasses
        return value

    # Type-specific conversions
    if value_type is int:
        return cast("ValueT", _convert_to_int(value))
    if value_type is float:
        return cast("ValueT", _convert_to_float(value))
    if value_type is str:
        return cast("ValueT", str(value))
    if value_type is bool:
        return cast("ValueT", _convert_to_bool(value))
    if value_type is datetime.datetime:
        return cast("ValueT", _convert_to_datetime(value))
    if value_type is datetime.date:
        return cast("ValueT", _convert_to_date(value))
    if value_type is datetime.time:
        return cast("ValueT", _convert_to_time(value))
    if value_type is Decimal:
        return cast("ValueT", _convert_to_decimal(value))
    if value_type is UUID:
        return cast("ValueT", _convert_to_uuid(value))
    if value_type is Path:
        return cast("ValueT", _convert_to_path(value))
    if value_type is dict:
        return cast("ValueT", _convert_to_dict(value))
    if value_type is list:
        return cast("ValueT", _convert_to_list(value))

    # Fallback: try direct construction
    try:
        return value_type(value)  # type: ignore[call-arg]
    except (TypeError, ValueError) as e:
        msg = f"Cannot convert {type(value).__name__} to {value_type.__name__}"
        raise TypeError(msg) from e
