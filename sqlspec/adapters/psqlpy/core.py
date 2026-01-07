"""psqlpy adapter compiled helpers."""

import datetime
import decimal
import io
import re
import uuid
from typing import TYPE_CHECKING, Any, Final

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import SQLSpecError
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_nested_decimal_normalizer

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import ParameterStyleConfig

__all__ = (
    "apply_psqlpy_driver_features",
    "build_psqlpy_insert_statement",
    "build_psqlpy_parameter_config",
    "build_psqlpy_profile",
    "build_psqlpy_statement_config",
    "coerce_numeric_for_write",
    "coerce_parameter_for_cast",
    "coerce_records_for_execute_many",
    "encode_records_for_binary_copy",
    "format_table_identifier",
    "normalize_scalar_parameter",
    "parse_psqlpy_command_tag",
    "prepare_dict_parameter",
    "prepare_list_parameter",
    "prepare_tuple_parameter",
    "psqlpy_statement_config",
    "split_schema_and_table",
)


_JSON_CASTS: Final[frozenset[str]] = frozenset({"JSON", "JSONB"})
_TIMESTAMP_CASTS: Final[frozenset[str]] = frozenset({
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIMESTAMP WITH TIME ZONE",
    "TIMESTAMP WITHOUT TIME ZONE",
})
_UUID_CASTS: Final[frozenset[str]] = frozenset({"UUID"})
_DECIMAL_NORMALIZER = build_nested_decimal_normalizer(mode="float")
_JSONB_TYPE: "type[Any] | None" = None
_JSONB_RESOLVED: bool = False
PSQLPY_STATUS_REGEX: "re.Pattern[str]" = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)


def _get_jsonb_type() -> "type[Any] | None":
    global _JSONB_TYPE, _JSONB_RESOLVED
    if _JSONB_RESOLVED:
        return _JSONB_TYPE
    try:
        from psqlpy.extra_types import JSONB
    except ImportError:
        _JSONB_TYPE = None
    else:
        _JSONB_TYPE = JSONB
    _JSONB_RESOLVED = True
    return _JSONB_TYPE


def _coerce_json_parameter(value: Any, cast_type: str, serializer: "Callable[[Any], str]") -> Any:
    """Serialize JSON parameters according to the detected cast type."""

    if value is None:
        return None
    jsonb_type = _get_jsonb_type()
    if cast_type == "JSONB":
        if jsonb_type is not None and isinstance(value, jsonb_type):
            return value
        if jsonb_type is not None:
            if isinstance(value, dict):
                return jsonb_type(value)
            if isinstance(value, (list, tuple)):
                return jsonb_type(list(value))
    if isinstance(value, tuple):
        return list(value)
    if jsonb_type is not None and isinstance(value, jsonb_type):
        return value
    if isinstance(value, (dict, list, str)):
        return value
    try:
        serialized_value = serializer(value)
    except Exception as error:
        msg = "Failed to serialize JSON parameter for psqlpy."
        raise SQLSpecError(msg) from error
    return serialized_value


def _coerce_uuid_parameter(value: Any) -> Any:
    """Convert UUID-compatible parameters to ``uuid.UUID`` instances."""

    if isinstance(value, uuid.UUID):
        return value
    if isinstance(value, str):
        try:
            return uuid.UUID(value)
        except ValueError as error:
            msg = "Invalid UUID parameter for psqlpy."
            raise SQLSpecError(msg) from error
    return value


def _coerce_timestamp_parameter(value: Any) -> Any:
    """Convert ISO-formatted timestamp strings to ``datetime.datetime``."""

    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, str):
        normalized_value = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return datetime.datetime.fromisoformat(normalized_value)
        except ValueError as error:
            msg = "Invalid ISO timestamp parameter for psqlpy."
            raise SQLSpecError(msg) from error
    return value


def coerce_parameter_for_cast(value: Any, cast_type: str, serializer: "Callable[[Any], str]") -> Any:
    """Apply cast-aware coercion for psqlpy parameters."""

    upper_cast = cast_type.upper()
    if upper_cast in _JSON_CASTS:
        return _coerce_json_parameter(value, upper_cast, serializer)
    if upper_cast in _UUID_CASTS:
        return _coerce_uuid_parameter(value)
    if upper_cast in _TIMESTAMP_CASTS:
        return _coerce_timestamp_parameter(value)
    return value


def prepare_dict_parameter(value: "dict[str, Any]") -> "dict[str, Any]":
    normalized = _DECIMAL_NORMALIZER(value)
    return normalized if isinstance(normalized, dict) else value


def prepare_list_parameter(value: "list[Any]") -> "list[Any]":
    return [_DECIMAL_NORMALIZER(item) for item in value]


def prepare_tuple_parameter(value: "tuple[Any, ...]") -> "tuple[Any, ...]":
    return tuple(_DECIMAL_NORMALIZER(item) for item in value)


def build_psqlpy_profile() -> "DriverParameterProfile":
    """Create the psqlpy driver parameter profile."""

    return DriverParameterProfile(
        name="Psqlpy",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR, ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.NUMERIC,
        supported_execution_styles={ParameterStyle.NUMERIC},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={decimal.Decimal: float},
        default_dialect="postgres",
    )


def build_psqlpy_parameter_config(
    profile: "DriverParameterProfile", serializer: "Callable[[Any], str]"
) -> "ParameterStyleConfig":
    """Construct parameter configuration for psqlpy.

    Args:
        profile: Driver parameter profile to extend.
        serializer: JSON serializer for parameter coercion.

    Returns:
        ParameterStyleConfig with updated type coercions.
    """

    base_config = build_statement_config_from_profile(profile, json_serializer=serializer).parameter_config

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[dict] = prepare_dict_parameter
    updated_type_map[list] = prepare_list_parameter
    updated_type_map[tuple] = prepare_tuple_parameter

    return base_config.replace(type_coercion_map=updated_type_map)


def build_psqlpy_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> "StatementConfig":
    """Construct the psqlpy statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    parameter_config = build_psqlpy_parameter_config(build_psqlpy_profile(), serializer)
    base_config = build_statement_config_from_profile(build_psqlpy_profile(), json_serializer=serializer)
    return base_config.replace(parameter_config=parameter_config)


psqlpy_statement_config = build_psqlpy_statement_config()


def apply_psqlpy_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply psqlpy driver feature defaults to statement config."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = processed_driver_features.get("json_serializer", to_json)
    processed_driver_features.setdefault("json_serializer", serializer)
    processed_driver_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)

    parameter_config = build_psqlpy_parameter_config(build_psqlpy_profile(), serializer)
    statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_driver_features


def normalize_scalar_parameter(value: Any) -> Any:
    return value


def coerce_numeric_for_write(value: Any) -> Any:
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, list):
        return [coerce_numeric_for_write(item) for item in value]
    if isinstance(value, tuple):
        coerced = [coerce_numeric_for_write(item) for item in value]
        return tuple(coerced)
    if isinstance(value, dict):
        return {key: coerce_numeric_for_write(item) for key, item in value.items()}
    return value


def _escape_copy_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _format_copy_value(value: Any) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (datetime.date, datetime.datetime, datetime.time)):
        return value.isoformat()
    if isinstance(value, (list, tuple, dict)):
        return to_json(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8")
    return str(coerce_numeric_for_write(value))


def encode_records_for_binary_copy(records: "list[tuple[Any, ...]]") -> bytes:
    """Encode row tuples into a bytes payload compatible with binary_copy_to_table."""

    buffer = io.StringIO()
    for record in records:
        encoded_columns = [_escape_copy_text(_format_copy_value(value)) for value in record]
        buffer.write("\t".join(encoded_columns))
        buffer.write("\n")
    return buffer.getvalue().encode("utf-8")


def split_schema_and_table(identifier: str) -> "tuple[str | None, str]":
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    if "." not in cleaned:
        return None, cleaned.strip('"')
    parts = [part for part in cleaned.split(".") if part]
    if len(parts) == 1:
        return None, parts[0].strip('"')
    schema_name = ".".join(parts[:-1]).strip('"')
    table_name = parts[-1].strip('"')
    if not table_name:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    return schema_name or None, table_name


def parse_psqlpy_command_tag(tag: str) -> int:
    """Parse PostgreSQL command tag to extract rows affected.

    Args:
        tag: PostgreSQL command tag string.

    Returns:
        Number of rows affected, -1 if unable to parse.
    """
    if not tag:
        return -1

    match = PSQLPY_STATUS_REGEX.match(tag.strip())
    if match:
        command = match.group(1).upper()
        if command == "INSERT" and match.group(3):
            return int(match.group(3))
        if command in {"UPDATE", "DELETE"} and match.group(3):
            return int(match.group(3))
    return -1


def _quote_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def format_table_identifier(identifier: str) -> str:
    schema_name, table_name = split_schema_and_table(identifier)
    if schema_name:
        return f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    return _quote_identifier(table_name)


def build_psqlpy_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    return f"INSERT INTO {format_table_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def coerce_records_for_execute_many(records: "list[tuple[Any, ...]]") -> "list[list[Any]]":
    formatted_records: list[list[Any]] = []
    for record in records:
        coerced = coerce_numeric_for_write(record)
        if isinstance(coerced, tuple):
            formatted_records.append(list(coerced))
        elif isinstance(coerced, list):
            formatted_records.append(coerced)
        else:
            formatted_records.append([coerced])
    return formatted_records
