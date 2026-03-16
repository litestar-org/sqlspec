"""psqlpy adapter compiled helpers."""

import datetime
import decimal
import io
import re
import uuid
from typing import TYPE_CHECKING, Any, Final, cast

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.typing import PGVECTOR_INSTALLED, Empty
from sqlspec.utils.dispatch import TypeDispatcher
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
from sqlspec.utils.type_converters import build_nested_decimal_normalizer, build_uuid_coercions
from sqlspec.utils.type_guards import has_query_result_metadata

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import SQL, ParameterStyleConfig, StatementConfig

__all__ = (
    "apply_driver_features",
    "build_connection_config",
    "build_insert_statement",
    "build_postgres_extension_probe_names",
    "build_profile",
    "build_statement_config",
    "coerce_numeric_for_write",
    "coerce_records_for_execute_many",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "encode_records_for_binary_copy",
    "extract_rows_affected",
    "format_execute_many_parameters",
    "format_table_identifier",
    "get_parameter_casts",
    "normalize_scalar_parameter",
    "prepare_parameters_with_casts",
    "resolve_postgres_extension_state",
    "resolve_runtime_statement_config",
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
_TYPE_COERCION_DISPATCHERS: "dict[tuple[tuple[type, Callable[[Any], Any]], ...], TypeDispatcher[Callable[[Any], Any]]]" = {}
PSQLPY_STATUS_REGEX: "re.Pattern[str]" = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

logger = get_logger("sqlspec.adapters.psqlpy.core")
_NUMERIC_COERCE_TYPES: "tuple[type[Any], ...]" = (float, decimal.Decimal, list, tuple, dict)


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


def _coerce_parameter_for_cast(value: Any, cast_type: str, serializer: "Callable[[Any], str]") -> Any:
    """Apply cast-aware coercion for psqlpy parameters."""

    upper_cast = cast_type.upper()
    if upper_cast in _JSON_CASTS:
        return _coerce_json_parameter(value, upper_cast, serializer)
    if upper_cast in _UUID_CASTS:
        return _coerce_uuid_parameter(value)
    if upper_cast in _TIMESTAMP_CASTS:
        return _coerce_timestamp_parameter(value)
    return value


def _prepare_dict_parameter(value: "dict[str, Any]") -> "dict[str, Any]":
    normalized = _DECIMAL_NORMALIZER(value)
    return normalized if isinstance(normalized, dict) else value


def _prepare_list_parameter(value: "list[Any]") -> "list[Any]":
    return [_DECIMAL_NORMALIZER(item) for item in value]


def _prepare_tuple_parameter(value: "tuple[Any, ...]") -> "tuple[Any, ...]":
    return tuple(_DECIMAL_NORMALIZER(item) for item in value)


def build_profile() -> "DriverParameterProfile":
    """Create the psqlpy driver parameter profile."""
    coercions: dict[type, Callable[[Any], Any]] = {decimal.Decimal: float, **build_uuid_coercions(native=True)}
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
        custom_type_coercions=coercions,
        default_dialect="postgres",
    )


driver_profile = build_profile()


def _build_psqlpy_parameter_config(
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
    updated_type_map[dict] = _prepare_dict_parameter
    updated_type_map[list] = _prepare_list_parameter
    updated_type_map[tuple] = _prepare_tuple_parameter

    return base_config.replace(type_coercion_map=updated_type_map)


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> "StatementConfig":
    """Construct the psqlpy statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    profile = driver_profile
    parameter_config = _build_psqlpy_parameter_config(profile, serializer)
    base_config = build_statement_config_from_profile(profile, json_serializer=serializer)
    return base_config.replace(parameter_config=parameter_config)


default_statement_config = build_statement_config()


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build connection configuration with non-null values only.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    return {key: value for key, value in connection_config.items() if value is not None}


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply psqlpy driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = features.get("json_serializer", to_json)
    features.setdefault("json_serializer", serializer)
    features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
    features.setdefault("enable_paradedb", True)

    parameter_config = _build_psqlpy_parameter_config(driver_profile, serializer)
    statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def collect_rows(query_result: Any | None) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect psqlpy rows and column names.

    Args:
        query_result: Result returned from cursor.fetch().

    Returns:
        Tuple of (rows, column_names).
    """
    if not query_result:
        return [], []

    dict_rows = cast("list[dict[str, Any]]", query_result if isinstance(query_result, list) else query_result.result())
    if not dict_rows:
        return [], []
    return dict_rows, list(dict_rows[0])


def normalize_scalar_parameter(value: Any) -> Any:
    return value


def coerce_numeric_for_write(value: Any) -> Any:
    if isinstance(value, float):
        return decimal.Decimal(str(value))
    if isinstance(value, decimal.Decimal):
        return value
    if isinstance(value, list):
        coerced_list: list[Any] | None = None
        for index, item in enumerate(value):
            coerced_item = coerce_numeric_for_write(item)
            if coerced_list is None:
                if coerced_item is item:
                    continue
                coerced_list = list(value[:index])
            coerced_list.append(coerced_item)
        return value if coerced_list is None else coerced_list
    if isinstance(value, tuple):
        coerced_tuple: list[Any] | None = None
        for index, item in enumerate(value):
            coerced_item = coerce_numeric_for_write(item)
            if coerced_tuple is None:
                if coerced_item is item:
                    continue
                coerced_tuple = list(value[:index])
            coerced_tuple.append(coerced_item)
        return value if coerced_tuple is None else tuple(coerced_tuple)
    if isinstance(value, dict):
        coerced_dict: dict[Any, Any] | None = None
        for key, item in value.items():
            coerced_item = coerce_numeric_for_write(item)
            if coerced_dict is None:
                if coerced_item is item:
                    continue
                coerced_dict = dict(value)
            coerced_dict[key] = coerced_item
        return value if coerced_dict is None else coerced_dict
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


def _parse_psqlpy_command_tag(tag: str) -> int:
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


def extract_rows_affected(result: Any) -> int:
    """Extract rows affected from psqlpy results."""
    try:
        if has_query_result_metadata(result):
            if result.tag:
                return _parse_psqlpy_command_tag(result.tag)
            if result.status:
                return _parse_psqlpy_command_tag(result.status)
        if isinstance(result, str):
            return _parse_psqlpy_command_tag(result)
    except Exception as error:
        logger.debug("Failed to parse psqlpy command tag: %s", error)
    return -1


def get_parameter_casts(statement: "SQL") -> "dict[int, str]":
    """Get parameter cast metadata from compiled statements."""
    processed_state = statement.get_processed_state()
    if processed_state is not Empty:
        return processed_state.parameter_casts or {}
    return {}


def _get_type_coercion_dispatcher(
    type_map: "dict[type, Callable[[Any], Any]]",
) -> "TypeDispatcher[Callable[[Any], Any]]":
    fallback_items = tuple(type_map.items())
    dispatcher = _TYPE_COERCION_DISPATCHERS.get(fallback_items)
    if dispatcher is not None:
        return dispatcher

    dispatcher = TypeDispatcher["Callable[[Any], Any]"]()
    dispatcher.register_all(fallback_items)
    _TYPE_COERCION_DISPATCHERS[fallback_items] = dispatcher
    return dispatcher


def prepare_parameters_with_casts(
    parameters: Any, parameter_casts: "dict[int, str]", statement_config: "StatementConfig"
) -> Any:
    """Prepare parameters with cast-aware type coercion."""
    if isinstance(parameters, (list, tuple)):
        result: list[Any] = []
        serializer = statement_config.parameter_config.json_serializer or to_json
        type_map = statement_config.parameter_config.type_coercion_map
        dispatcher = _get_type_coercion_dispatcher(type_map) if type_map else None
        for idx, param in enumerate(parameters, start=1):
            cast_type = parameter_casts.get(idx, "")
            prepared_value = param
            if type_map and dispatcher is not None:
                exact_converter = type_map.get(type(prepared_value))
                if exact_converter is not None:
                    prepared_value = exact_converter(prepared_value)
                else:
                    fallback_converter = dispatcher.get(prepared_value)
                    if fallback_converter is not None:
                        prepared_value = fallback_converter(prepared_value)
            if cast_type:
                prepared_value = _coerce_parameter_for_cast(prepared_value, cast_type, serializer)
            result.append(prepared_value)
        return tuple(result) if isinstance(parameters, tuple) else result
    return parameters


def _create_postgres_error(error: Any, error_class: type[SQLSpecError], description: str) -> SQLSpecError:
    """Create a SQLSpec exception from a psqlpy error.

    Args:
        error: The original psqlpy exception
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"PostgreSQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any) -> SQLSpecError:
    """Map psqlpy exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    psqlpy doesn't expose SQLSTATE codes directly, so we rely on message-based
    pattern matching for exception classification.

    Args:
        error: The psqlpy exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    error_msg = str(error).lower()

    # Integrity constraint violations (most specific first)
    if "unique" in error_msg or "duplicate key" in error_msg:
        return _create_postgres_error(error, UniqueViolationError, "unique constraint violation")
    if "foreign key" in error_msg or "violates foreign key" in error_msg:
        return _create_postgres_error(error, ForeignKeyViolationError, "foreign key constraint violation")
    if "not null" in error_msg or ("null value" in error_msg and "violates not-null" in error_msg):
        return _create_postgres_error(error, NotNullViolationError, "not-null constraint violation")
    if "check constraint" in error_msg or "violates check constraint" in error_msg:
        return _create_postgres_error(error, CheckViolationError, "check constraint violation")
    if "constraint" in error_msg:
        return _create_postgres_error(error, IntegrityError, "integrity constraint violation")

    # Transaction and serialization errors (deadlock before serialization)
    if "deadlock" in error_msg:
        return _create_postgres_error(error, DeadlockError, "deadlock detected")
    if "serialization failure" in error_msg or "could not serialize" in error_msg:
        return _create_postgres_error(error, SerializationConflictError, "serialization failure")

    # Query timeout/cancellation
    if "cancel" in error_msg or "timeout" in error_msg or "statement timeout" in error_msg:
        return _create_postgres_error(error, QueryTimeoutError, "query canceled or timed out")

    # Permission/authentication errors
    if "permission denied" in error_msg or "insufficient privilege" in error_msg:
        return _create_postgres_error(error, PermissionDeniedError, "permission denied")
    if "authentication failed" in error_msg or "password" in error_msg:
        return _create_postgres_error(error, PermissionDeniedError, "authentication error")

    # Connection errors
    if "connection" in error_msg or "could not connect" in error_msg:
        if "timeout" in error_msg:
            return _create_postgres_error(error, ConnectionTimeoutError, "connection timeout")
        return _create_postgres_error(error, DatabaseConnectionError, "connection error")

    # SQL syntax errors
    if "syntax error" in error_msg or "parse" in error_msg:
        return _create_postgres_error(error, SQLParsingError, "SQL syntax error")

    return _create_postgres_error(error, SQLSpecError, "database error")


def _quote_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def format_table_identifier(identifier: str) -> str:
    schema_name, table_name = split_schema_and_table(identifier)
    if schema_name:
        return f"{_quote_identifier(schema_name)}.{_quote_identifier(table_name)}"
    return _quote_identifier(table_name)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_identifier(column) for column in columns)
    placeholders = ", ".join(f"${index}" for index in range(1, len(columns) + 1))
    return f"INSERT INTO {format_table_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def _sequence_needs_numeric_coercion(values: "list[Any] | tuple[Any, ...]") -> bool:
    """Return True when any value in a parameter sequence needs numeric coercion."""
    return any(type(value) in _NUMERIC_COERCE_TYPES for value in values)


def _format_execute_many_param_set(param_set: Any, *, coerce_numeric: bool) -> "list[Any]":
    """Normalize a single execute_many parameter set to list form."""
    if isinstance(param_set, list):
        if not coerce_numeric or not _sequence_needs_numeric_coercion(param_set):
            return param_set
        coerced = coerce_numeric_for_write(param_set)
        if isinstance(coerced, list):
            return coerced
        if isinstance(coerced, tuple):
            return list(coerced)
        return [coerced]

    if isinstance(param_set, tuple):
        if not coerce_numeric or not _sequence_needs_numeric_coercion(param_set):
            return list(param_set)
        coerced = coerce_numeric_for_write(param_set)
        if isinstance(coerced, tuple):
            return list(coerced)
        if isinstance(coerced, list):
            return coerced
        return [coerced]

    if coerce_numeric:
        coerced = coerce_numeric_for_write(param_set)
        if isinstance(coerced, list):
            return coerced
        if isinstance(coerced, tuple):
            return list(coerced)
        return [coerced]

    return [param_set]


def format_execute_many_parameters(parameters: Any, *, coerce_numeric: bool) -> "list[list[Any]]":
    """Normalize execute_many parameters for psqlpy.

    Args:
        parameters: Prepared parameter payload.
        coerce_numeric: Whether numeric write coercion should be applied.

    Returns:
        Parameter payload normalized to ``list[list[Any]]``.
    """
    if not parameters:
        return []

    if (
        isinstance(parameters, list)
        and not coerce_numeric
        and all(isinstance(param_set, list) for param_set in parameters)
    ):
        return cast("list[list[Any]]", parameters)

    if isinstance(parameters, (list, tuple)):
        formatted: list[list[Any]] = []
        append = formatted.append
        for param_set in parameters:
            append(_format_execute_many_param_set(param_set, coerce_numeric=coerce_numeric))
        return formatted

    return [_format_execute_many_param_set(parameters, coerce_numeric=coerce_numeric)]


def coerce_records_for_execute_many(records: "list[tuple[Any, ...]]") -> "list[list[Any]]":
    return format_execute_many_parameters(records, coerce_numeric=True)
