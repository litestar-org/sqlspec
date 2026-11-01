"""psqlpy adapter compiled helpers."""

import contextlib
import datetime as dt
import decimal
import re
import uuid
from collections.abc import Mapping
from functools import lru_cache
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, cast

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from sqlspec.adapters.psqlpy._typing import PSQLPY_JSONB as _JSONB_TYPE
from sqlspec.adapters.psqlpy._typing import (
    PsqlpyConnectionExecuteError,
    PsqlpyDataError,
    PsqlpyIntegrityError,
    PsqlpyOperationalError,
)
from sqlspec.adapters.psqlpy.type_converter import coerce_pgvector
from sqlspec.core import (
    DriverParameterProfile,
    ParameterStyle,
    ParameterStyleConfig,
    StatementConfig,
    build_statement_config_from_profile,
)
from sqlspec.core.config_runtime import (
    build_postgres_extension_probe_names,
    is_postgres_extension_active,
    resolve_postgres_extension_state,
    resolve_runtime_statement_config,
)
from sqlspec.core.parameters import type_coercion_dispatcher, type_coercion_fallbacks
from sqlspec.driver import CachedQuery
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
    _classify_timeout_or_cancellation,
)
from sqlspec.typing import PGVECTOR_INSTALLED, Empty
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_nested_decimal_normalizer, build_uuid_coercions
from sqlspec.utils.type_guards import has_query_result_metadata
from sqlspec.utils.uuids import uuid_from_string

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import SQL

__all__ = (
    "PsqlpyStreamSource",
    "apply_driver_features",
    "build_connection_config",
    "build_postgres_extension_probe_names",
    "build_postgres_storage_params_sql",
    "build_profile",
    "build_statement_config",
    "coerce_json_columns",
    "coerce_numeric_for_write",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "extract_rows_affected",
    "format_execute_many_parameters",
    "format_table_identifier",
    "get_parameter_casts",
    "is_postgres_extension_active",
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
PSQLPY_STATUS_REGEX: "re.Pattern[str]" = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)
_DML_COUNT_CTE_ALIAS: Final = "_sqlspec_affected"
_DML_COUNT_COLUMN: Final = "_sqlspec_rows_affected"
_DML_COUNT_QUERY_CACHE_SIZE: Final = 1024

logger = get_logger("sqlspec.adapters.psqlpy.core")
_NUMERIC_COERCE_TYPES: "tuple[type[Any], ...]" = (float, decimal.Decimal, list, tuple)


def build_postgres_storage_params_sql(
    settings: "dict[str, object] | Mapping[str, object] | None",
    *,
    include_autovacuum: bool = False,
    default_fillfactor: int | None = None,
    default_vacuum_scale: float | None = None,
    default_analyze_scale: float | None = None,
    section: str = "storage",
) -> str:
    """Format PostgreSQL table storage parameters (WITH fillfactor = ...) clause."""
    options: list[str] = []
    config_dict = dict(settings) if settings else {}
    fillfactor = config_dict.get("fillfactor", default_fillfactor)
    if fillfactor is not None:
        if not isinstance(fillfactor, int) or isinstance(fillfactor, bool) or fillfactor not in range(10, 101):
            msg = f"extension_config['{section}']['fillfactor'] must be an integer from 10 to 100"
            raise ValueError(msg)
        options.append(f"fillfactor = {fillfactor}")
    if include_autovacuum:
        scales = (
            ("autovacuum_vacuum_scale_factor", default_vacuum_scale),
            ("autovacuum_analyze_scale_factor", default_analyze_scale),
        )
        for key, default_val in scales:
            value = config_dict.get(key, default_val)
            if value is not None:
                if not isinstance(value, (int, float)) or isinstance(value, bool) or not 0 <= float(value) <= 1:
                    msg = f"extension_config['{section}']['{key}'] must be a number from 0 to 1"
                    raise ValueError(msg)
                options.append(f"{key} = {float(value):g}")
    return f" WITH ({', '.join(options)})" if options else ""


def build_profile() -> "DriverParameterProfile":
    """Create the psqlpy driver parameter profile."""
    coercions: dict[type, Callable[[Any], Any]] = build_uuid_coercions(native=True)
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


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the psqlpy statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    base_config = build_statement_config_from_profile(
        profile, json_serializer=serializer, json_deserializer=deserializer
    )
    parameter_config = _parameter_config(base_config.parameter_config)
    return base_config.replace(parameter_config=parameter_config)


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build normalized connection configuration resolving parameter aliases for psqlpy.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    config = {key: value for key, value in connection_config.items() if value is not None}
    dsn = (
        config.pop("dsn", None)
        or config.pop("conninfo", None)
        or config.pop("url", None)
        or config.pop("connection_string", None)
    )
    if dsn is not None:
        config["dsn"] = dsn
    db_name = (
        config.pop("db_name", None)
        or config.pop("database", None)
        or config.pop("dbname", None)
        or config.pop("db", None)
    )
    if db_name is not None:
        config["db_name"] = db_name
    username = config.pop("username", None) or config.pop("user", None)
    if username is not None:
        config["username"] = username
    if "auth_token_provider" in config and config["auth_token_provider"] is None:
        config.pop("auth_token_provider", None)
    return config


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply psqlpy driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = features.get("json_serializer", to_json)
    deserializer = features.get("json_deserializer", from_json)
    features.setdefault("json_serializer", serializer)
    features.setdefault("json_deserializer", deserializer)
    features.setdefault("enable_cast_detection", True)
    features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
    features.setdefault("enable_paradedb", True)
    features.setdefault("enable_pg_textsearch", True)
    raw_pooler_mode = features.get("pooler_mode")
    if raw_pooler_mode is None:
        features["pooler_mode"] = "session"
    else:
        normalized_mode = str(raw_pooler_mode).lower()
        if normalized_mode not in {"session", "transaction", "statement", "auto"}:
            msg = (
                f"Invalid pooler_mode: {raw_pooler_mode!r}. Expected 'session', 'transaction', 'statement', or 'auto'."
            )
            raise ImproperConfigurationError(msg)
        features["pooler_mode"] = "session" if normalized_mode == "auto" else normalized_mode
    features.setdefault("direct_dsn", None)
    features.setdefault("enable_alloydb_columnar", False)

    base_config = build_statement_config_from_profile(
        driver_profile, json_serializer=serializer, json_deserializer=deserializer
    )
    parameter_config = _parameter_config(base_config.parameter_config)
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

    if isinstance(query_result, list):
        dict_rows = cast("list[dict[str, Any]]", query_result)
    else:
        try:
            dict_rows = cast("list[dict[str, Any]]", query_result.result())
        except Exception as exc:
            if "type: void" in str(exc):
                return [], []
            raise
    if not dict_rows:
        return [], []
    return dict_rows, list(dict_rows[0])


class PsqlpyStreamSource:
    """Compiled async chunk source streaming dict rows from a psqlpy server-side cursor.

    Outside a caller transaction the cursor is declared inside a stream-owned
    transaction, which is committed on close and rolled back on failure. Inside
    one the cursor is declared on the connection and the caller's transaction is
    left untouched.
    """

    __slots__ = ("_chunk_size", "_cursor", "_driver", "_parameters", "_sql", "_transaction")

    def __init__(self, driver: Any, sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._transaction: Any = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        await self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        query_result = await self._driver._run_with_exception_handler(handler, self._cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if query_result is None:
            return []
        return cast("list[dict[str, Any]]", query_result.result())

    async def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()
        transaction = self._transaction
        self._transaction = None
        if transaction is not None:
            try:
                if error:
                    await transaction.rollback()
                else:
                    await transaction.commit()
            except Exception:
                with contextlib.suppress(Exception):
                    await transaction.rollback()

    async def _start(self) -> None:
        if self._driver._connection_in_transaction():
            cursor = self._driver.connection.cursor(self._sql, self._parameters, array_size=self._chunk_size)
            await cursor.start()
            self._cursor = cursor
            return
        transaction = self._driver.connection.transaction()
        await transaction.begin()
        self._transaction = transaction
        try:
            cursor = transaction.cursor(self._sql, self._parameters, array_size=self._chunk_size)
            await cursor.start()
            self._cursor = cursor
        except BaseException:
            self._transaction = None
            with contextlib.suppress(Exception):
                await transaction.rollback()
            raise


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
    return value


def coerce_json_columns(
    records: "list[tuple[Any, ...]]",
    columns: "list[str]",
    json_columns: "set[str]",
    *,
    deserializer: "Callable[[str], Any]" = from_json,
) -> "list[tuple[Any, ...]]":
    """Parse JSON text into objects for destination columns typed json or jsonb.

    psqlpy binds a value by the destination column type, so a JSON column needs
    a dict or list rather than the text an Arrow string column carries. Only the
    columns the database reports as JSON are converted, because converting a
    JSON-looking string bound for a text column would break it.

    Args:
        records: Row tuples in column order.
        columns: Destination column names, positionally aligned with the rows.
        json_columns: Names of destination columns typed json or jsonb.
        deserializer: JSON decoder callable.

    Returns:
        The rows, with JSON column values decoded.
    """
    if not json_columns:
        return records
    indexes = [index for index, name in enumerate(columns) if name in json_columns]
    if not indexes:
        return records
    coerced: list[tuple[Any, ...]] = []
    for record in records:
        row = list(record)
        for index in indexes:
            row[index] = _coerce_json_text_for_execute_many(row[index], deserializer=deserializer)
        coerced.append(tuple(row))
    return coerced


def split_schema_and_table(identifier: str) -> "tuple[str | None, str]":
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    if len(parts) == 1:
        return None, parts[0]
    schema_name = ".".join(parts[:-1])
    table_name = parts[-1]
    if not table_name:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    return schema_name or None, table_name


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
    return 0


_EMPTY_PARAMETER_CASTS: Final[Mapping[int, str]] = MappingProxyType({})


def get_parameter_casts(statement: "SQL | CachedQuery") -> "Mapping[int, str]":
    """Get parameter cast metadata from a SQLSpec statement."""
    if isinstance(statement, CachedQuery):
        return statement.parameter_casts
    state = statement.get_processed_state()
    return _EMPTY_PARAMETER_CASTS if state is Empty or not state.parameter_casts else state.parameter_casts


def prepare_parameters_with_casts(
    parameters: Any, parameter_casts: "Mapping[int, str]", statement_config: "StatementConfig"
) -> Any:
    """Prepare parameters with cast-aware type coercion."""
    if isinstance(parameters, (list, tuple)):
        serializer = statement_config.parameter_config.json_serializer or to_json
        type_map = statement_config.parameter_config.type_coercion_map
        fallbacks = type_coercion_fallbacks(type_map) if type_map else ()
        dispatcher = type_coercion_dispatcher(fallbacks) if fallbacks else None
        coerced: list[Any] | None = None

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
            if prepared_value is not param and coerced is None:
                coerced = list(parameters[: idx - 1])
            if coerced is not None:
                coerced.append(prepared_value)

        if coerced is not None:
            return tuple(coerced) if isinstance(parameters, tuple) else coerced
        return parameters
    return parameters


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map psqlpy exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    psqlpy doesn't expose SQLSTATE codes directly, so we rely on message-based
    pattern matching for exception classification.

    Mapped Exceptions:
        * Integrity constraint violations (UniqueViolationError, ForeignKeyViolationError, etc.)
        * Transaction/serialization errors (DeadlockError, SerializationConflictError)
        * ConnectionTimeoutError / DatabaseConnectionError: connection errors
        * QueryTimeoutError and OperationCancelledError
        * PermissionDeniedError: permission denied, authentication failed
        * DataError: data conversion and formatting errors
        * SQLParsingError: syntax/parse errors

    Args:
        error: The psqlpy exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    _ = logger
    error_msg = str(error).lower()

    if isinstance(error, PsqlpyDataError):
        return _create_postgres_error(error, DataError, "data error")

    if "syntax error" in error_msg or "parse error" in error_msg:
        return _create_postgres_error(error, SQLParsingError, "SQL syntax error")

    if (
        "unique constraint" in error_msg
        or "duplicate key value" in error_msg
        or "violates unique constraint" in error_msg
    ):
        return _create_postgres_error(error, UniqueViolationError, "unique constraint violation")
    if "foreign key constraint" in error_msg or "violates foreign key constraint" in error_msg:
        return _create_postgres_error(error, ForeignKeyViolationError, "foreign key constraint violation")
    if (
        "not-null constraint" in error_msg
        or "violates not-null constraint" in error_msg
        or ("null value in column" in error_msg and "violates not-null" in error_msg)
    ):
        return _create_postgres_error(error, NotNullViolationError, "not-null constraint violation")
    if "check constraint" in error_msg or "violates check constraint" in error_msg:
        return _create_postgres_error(error, CheckViolationError, "check constraint violation")
    if "constraint" in error_msg:
        return _create_postgres_error(error, IntegrityError, "integrity constraint violation")

    if "deadlock" in error_msg:
        return _create_postgres_error(error, DeadlockError, "deadlock detected")
    if "serialization failure" in error_msg or "could not serialize" in error_msg:
        return _create_postgres_error(error, SerializationConflictError, "serialization failure")

    if "connection" in error_msg:
        if "timeout" in error_msg or "timed out" in error_msg:
            return _create_postgres_error(error, ConnectionTimeoutError, "connection timeout")
        return _create_postgres_error(error, DatabaseConnectionError, "connection error")

    if (
        "connection refused" in error_msg
        or "connection reset" in error_msg
        or "could not connect to server" in error_msg
        or "server closed the connection" in error_msg
        or "broken pipe" in error_msg
        or "failed to connect" in error_msg
    ):
        if "timeout" in error_msg or "timed out" in error_msg:
            return _create_postgres_error(error, ConnectionTimeoutError, "connection timeout")
        return _create_postgres_error(error, DatabaseConnectionError, "connection error")

    if error_class := _classify_timeout_or_cancellation(error_msg):
        return _create_postgres_error(error, error_class, "query terminated")

    if "permission denied" in error_msg or "insufficient privilege" in error_msg:
        return _create_postgres_error(error, PermissionDeniedError, "permission denied")
    if (
        "authentication failed" in error_msg
        or "password authentication failed" in error_msg
        or "no password supplied" in error_msg
    ):
        return _create_postgres_error(error, PermissionDeniedError, "authentication error")

    if isinstance(error, PsqlpyOperationalError):
        return _create_postgres_error(error, OperationalError, "operational error")
    if isinstance(error, PsqlpyIntegrityError):
        return _create_postgres_error(error, IntegrityError, "integrity constraint violation")
    if isinstance(error, (PsqlpyConnectionExecuteError,)):
        return _create_postgres_error(error, DatabaseConnectionError, "connection execute error")

    if "connection" in error_msg:
        if "timeout" in error_msg or "timed out" in error_msg:
            return _create_postgres_error(error, ConnectionTimeoutError, "connection timeout")
        return _create_postgres_error(error, DatabaseConnectionError, "connection error")

    return _create_postgres_error(error, SQLSpecError, "database error")


def format_table_identifier(identifier: str) -> str:
    schema_name, table_name = split_schema_and_table(identifier)
    if schema_name:
        return f"{quote_identifier(schema_name)}.{quote_identifier(table_name)}"
    return quote_identifier(table_name)


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


def _coerce_json_parameter(value: Any, cast_type: str, serializer: "Callable[[Any], str]") -> Any:
    """Serialize JSON parameters according to the detected cast type."""

    if value is None:
        return None
    jsonb_type = _JSONB_TYPE
    if cast_type == "JSONB":
        if jsonb_type is not None and isinstance(value, jsonb_type):
            return value
        if jsonb_type is not None:
            if isinstance(value, dict):
                return jsonb_type(value)
            if isinstance(value, (list, tuple)):
                return jsonb_type(list(value))
            if isinstance(value, str):
                stripped = value.strip()
                if stripped.startswith(("{", "[")):
                    with contextlib.suppress(Exception):
                        parsed = from_json(value)
                        if isinstance(parsed, dict):
                            return jsonb_type(parsed)
                        if isinstance(parsed, (list, tuple)):
                            return jsonb_type(list(parsed))
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
            return uuid_from_string(value)
        except ValueError as error:
            msg = "Invalid UUID parameter for psqlpy."
            raise SQLSpecError(msg) from error
    return value


def _coerce_timestamp_parameter(value: Any) -> Any:
    """Convert ISO-formatted timestamp strings to ``dt.datetime``."""

    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, str):
        normalized_value = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return dt.datetime.fromisoformat(normalized_value)
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
    if upper_cast == "VECTOR":
        return coerce_pgvector(value)
    if upper_cast in _TIMESTAMP_CASTS:
        return _coerce_timestamp_parameter(value)
    return value


def _prepare_dict_parameter(value: "dict[str, Any]") -> "dict[str, Any]":
    normalized = _DECIMAL_NORMALIZER(value)
    return normalized if isinstance(normalized, dict) else value


def _prepare_list_parameter(value: "list[Any]") -> "list[Any]":
    normalized = _DECIMAL_NORMALIZER(value)
    return normalized if isinstance(normalized, list) else value


def _prepare_tuple_parameter(value: "tuple[Any, ...]") -> "tuple[Any, ...]":
    normalized = _DECIMAL_NORMALIZER(value)
    if isinstance(normalized, list):
        return tuple(normalized)
    return normalized if isinstance(normalized, tuple) else value


def _parameter_config(base_config: "ParameterStyleConfig") -> "ParameterStyleConfig":
    """Construct parameter configuration for psqlpy.

    Args:
        base_config: Base parameter configuration to extend.

    Returns:
        ParameterStyleConfig with updated type coercions.
    """

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[dict] = _prepare_dict_parameter
    updated_type_map[list] = _prepare_list_parameter
    updated_type_map[tuple] = _prepare_tuple_parameter

    return base_config.replace(type_coercion_map=updated_type_map)


def _parse_psqlpy_command_tag(tag: str) -> int:
    """Parse PostgreSQL command tag to extract rows affected.

    Args:
        tag: PostgreSQL command tag string.

    Returns:
        Number of rows affected, 0 if unable to parse.
    """
    if not tag:
        return 0

    match = PSQLPY_STATUS_REGEX.match(tag.strip())
    if match:
        command = match.group(1).upper()
        if command == "INSERT" and match.group(3):
            return int(match.group(3))
        if command in {"UPDATE", "DELETE"} and match.group(3):
            return int(match.group(3))
    return 0


@lru_cache(maxsize=_DML_COUNT_QUERY_CACHE_SIZE)
def _dml_count_query(sql: str) -> str | None:
    """Build a PostgreSQL query that returns an exact count for one DML statement."""
    try:
        expression = sqlglot.parse_one(sql, dialect="postgres")
    except ParseError:
        return None

    if not isinstance(expression, (exp.Insert, exp.Update, exp.Delete)) or expression.args.get("returning"):
        return None

    existing_with = expression.args.get("with_")
    if existing_with is not None and existing_with.args.get("recursive"):
        return None

    used_aliases = {cte.alias_or_name for cte in expression.find_all(exp.CTE)}
    cte_alias = _DML_COUNT_CTE_ALIAS
    suffix = 1
    while cte_alias in used_aliases:
        cte_alias = f"{_DML_COUNT_CTE_ALIAS}_{suffix}"
        suffix += 1

    if existing_with is not None:
        expression.set("with_", None)

    expression = expression.returning("1", dialect="postgres", copy=False)
    count_expression = exp.alias_(exp.Count(this=exp.Star()), _DML_COUNT_COLUMN)
    count_query = exp.select(count_expression).from_(cte_alias, copy=False)
    if existing_with is not None:
        count_query.set("with_", existing_with)
    count_query.with_(cte_alias, as_=expression, copy=False)
    return count_query.sql(dialect="postgres", copy=False)


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


def _sequence_needs_numeric_coercion(values: "list[Any] | tuple[Any, ...]") -> bool:
    """Return True when any value in a parameter sequence needs numeric coercion."""
    return any(isinstance(value, _NUMERIC_COERCE_TYPES) for value in values)


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


def _coerce_json_text_for_execute_many(value: Any, *, deserializer: "Callable[[str], Any]" = from_json) -> Any:
    if not isinstance(value, str):
        return value
    stripped = value.lstrip()
    if not stripped or stripped[0] not in "{[":
        return value
    try:
        decoded = deserializer(value)
    except Exception:
        return value
    return decoded if isinstance(decoded, (dict, list, tuple)) else value


driver_profile = build_profile()

default_statement_config = build_statement_config()
