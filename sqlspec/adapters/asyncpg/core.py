"""AsyncPG adapter compiled helpers."""

import asyncio
import contextlib
import datetime as dt
import re
import sys
from collections.abc import Sized
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlspec.adapters.asyncpg._typing import asyncpg_module as asyncpg
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.core.config_runtime import resolve_runtime_statement_config
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
    _classify_timeout_or_cancellation,
    map_sqlstate_to_exception,
)
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.dispatch import TypeDispatcher
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_optional
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import SQL, ParameterStyleConfig, StackOperation

__all__ = (
    "AsyncpgStreamSource",
    "NormalizedStackOperation",
    "apply_driver_features",
    "build_connection_config",
    "build_postgres_storage_params_sql",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "configure_parameter_serializers",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "parse_status",
    "register_json_codecs",
    "register_pgvector_support",
    "resolve_many_rowcount",
    "resolve_runtime_statement_config",
)

ASYNC_PG_STATUS_REGEX: "re.Pattern[str]" = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)
EXPECTED_REGEX_GROUPS = 3

logger = get_logger("sqlspec.adapters.asyncpg.core")
_PGVECTOR_MISSING_LOGGED = False
_JSONB_BINARY_VERSION = b"\x01"


class NormalizedStackOperation(NamedTuple):
    """Normalized execution metadata used for prepared stack operations."""

    operation: "StackOperation"
    statement: "SQL"
    sql: str
    parameters: "tuple[Any, ...] | dict[str, Any] | None"


_EXCEPTION_MAPPING_DISPATCHER = TypeDispatcher["tuple[str, type[SQLSpecError], str]"]()


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build normalized connection configuration resolving parameter aliases for asyncpg.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    config = dict(connection_config)
    found_dsn = False
    dsn_val = None
    for key in ("dsn", "conninfo", "url", "connection_string"):
        if key in config:
            val = config.pop(key)
            if not found_dsn:
                dsn_val = val
                found_dsn = True
    if found_dsn:
        config["dsn"] = dsn_val

    found_database = False
    database_val = None
    for key in ("database", "dbname", "db"):
        if key in config:
            val = config.pop(key)
            if not found_database:
                database_val = val
                found_database = True
    if found_database:
        config["database"] = database_val

    found_user = False
    user_val = None
    for key in ("user", "username"):
        if key in config:
            val = config.pop(key)
            if not found_user:
                user_val = val
                found_user = True
    if found_user:
        config["user"] = user_val

    if config.pop("pgbouncer", False):
        config["statement_cache_size"] = 0

    return config


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
    """Create the AsyncPG driver parameter profile."""

    return DriverParameterProfile(
        name="AsyncPG",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.NUMERIC,
        supported_execution_styles={ParameterStyle.NUMERIC},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="driver",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="postgres",
    )


def configure_parameter_serializers(
    parameter_config: "ParameterStyleConfig",
    serializer: "Callable[[Any], str]",
    *,
    deserializer: "Callable[[str], Any] | None" = None,
) -> "ParameterStyleConfig":
    """Return a parameter configuration updated with AsyncPG JSON codecs."""

    effective_deserializer = deserializer or parameter_config.json_deserializer or from_json
    return parameter_config.replace(json_serializer=serializer, json_deserializer=effective_deserializer)


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AsyncPG statement configuration with optional JSON codecs."""

    effective_serializer = json_serializer or to_json
    effective_deserializer = json_deserializer or from_json

    profile = driver_profile
    base_config = build_statement_config_from_profile(
        profile,
        statement_overrides={"dialect": "postgres"},
        json_serializer=effective_serializer,
        json_deserializer=effective_deserializer,
    )

    parameter_config = configure_parameter_serializers(
        base_config.parameter_config, effective_serializer, deserializer=effective_deserializer
    )

    return base_config.replace(parameter_config=parameter_config)


class _JsonEncoder:
    __slots__ = ("_encoder",)

    def __init__(self, encoder: "Callable[[Any], str]") -> None:
        self._encoder = encoder

    def __call__(self, value: Any) -> bytes:
        return _encode_json_payload(value, self._encoder)


class _JsonDecoder:
    __slots__ = ("_decoder",)

    def __init__(self, decoder: "Callable[[str], Any]") -> None:
        self._decoder = decoder

    def __call__(self, value: Any) -> Any:
        return _decode_json_payload(value, self._decoder)


class _JsonbEncoder:
    __slots__ = ("_encoder",)

    def __init__(self, encoder: "Callable[[Any], str]") -> None:
        self._encoder = encoder

    def __call__(self, value: Any) -> bytes:
        return _encode_jsonb_payload(value, self._encoder)


class _JsonbDecoder:
    __slots__ = ("_decoder",)

    def __init__(self, decoder: "Callable[[str], Any]") -> None:
        self._decoder = decoder

    def __call__(self, value: Any) -> Any:
        return _decode_jsonb_payload(value, self._decoder)


async def register_json_codecs(connection: Any, encoder: Any, decoder: Any) -> None:
    """Register JSON type codecs on asyncpg connection."""
    try:
        await connection.set_type_codec(
            "json", encoder=_JsonEncoder(encoder), decoder=_JsonDecoder(decoder), schema="pg_catalog", format="binary"
        )
        await connection.set_type_codec(
            "jsonb",
            encoder=_JsonbEncoder(encoder),
            decoder=_JsonbDecoder(decoder),
            schema="pg_catalog",
            format="binary",
        )
    except Exception:
        logger.exception("Failed to register JSON type codecs")


async def register_pgvector_support(connection: Any) -> None:
    """Register pgvector extension support on asyncpg connection."""
    if not PGVECTOR_INSTALLED:
        global _PGVECTOR_MISSING_LOGGED
        if not _PGVECTOR_MISSING_LOGGED:
            logger.debug("pgvector not installed - skipping vector type support")
            _PGVECTOR_MISSING_LOGGED = True
        return

    pgvector_asyncpg = import_optional("pgvector.asyncpg")
    if pgvector_asyncpg is None:
        return
    try:
        await pgvector_asyncpg.register_vector(connection)
    except Exception:
        logger.exception("Failed to register pgvector support")


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply AsyncPG driver feature defaults to statement config."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}

    serializer = processed_features.setdefault("json_serializer", to_json)
    deserializer = processed_features.setdefault("json_deserializer", from_json)
    processed_features.setdefault("enable_json_codecs", True)
    processed_features.setdefault("enable_pgvector", PGVECTOR_INSTALLED)
    processed_features.setdefault("enable_paradedb", True)
    processed_features.setdefault("enable_pg_textsearch", True)
    processed_features.setdefault("enable_cloud_sql", False)
    processed_features.setdefault("enable_alloydb", False)
    processed_features.setdefault("enable_events", True)
    processed_features.setdefault("events_backend", "notify")
    pgbouncer_enabled = bool(processed_features.get("pgbouncer") or processed_features.get("pgbouncer_mode"))
    processed_features["pgbouncer"] = pgbouncer_enabled
    processed_features["pgbouncer_mode"] = pgbouncer_enabled
    raw_pooler_mode = processed_features.get("pooler_mode")
    if raw_pooler_mode is None:
        processed_features["pooler_mode"] = "transaction" if pgbouncer_enabled else "session"
    else:
        normalized_mode = str(raw_pooler_mode).lower()
        if normalized_mode not in {"session", "transaction", "statement", "auto"}:
            msg = (
                f"Invalid pooler_mode: {raw_pooler_mode!r}. Expected 'session', 'transaction', 'statement', or 'auto'."
            )
            raise ImproperConfigurationError(msg)
        processed_features["pooler_mode"] = (
            "transaction" if normalized_mode == "auto" and pgbouncer_enabled else normalized_mode
        )
    if processed_features["pooler_mode"] in {"transaction", "statement"}:
        processed_features["pgbouncer"] = True
        processed_features["pgbouncer_mode"] = True
    processed_features.setdefault("direct_dsn", None)
    processed_features.setdefault("enable_alloydb_columnar", False)

    parameter_config = configure_parameter_serializers(
        statement_config.parameter_config, serializer, deserializer=deserializer
    )
    statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_features


def parse_status(status: Any) -> int:
    """Parse AsyncPG status string to extract row count.

    AsyncPG returns status strings like "INSERT 0 1", "UPDATE 3", "DELETE 2"
    for non-SELECT operations. This method extracts the affected row count.

    Args:
        status: Status string from AsyncPG operation.

    Returns:
        Number of affected rows, or 0 if cannot parse.
    """
    if not status or not isinstance(status, str):
        return 0

    match = ASYNC_PG_STATUS_REGEX.match(status.strip())
    if match:
        groups = match.groups()
        if len(groups) >= EXPECTED_REGEX_GROUPS:
            try:
                return int(groups[-1])
            except (ValueError, IndexError):
                pass

    return 0


def resolve_many_rowcount(parameter_sets: Any, *, fallback_count: "int | None" = None) -> int:
    """Resolve execute_many rowcount using the parameter payload size."""
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameter_sets, Sized):
        return len(parameter_sets)
    return 0


_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.UniqueViolationError, ("23505", UniqueViolationError, "unique constraint violation")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.ForeignKeyViolationError, ("23503", ForeignKeyViolationError, "foreign key constraint violation")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.NotNullViolationError, ("23502", NotNullViolationError, "not-null constraint violation")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.CheckViolationError, ("23514", CheckViolationError, "check constraint violation")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.IntegrityConstraintViolationError, ("23000", IntegrityError, "integrity constraint violation")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.DeadlockDetectedError, ("40P01", DeadlockError, "deadlock detected")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.SerializationError, ("40001", SerializationConflictError, "serialization failure")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.QueryCanceledError, ("57014", OperationCancelledError, "query canceled")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.InsufficientPrivilegeError, ("42501", PermissionDeniedError, "insufficient privilege")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.InvalidPasswordError, ("28P01", PermissionDeniedError, "invalid password")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.InvalidAuthorizationSpecificationError, ("28000", PermissionDeniedError, "authorization error")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.ConnectionDoesNotExistError, ("08003", DatabaseConnectionError, "connection does not exist")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.CannotConnectNowError, ("57P03", DatabaseConnectionError, "cannot connect now")
)
_EXCEPTION_MAPPING_DISPATCHER.register(
    asyncpg.exceptions.PostgresSyntaxError, ("42601", SQLParsingError, "SQL syntax error")
)


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map asyncpg exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __aexit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. Native asyncpg exception types (isinstance checks - most reliable)
        2. SQLSTATE code via centralized utility
        3. Generic SQLSpecError fallback

    Args:
        error: The asyncpg exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    _ = logger
    if isinstance(error, (asyncio.TimeoutError, TimeoutError)):
        return QueryTimeoutError(str(error))
    if isinstance(error, OSError) or (
        asyncpg is not None and isinstance(error, getattr(asyncpg, "InterfaceError", ()))
    ):
        return DatabaseConnectionError(str(error))

    mapped_error = _EXCEPTION_MAPPING_DISPATCHER.get(error)
    if mapped_error is not None:
        error_code, error_class, description = mapped_error
        if error_code == "57014":
            termination_class = _classify_timeout_or_cancellation(str(error)) or OperationalError
            return _create_postgres_error(error, error_code, termination_class, description)
        return _create_postgres_error(error, error_code, error_class, description)

    sqlstate_attr = error.sqlstate if has_sqlstate(error) else None
    sqlstate_code: str | None = sqlstate_attr if sqlstate_attr is not None else None
    if sqlstate_code:
        if sqlstate_code == "57014":
            termination_class = _classify_timeout_or_cancellation(str(error)) or OperationalError
            return _create_postgres_error(error, sqlstate_code, termination_class, "query terminated")
        exc_class = map_sqlstate_to_exception(sqlstate_code)
        if exc_class:
            return _create_postgres_error(error, sqlstate_code, exc_class, "database error")

    return _create_postgres_error(error, sqlstate_code, SQLSpecError, "database error")


def collect_rows(records: "list[Any] | None") -> "tuple[list[Any], list[str]]":
    """Collect AsyncPG records and column names.

    Returns raw asyncpg.Record objects without copying to dicts.
    Lazy dict materialization is handled by SQLResult when needed.

    Args:
        records: Records returned from asyncpg fetch.

    Returns:
        Tuple of (rows, column_names).
    """
    if not records:
        return [], []
    column_names = list(records[0].keys())
    return records, column_names


class AsyncpgStreamSource:
    """Compiled async chunk source streaming dict rows from an asyncpg cursor in a stream-owned transaction."""

    __slots__ = ("_chunk_size", "_cursor", "_driver", "_parameters", "_sql", "_timeout", "_transaction")

    def __init__(
        self,
        driver: Any,
        sql: str,
        parameters: "tuple[Any, ...]",
        chunk_size: int,
        timeout: "float | dict[str, Any] | None" = None,
    ) -> None:
        if isinstance(timeout, dict):
            self._timeout: float | None = (
                float(timeout["timeout"]) if "timeout" in timeout and timeout["timeout"] is not None else None
            )
        else:
            self._timeout = float(timeout) if timeout is not None else None
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._transaction: Any = None

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        if sys.exc_info()[1] is None:
            async with handler:
                await self._start()
        else:
            await self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        if sys.exc_info()[1] is None:
            async with handler:
                records = (
                    await self._cursor.fetch(self._chunk_size, timeout=self._timeout)
                    if self._timeout is not None
                    else await self._cursor.fetch(self._chunk_size)
                )
        elif self._timeout is not None:
            records = await self._driver._run_with_exception_handler(
                handler, self._cursor.fetch, self._chunk_size, timeout=self._timeout
            )
        else:
            records = await self._driver._run_with_exception_handler(handler, self._cursor.fetch, self._chunk_size)
        self._driver._check_pending_exception(handler)
        assert records is not None
        return [dict(record) for record in records]

    async def close(self, error: bool = False) -> None:
        self._cursor = None
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
        already_in_tx = self._driver._connection_in_transaction()
        if not already_in_tx:
            transaction = self._driver.connection.transaction()
            await transaction.start()
            self._transaction = transaction
        try:
            if self._timeout is not None:
                self._cursor = await self._driver.connection.cursor(self._sql, *self._parameters, timeout=self._timeout)
            else:
                self._cursor = await self._driver.connection.cursor(self._sql, *self._parameters)
        except BaseException:
            transaction = self._transaction
            self._transaction = None
            if transaction is not None:
                with contextlib.suppress(Exception):
                    await transaction.rollback()
            raise


def _convert_datetime_param(value: Any) -> Any:
    """Convert datetime parameter, handling ISO strings."""

    if isinstance(value, str):
        return dt.datetime.fromisoformat(value)
    return value


def _convert_date_param(value: Any) -> Any:
    """Convert date parameter, handling ISO strings."""

    if isinstance(value, str):
        return dt.date.fromisoformat(value)
    return value


def _convert_time_param(value: Any) -> Any:
    """Convert time parameter, handling ISO strings."""

    if isinstance(value, str):
        return dt.time.fromisoformat(value)
    return value


def _custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for AsyncPG."""

    return {
        dt.datetime: _convert_datetime_param,
        dt.date: _convert_date_param,
        dt.time: _convert_time_param,
        **build_uuid_coercions(native=True),
    }


def _is_pre_serialized_json(value: str) -> bool:
    stripped = value.strip()
    if not (
        (stripped.startswith("{") and stripped.endswith("}")) or (stripped.startswith("[") and stripped.endswith("]"))
    ):
        return False
    try:
        from_json(stripped)
    except Exception:
        return False
    return True


def _encode_json_payload(value: Any, encoder: "Callable[[Any], str]") -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw_bytes = value.tobytes() if isinstance(value, memoryview) else bytes(value)
        if raw_bytes.startswith(_JSONB_BINARY_VERSION):
            return raw_bytes[1:]
        return raw_bytes
    if isinstance(value, str) and _is_pre_serialized_json(value):
        return value.encode("utf-8")

    encoded: Any = encoder(value)
    if isinstance(encoded, bytes):
        return encoded[1:] if encoded.startswith(_JSONB_BINARY_VERSION) else encoded
    if isinstance(encoded, bytearray):
        raw_encoded = bytes(encoded)
        return raw_encoded[1:] if raw_encoded.startswith(_JSONB_BINARY_VERSION) else raw_encoded
    if isinstance(encoded, memoryview):
        raw_encoded = encoded.tobytes()
        return raw_encoded[1:] if raw_encoded.startswith(_JSONB_BINARY_VERSION) else raw_encoded
    return str(encoded).encode("utf-8")


def _decode_json_payload(value: Any, decoder: "Callable[[str], Any]") -> Any:
    if isinstance(value, str):
        return decoder(value)
    if isinstance(value, memoryview):
        value = value.tobytes()
    return decoder(bytes(value).decode("utf-8"))


def _encode_jsonb_payload(value: Any, encoder: "Callable[[Any], str]") -> bytes:
    payload = _encode_json_payload(value, encoder)
    if payload.startswith(_JSONB_BINARY_VERSION):
        return payload
    return _JSONB_BINARY_VERSION + payload


def _decode_jsonb_payload(value: Any, decoder: "Callable[[str], Any]") -> Any:
    if isinstance(value, str):
        return decoder(value)
    if isinstance(value, memoryview):
        value = value.tobytes()
    payload = bytes(value)
    if payload.startswith(_JSONB_BINARY_VERSION):
        payload = payload[1:]
    return decoder(payload.decode("utf-8"))


def _create_postgres_error(
    error: Any, code: "str | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from an asyncpg error.

    Args:
        error: The original asyncpg exception
        code: PostgreSQL SQLSTATE error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    msg = f"PostgreSQL {description} [{code}]: {error}" if code else f"PostgreSQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


driver_profile = build_profile()

default_statement_config = build_statement_config()
