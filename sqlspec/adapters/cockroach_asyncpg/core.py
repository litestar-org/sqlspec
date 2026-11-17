"""CockroachDB AsyncPG adapter helpers."""

import asyncio
import datetime as dt
import re
import secrets
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import urlsplit, urlunsplit

from mypy_extensions import mypyc_attr
from sqlglot import tokenize
from sqlglot.tokenizer_core import TokenType

from sqlspec.adapters.cockroach_asyncpg._typing import CockroachAsyncpgInterfaceError, CockroachAsyncpgPostgresError
from sqlspec.adapters.cockroach_asyncpg._typing import cockroach_asyncpg_module as asyncpg
from sqlspec.core.parameters._registry import build_statement_config_from_profile
from sqlspec.core.parameters._types import DriverParameterProfile, ParameterStyle
from sqlspec.core.result._base import DMLResult, SQLResult
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
    ReadOnlyTransactionError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
    _classify_timeout_or_cancellation,
    map_sqlstate_to_exception,
)
from sqlspec.typing import PGVECTOR_INSTALLED
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.dispatch import TypeDispatcher
from sqlspec.utils.logging import get_logger
from sqlspec.utils.module_loader import import_optional
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.serializers._json import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core.cache import CachedQuery
    from sqlspec.core.parameters._types import ParameterStyleConfig
    from sqlspec.core.statement import StatementConfig
    from sqlspec.storage.pipeline import StorageTelemetry

__all__ = (
    "DEFAULT_FOLLOWER_READ_STALENESS",
    "CockroachAsyncpgRetryConfig",
    "apply_driver_features",
    "build_connection_config",
    "build_native_export",
    "build_native_import",
    "build_profile",
    "build_statement_config",
    "calculate_backoff_seconds",
    "configure_parameter_serializers",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "execute_cache_hit",
    "is_retryable_error",
    "native_export_telemetry",
    "native_import_telemetry",
    "normalize_native_export_query",
    "register_json_codecs",
    "register_pgvector_support",
    "resolve_follower_read_options",
    "sanitize_native_storage_uri",
    "validate_follower_read_staleness",
)

logger = get_logger("sqlspec.adapters.cockroach_asyncpg.core")
_DEFAULT_MAX_RETRIES: Final[int] = 10
_DEFAULT_BASE_DELAY_MS: Final[float] = 50.0
_DEFAULT_MAX_DELAY_MS: Final[float] = 5000.0
_DEFAULT_ENABLE_LOGGING: Final[bool] = True
_MAX_CAUSE_CHAIN_DEPTH: Final[int] = 16
_SYSTEM_RANDOM: Final[secrets.SystemRandom] = secrets.SystemRandom()
_CLUSTER_NAME_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9._-]+")
_UNSET: Final[object] = object()
_PGVECTOR_MISSING_LOGGED = False
_JSONB_BINARY_VERSION: Final[bytes] = b"\x01"
_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"\A([A-Z]+)(?:\s+(\d+))?\s+(\d+)\Z", re.IGNORECASE)
_EXCEPTION_MAPPING_DISPATCHER = TypeDispatcher["tuple[str, type[SQLSpecError], str]"]()
_STALENESS_LITERAL: Final[re.Pattern[str]] = re.compile(r"'[^'\\;]+'")
_STALENESS_FUNCTION: Final[re.Pattern[str]] = re.compile(
    r"(?:follower_read_timestamp|with_max_staleness|with_min_timestamp)"
    r"\(\s*(?:'[^'\\;]*'\s*(?:,\s*(?:'[^'\\;]*'|true|false)\s*)*)?\)",
    re.IGNORECASE,
)
DEFAULT_FOLLOWER_READ_STALENESS: Final[str] = "follower_read_timestamp()"


@mypyc_attr(allow_interpreted_subclasses=False)
class CockroachAsyncpgRetryConfig:
    """CockroachDB asyncpg transaction retry configuration."""

    __slots__ = ("base_delay_ms", "enable_logging", "max_delay_ms", "max_retries")

    def __init__(
        self,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        base_delay_ms: float = _DEFAULT_BASE_DELAY_MS,
        max_delay_ms: float = _DEFAULT_MAX_DELAY_MS,
        enable_logging: bool = _DEFAULT_ENABLE_LOGGING,
    ) -> None:
        if max_retries < 0:
            msg = "max_retries must be non-negative"
            raise ImproperConfigurationError(msg)
        if base_delay_ms < 0:
            msg = "base_delay_ms must be non-negative"
            raise ImproperConfigurationError(msg)
        if max_delay_ms < 0:
            msg = "max_delay_ms must be non-negative"
            raise ImproperConfigurationError(msg)
        self.max_retries = max_retries
        self.base_delay_ms = base_delay_ms
        self.max_delay_ms = max_delay_ms
        self.enable_logging = enable_logging

    @classmethod
    def from_features(cls, driver_features: "Mapping[str, Any]") -> "CockroachAsyncpgRetryConfig":
        """Build retry config from driver feature mappings."""
        return cls(
            max_retries=int(driver_features.get("max_retries", _DEFAULT_MAX_RETRIES)),
            base_delay_ms=float(driver_features.get("retry_delay_base_ms", _DEFAULT_BASE_DELAY_MS)),
            max_delay_ms=float(driver_features.get("retry_delay_max_ms", _DEFAULT_MAX_DELAY_MS)),
            enable_logging=bool(driver_features.get("enable_retry_logging", _DEFAULT_ENABLE_LOGGING)),
        )


def build_profile() -> "DriverParameterProfile":
    """Create the CockroachDB AsyncPG driver parameter profile."""
    return DriverParameterProfile(
        name="CockroachAsyncpg",
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


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the CockroachDB AsyncPG statement configuration with optional JSON codecs."""
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


async def register_json_codecs(connection: Any, encoder: Any, decoder: Any) -> None:
    """Register JSON type codecs on a CockroachDB asyncpg connection."""
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
    """Register pgvector extension support on a CockroachDB asyncpg connection."""
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
    """Apply CockroachDB AsyncPG driver feature defaults to statement config."""
    processed_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = processed_features.setdefault("json_serializer", to_json)
    deserializer = processed_features.setdefault("json_deserializer", from_json)
    processed_features.setdefault("enable_json_codecs", True)
    processed_features["enable_pgvector"] = bool(driver_features and driver_features.get("enable_pgvector") is True)
    processed_features.setdefault("enable_events", True)
    processed_features.setdefault("events_backend", "poll_queue")
    parameter_config = configure_parameter_serializers(
        statement_config.parameter_config, serializer, deserializer=deserializer
    )
    statement_config = statement_config.replace(parameter_config=parameter_config)
    return statement_config, processed_features


def configure_parameter_serializers(
    parameter_config: "ParameterStyleConfig",
    serializer: "Callable[[Any], str]",
    *,
    deserializer: "Callable[[str], Any] | None" = None,
) -> "ParameterStyleConfig":
    """Return a parameter configuration updated with AsyncPG JSON codecs."""
    effective_deserializer = deserializer or parameter_config.json_deserializer or from_json
    return parameter_config.replace(json_serializer=serializer, json_deserializer=effective_deserializer)


def build_connection_config(config: "Mapping[str, Any] | None") -> "dict[str, Any]":
    """Prepare CockroachDB AsyncPG connection config, extracting multi-region server settings."""
    normalized: dict[str, Any] = dict(normalize_connection_config(config))
    extra = normalized.pop("extra", None)
    if isinstance(extra, Mapping):
        for key, value in extra.items():
            if value is not None:
                normalized.setdefault(key, value)
    result: dict[str, Any] = dict(normalized)
    _resolve_connection_alias(result, "dsn", ("dsn", "conninfo", "url", "connection_string"))
    _resolve_connection_alias(result, "database", ("database", "dbname", "db"))
    _resolve_connection_alias(result, "user", ("user", "username"))
    server_settings = dict(result.get("server_settings") or {})
    if "application_name" in result:
        server_settings.setdefault("application_name", str(result.pop("application_name")))
    if "cluster" in result:
        cluster = result.pop("cluster")
        if cluster:
            name = str(cluster)
            if _CLUSTER_NAME_PATTERN.fullmatch(name) is None:
                msg = (
                    f"CockroachDB cluster name {name!r} may contain only letters, "
                    "digits, hyphens, underscores, and periods."
                )
                raise ImproperConfigurationError(msg)
            routing = f"--cluster={name}"
            existing = server_settings.get("options")
            server_settings["options"] = f"{existing} {routing}" if existing else routing
    if "default_transaction_use_follower_reads" in result:
        val = result.pop("default_transaction_use_follower_reads")
        if not isinstance(val, bool):
            msg = "default_transaction_use_follower_reads must be a boolean"
            raise ImproperConfigurationError(msg)
        server_settings.setdefault("default_transaction_use_follower_reads", "on" if val else "off")
    for key in ("results_buffer_size", "statement_timeout", "idle_in_transaction_session_timeout"):
        if key not in result:
            continue
        value = result.pop(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            msg = f"{key} must be a non-negative integer"
            raise ImproperConfigurationError(msg)
        server_settings[key] = str(value)
    if server_settings:
        result["server_settings"] = server_settings
    return result


def is_retryable_error(error: BaseException) -> bool:
    """Return True when the error should trigger a CockroachDB retry.

    Translated errors arrive as ``SerializationConflictError`` with no SQLSTATE
    attribute, so the class check comes first, and the SQLSTATE branch covers
    raw driver errors raised outside the translation seam.

    The cause chain is also walked because CockroachDB reports a serialization
    failure at COMMIT for the write-skew case, and transaction control wraps the
    driver error with ``raise ... from e``, which keeps the original reachable.
    The walk descends only through SQLSpec's own errors, so an exception the
    caller raised from a conflict is treated as the deliberate abort it is
    rather than being retried against a database that will never accept it.

    Args:
        error: The exception raised by the transaction body or its commit.

    Returns:
        True when the transaction should be retried.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    depth = 0
    while current is not None and id(current) not in seen and depth < _MAX_CAUSE_CHAIN_DEPTH:
        seen.add(id(current))
        depth += 1
        if isinstance(current, SerializationConflictError):
            return True
        if has_sqlstate(current):
            sqlstate = getattr(current, "sqlstate", None)
            if str(sqlstate) == "40001":
                return True
        if not isinstance(current, SQLSpecError):
            return False
        next_exc = cast("BaseException | None", cast("Any", current).__cause__)
        if next_exc is None and not getattr(current, "__suppress_context__", False):
            next_exc = cast("BaseException | None", cast("Any", current).__context__)
        current = next_exc
    return False


def calculate_backoff_seconds(attempt: int, config: "CockroachAsyncpgRetryConfig") -> float:
    """Calculate exponential backoff delay in seconds.

    The exponential term is capped before jitter is applied so that delays stay
    spread out once the cap is reached, rather than collapsing onto a single
    value at the exact moment contention is highest.
    """
    capped_ms: float = min(config.base_delay_ms * (2**attempt), config.max_delay_ms)
    if capped_ms <= 0.0:
        return 0.0
    return _SYSTEM_RANDOM.uniform(capped_ms / 2.0, capped_ms) / 1000.0


def validate_follower_read_staleness(staleness: str) -> str:
    """Validate a follower-read staleness clause.

    ``AS OF SYSTEM TIME`` accepts no placeholders, so the value is interpolated
    into the statement and must be restricted to the literal and function forms
    CockroachDB documents.

    Args:
        staleness: Interval or timestamp literal, or a staleness function call.

    Returns:
        The trimmed staleness clause.

    Raises:
        ImproperConfigurationError: If the value matches no accepted form.
    """
    candidate = staleness.strip()
    if _STALENESS_LITERAL.fullmatch(candidate) or _STALENESS_FUNCTION.fullmatch(candidate):
        return candidate
    msg = (
        "default_staleness must be a quoted interval or timestamp literal such as \"'-10s'\", "
        "or one of follower_read_timestamp(), with_max_staleness(...), with_min_timestamp(...)."
    )
    raise ImproperConfigurationError(msg)


def build_native_export(
    query: str, parameters: "list[Any]", uri: str, file_format: str, options: "dict[str, Any]"
) -> "tuple[str, list[Any]]":
    """Wrap compiled query SQL without embedding destination or CSV values."""
    format_sql = "PARQUET" if file_format == "parquet" else "CSV"
    values = [*parameters, uri]
    command = "EXPORT INTO " + format_sql + " " + ("$" + str(len(values)))
    if file_format == "csv" and "nullas" in options:
        values.append(options["nullas"])
        command += " WITH nullas = " + ("$" + str(len(values)))
    return command + " FROM (" + query.rstrip().removesuffix(";") + "\n)", values


def build_native_import(table: str, uri: str, file_format: str, options: "dict[str, Any]") -> "tuple[str, list[Any]]":
    """Quote the target identifier and bind explicit CSV conventions."""
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise ValueError(msg)
    target = ".".join(quote_identifier(part) for part in parts)
    format_sql = "PARQUET" if file_format == "parquet" else "CSV"
    values: list[Any] = [uri]
    command = "IMPORT INTO " + target + " " + format_sql + " DATA (" + "$1" + ")"
    clauses = []
    if file_format == "csv":
        for key in ("skip", "nullif"):
            if key in options:
                values.append(str(options[key]))
                clauses.append(key + " = " + ("$" + str(len(values))))
    if clauses:
        command += " WITH " + ", ".join(clauses)
    return command, values


def sanitize_native_storage_uri(uri: str) -> str:
    """Strip userinfo, query parameters, and fragments from a native storage URI for telemetry."""
    parsed = urlsplit(uri)
    if not parsed.scheme and not parsed.netloc:
        return uri
    netloc = parsed.netloc.rsplit("@", 1)[-1] if "@" in parsed.netloc else parsed.netloc
    return urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


def native_export_telemetry(
    rows: "list[dict[str, Any]]", destination: str, backend: str, file_format: str
) -> "StorageTelemetry":
    """Retain measured export metadata and generated relative filenames."""
    return {
        "destination": sanitize_native_storage_uri(destination),
        "backend": backend,
        "format": file_format,
        "rows_processed": sum(int(row["rows"]) for row in rows),
        "bytes_processed": sum(int(row["bytes"]) for row in rows),
        "extra": {"files": [str(row["filename"]) for row in rows]},
    }


def native_import_telemetry(
    rows: "list[dict[str, Any]]", table: str, backend: str, file_format: str
) -> "StorageTelemetry":
    """Expose import job metadata without treating logical bytes as file size."""
    row = rows[0]
    if row["status"] != "succeeded":
        msg = "Native storage import did not succeed"
        raise ValueError(msg)
    return {
        "destination": table,
        "backend": backend,
        "format": file_format,
        "rows_processed": int(row["rows"]),
        "extra": {"job_id": row["job_id"], "status": row["status"]},
    }


def normalize_native_export_query(query: str) -> str | None:
    """Remove a terminal delimiter, preserving comments; refuse raw scripts."""
    if ";" not in query:
        return query
    tokens = tokenize(query, read="postgres")
    delimiters = [token for token in tokens if token.token_type == TokenType.SEMICOLON]
    if not delimiters:
        return query
    if len(delimiters) != 1 or delimiters[0] is not tokens[-1]:
        return None
    delimiter = delimiters[0]
    return query[: delimiter.start] + query[delimiter.end + 1 :]


def resolve_follower_read_options(
    driver_features: "Mapping[str, Any]",
    *,
    follower_reads: bool | None = None,
    staleness: str | None = None,
    read_only_session: bool = False,
    bind_key: str | None = None,
) -> tuple[bool, str | None, bool]:
    """Resolve follower bounds with primary consistency taking precedence."""
    entry = RoutingContext.get_entry(bind_key)
    if entry is not None and (entry.mode == "primary" or entry.pinned_by_write):
        return False, None, read_only_session
    enabled = bool(driver_features.get("enable_follower_reads", False)) if follower_reads is None else follower_reads
    if staleness is not None:
        return enabled, validate_follower_read_staleness(staleness), read_only_session
    if entry is not None and entry.max_staleness_seconds is not None:
        seconds = entry.max_staleness_seconds
        if seconds <= 0:
            return False, None, read_only_session
        return True, validate_follower_read_staleness(f"'-{seconds:g}s'"), read_only_session
    value = driver_features.get("default_staleness") or (DEFAULT_FOLLOWER_READ_STALENESS if enabled else None)
    return enabled, validate_follower_read_staleness(value) if value is not None else None, read_only_session


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map CockroachDB AsyncPG exceptions to SQLSpec exceptions."""
    _ = logger
    if (has_sqlstate(error) and getattr(error, "sqlstate", None) == "25006") or type(error).__name__ in {
        "ReadOnlySqlTransaction",
        "ReadOnlySQLTransactionError",
    }:
        mapped = ReadOnlyTransactionError(str(error))
        mapped.__cause__ = error if isinstance(error, BaseException) else None
        return mapped

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

    sqlstate_attr = getattr(error, "sqlstate", None) if has_sqlstate(error) else None
    sqlstate_code: str | None = str(sqlstate_attr) if sqlstate_attr is not None else None
    if sqlstate_code:
        if sqlstate_code == "57014":
            termination_class = _classify_timeout_or_cancellation(str(error)) or OperationalError
            return _create_postgres_error(error, sqlstate_code, termination_class, "query terminated")
        exc_class = map_sqlstate_to_exception(sqlstate_code)
        if exc_class:
            return _create_postgres_error(error, sqlstate_code, exc_class, "database error")

    return _create_postgres_error(error, sqlstate_code, SQLSpecError, "database error")


def _cache_hit_timeout(statement_config: "StatementConfig") -> "float | None":
    """Return the per-statement timeout configured in the statement execution arguments."""
    options = statement_config.execution_args
    if not options:
        return None
    if "timeout" in options:
        value = options["timeout"]
        return float(value) if value is not None else None
    if "command_timeout" in options:
        value = options["command_timeout"]
        return float(value) if value is not None else None
    return None


def _parse_status(status: Any) -> int:
    """Return the affected-row count carried by a command status string, or 0 when it has none."""
    if not status or not isinstance(status, str):
        return 0
    match = _STATUS_REGEX.fullmatch(status.strip())
    if match is None:
        return 0
    try:
        return int(match.group(3))
    except ValueError:
        return 0


async def execute_cache_hit(
    connection: Any,
    sql: str,
    params: Any,
    cached: "CachedQuery",
    statement_config: "StatementConfig",
    driver_features: "dict[str, Any]",
) -> "SQLResult | None":
    """Execute a cached statement on a CockroachDB asyncpg connection and build its result.

    Statements on a read-only session, and read-only statements on a session
    with follower reads enabled, are declined.

    Args:
        connection: asyncpg connection to execute on.
        sql: Raw SQL text the caller passed.
        params: Parameters the caller passed, already valid for the compiled SQL.
        cached: Cached compilation of ``sql``.
        statement_config: Statement configuration supplying the timeout and rebuilding the result statement.
        driver_features: Driver feature flags of the owning session.

    Returns:
        The statement result, or None when the statement is declined.

    Raises:
        SQLSpecError: Mapped from any asyncpg, timeout, transport, or SQLSTATE-bearing error the statement raises.
    """
    if driver_features.get("read_only_session") or (
        cached.operation_profile.is_read_only and driver_features.get("enable_follower_reads", False)
    ):
        return None
    timeout = _cache_hit_timeout(statement_config)
    compiled_sql = cached.compiled_sql
    args: Any = (
        params if isinstance(params, (tuple, list)) else tuple(params.values()) if isinstance(params, dict) else ()
    )
    try:
        if cached.operation_profile.returns_rows:
            records = (
                await connection.fetch(compiled_sql, *args, timeout=timeout)
                if timeout is not None
                else await connection.fetch(compiled_sql, *args)
            )
            data: list[Any] = records or []
            column_names: list[str] = list(data[0].keys()) if data else []
            return SQLResult.create_cached_select(
                sql,
                statement_config,
                cached.processed_state,
                compiled_sql,
                params,
                data,
                column_names,
                len(data),
                "record",
                None,
            )
        status = (
            await connection.execute(compiled_sql, *args, timeout=timeout)
            if timeout is not None
            else await connection.execute(compiled_sql, *args)
        )
        return DMLResult(cached.operation_type, _parse_status(status))
    except Exception as exc:
        if isinstance(exc, SQLSpecError):
            raise
        if isinstance(
            exc, (CockroachAsyncpgPostgresError, CockroachAsyncpgInterfaceError, TimeoutError, OSError)
        ) or hasattr(exc, "sqlstate"):
            mapped_error = create_mapped_exception(exc)
            mapped_error.__cause__ = exc
            raise mapped_error from exc
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
    """Return custom type coercions for CockroachDB AsyncPG."""
    return {
        dt.datetime: _convert_datetime_param,
        dt.date: _convert_date_param,
        dt.time: _convert_time_param,
        **build_uuid_coercions(native=True),
    }


@mypyc_attr(allow_interpreted_subclasses=False)
class _JsonEncoder:
    __slots__ = ("_encoder",)

    def __init__(self, encoder: "Callable[[Any], str]") -> None:
        self._encoder = encoder

    def __call__(self, value: Any) -> bytes:
        return _encode_json_payload(value, self._encoder)


@mypyc_attr(allow_interpreted_subclasses=False)
class _JsonDecoder:
    __slots__ = ("_decoder",)

    def __init__(self, decoder: "Callable[[str], Any]") -> None:
        self._decoder = decoder

    def __call__(self, value: Any) -> Any:
        return _decode_json_payload(value, self._decoder)


@mypyc_attr(allow_interpreted_subclasses=False)
class _JsonbEncoder:
    __slots__ = ("_encoder",)

    def __init__(self, encoder: "Callable[[Any], str]") -> None:
        self._encoder = encoder

    def __call__(self, value: Any) -> bytes:
        return _encode_jsonb_payload(value, self._encoder)


@mypyc_attr(allow_interpreted_subclasses=False)
class _JsonbDecoder:
    __slots__ = ("_decoder",)

    def __init__(self, decoder: "Callable[[str], Any]") -> None:
        self._decoder = decoder

    def __call__(self, value: Any) -> Any:
        return _decode_jsonb_payload(value, self._decoder)


def _encode_json_payload(value: Any, encoder: "Callable[[Any], str]") -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw_bytes = value.tobytes() if isinstance(value, memoryview) else bytes(value)
        if raw_bytes.startswith(_JSONB_BINARY_VERSION):
            return raw_bytes[1:]
        return raw_bytes

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


def _resolve_connection_alias(config: "dict[str, Any]", target: str, aliases: "tuple[str, ...]") -> None:
    """Normalize connection alias keys in-place using the first present alias."""
    selected: Any = _UNSET
    for key in aliases:
        if key in config:
            value = config.pop(key)
            if selected is _UNSET:
                selected = value
    if selected is not _UNSET:
        config[target] = selected


def _create_postgres_error(
    error: Any, code: "str | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception wrapping a CockroachDB AsyncPG driver exception."""
    msg = f"PostgreSQL {description} [{code}]: {error}" if code else f"PostgreSQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def _register_exception_mappings() -> None:
    """Register native asyncpg exception types on the adapter dispatcher."""
    if asyncpg is None:
        return
    _EXCEPTION_MAPPING_DISPATCHER.register(
        asyncpg.exceptions.ReadOnlySQLTransactionError, ("25006", ReadOnlyTransactionError, "read-only transaction")
    )
    _EXCEPTION_MAPPING_DISPATCHER.register(
        asyncpg.exceptions.UniqueViolationError, ("23505", UniqueViolationError, "unique constraint violation")
    )
    _EXCEPTION_MAPPING_DISPATCHER.register(
        asyncpg.exceptions.ForeignKeyViolationError,
        ("23503", ForeignKeyViolationError, "foreign key constraint violation"),
    )
    _EXCEPTION_MAPPING_DISPATCHER.register(
        asyncpg.exceptions.NotNullViolationError, ("23502", NotNullViolationError, "not-null constraint violation")
    )
    _EXCEPTION_MAPPING_DISPATCHER.register(
        asyncpg.exceptions.CheckViolationError, ("23514", CheckViolationError, "check constraint violation")
    )
    _EXCEPTION_MAPPING_DISPATCHER.register(
        asyncpg.exceptions.IntegrityConstraintViolationError,
        ("23000", IntegrityError, "integrity constraint violation"),
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
        asyncpg.exceptions.InvalidAuthorizationSpecificationError,
        ("28000", PermissionDeniedError, "authorization error"),
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


driver_profile = build_profile()

default_statement_config = build_statement_config()

_register_exception_mappings()
