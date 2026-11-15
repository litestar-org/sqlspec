"""CockroachDB psycopg adapter compiled helpers."""

import datetime as dt
import re
import secrets
from typing import TYPE_CHECKING, Any, Final, cast
from urllib.parse import urlsplit, urlunsplit

from mypy_extensions import mypyc_attr
from sqlglot import tokenize
from sqlglot.tokenizer_core import TokenType

from sqlspec.adapters.cockroach_psycopg._typing import (
    CockroachPsycopgComposed,
    CockroachPsycopgIdentifier,
    CockroachPsycopgSQL,
)
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_errors as pg_errors
from sqlspec.adapters.cockroach_psycopg._typing import cockroach_psycopg_module as psycopg
from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
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
    OperationCancelledError,
    PermissionDeniedError,
    QueryTimeoutError,
    ReadOnlyTransactionError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
    _classify_timeout_or_cancellation,
    map_sqlstate_to_exception,
)
from sqlspec.typing import import_optional_attr
from sqlspec.utils.config_tools import normalize_connection_config
from sqlspec.utils.routing import RoutingContext
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_json_list_converter, build_json_tuple_converter, build_uuid_coercions
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.adapters.cockroach_psycopg.config import CockroachPsycopgPoolConfig
    from sqlspec.core import ParameterStyleConfig
    from sqlspec.storage import StorageTelemetry

__all__ = (
    "DEFAULT_FOLLOWER_READ_STALENESS",
    "CockroachPsycopgRetryConfig",
    "apply_driver_features",
    "build_connection_config",
    "build_copy_from_command",
    "build_native_export",
    "build_native_import",
    "build_profile",
    "build_statement_config",
    "calculate_backoff_seconds",
    "create_mapped_exception",
    "driver_profile",
    "is_retryable_error",
    "native_export_telemetry",
    "native_import_telemetry",
    "normalize_native_export_query",
    "resolve_follower_read_options",
    "sanitize_native_storage_uri",
    "validate_follower_read_staleness",
)

_DEFAULT_MAX_RETRIES: Final[int] = 10
_DEFAULT_BASE_DELAY_MS: Final[float] = 50.0
_DEFAULT_MAX_DELAY_MS: Final[float] = 5000.0
_DEFAULT_ENABLE_LOGGING: Final[bool] = True
_MAX_CAUSE_CHAIN_DEPTH: Final[int] = 16
_SYSTEM_RANDOM: Final[secrets.SystemRandom] = secrets.SystemRandom()
_LITERAL_PERCENT_PATTERN: Final[re.Pattern[str]] = re.compile(r"%(?!s)")
_EXCEPTION_MAPPING: Final[dict[type[Any], tuple[str, type[SQLSpecError], str]]] = {}
_EXCEPTION_MAPPING_CACHE: Final[dict[type[Any], tuple[str, type[SQLSpecError], str]]] = {}
_STALENESS_LITERAL: Final[re.Pattern[str]] = re.compile(r"'[^'\\;]+'")
_STALENESS_FUNCTION: Final[re.Pattern[str]] = re.compile(
    r"(?:follower_read_timestamp|with_max_staleness|with_min_timestamp)"
    r"\(\s*(?:'[^'\\;]*'\s*(?:,\s*(?:'[^'\\;]*'|true|false)\s*)*)?\)",
    re.IGNORECASE,
)
DEFAULT_FOLLOWER_READ_STALENESS: Final[str] = "follower_read_timestamp()"


@mypyc_attr(allow_interpreted_subclasses=False)
class CockroachPsycopgRetryConfig:
    """CockroachDB psycopg transaction retry configuration."""

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
    def from_features(cls, driver_features: "Mapping[str, Any]") -> "CockroachPsycopgRetryConfig":
        """Build retry config from driver feature mappings."""
        return cls(
            max_retries=int(driver_features.get("max_retries", _DEFAULT_MAX_RETRIES)),
            base_delay_ms=float(driver_features.get("retry_delay_base_ms", _DEFAULT_BASE_DELAY_MS)),
            max_delay_ms=float(driver_features.get("retry_delay_max_ms", _DEFAULT_MAX_DELAY_MS)),
            enable_logging=bool(driver_features.get("enable_retry_logging", _DEFAULT_ENABLE_LOGGING)),
        )


def build_profile() -> "DriverParameterProfile":
    """Create the CockroachDB psycopg driver parameter profile."""
    return DriverParameterProfile(
        name="CockroachPsycopg",
        default_style=ParameterStyle.NUMERIC,
        supported_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        has_native_list_expansion=True,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="postgres",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the CockroachDB psycopg statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    parameter_config = _parameter_config(profile, serializer, deserializer)
    base_config = build_statement_config_from_profile(
        profile, json_serializer=serializer, json_deserializer=deserializer
    )
    return base_config.replace(parameter_config=parameter_config)


def build_copy_from_command(table: str, columns: "list[str]", *, binary: bool = False) -> "CockroachPsycopgComposed":
    """Construct a PostgreSQL-compatible COPY FROM STDIN command with quoted identifiers."""
    table_identifier = _compose_table_identifier(table)
    column_sql = CockroachPsycopgSQL(", ").join([CockroachPsycopgIdentifier(column) for column in columns])
    if binary:
        return CockroachPsycopgSQL("COPY {} ({}) FROM STDIN WITH (FORMAT BINARY)").format(table_identifier, column_sql)
    return CockroachPsycopgSQL("COPY {} ({}) FROM STDIN").format(table_identifier, column_sql)


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply CockroachDB psycopg driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    serializer = features.get("json_serializer", to_json)
    deserializer = features.get("json_deserializer", from_json)
    features.setdefault("json_serializer", serializer)
    features.setdefault("json_deserializer", deserializer)
    features.setdefault("enable_alloydb_columnar", False)
    features.setdefault("enable_pgcopy_binary", False)
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
    features.setdefault("enable_events", True)
    features.setdefault("events_backend", "poll_queue")

    parameter_config = _parameter_config(driver_profile, serializer, deserializer)
    statement_config = statement_config.replace(parameter_config=parameter_config)
    return statement_config, features


def build_connection_config(
    connection_config: "CockroachPsycopgPoolConfig | Mapping[str, Any] | None",
) -> dict[str, Any]:
    """Build normalized CockroachDB psycopg connection configuration, resolving aliases for libpq compatibility."""
    config: dict[str, Any] = dict(normalize_connection_config(connection_config))
    conninfo = _pop_alias(config, ("conninfo", "dsn", "url", "connection_string"))
    if conninfo is not None:
        config["conninfo"] = conninfo
    dbname = _pop_alias(config, ("dbname", "database", "db"))
    if dbname is not None:
        config["dbname"] = dbname
    user = _pop_alias(config, ("user", "username"))
    if user is not None:
        config["user"] = user

    managed: dict[str, str] = {}
    pattern = re.compile(
        r"(?<!\S)(?:-c\s*|--)(default_transaction_use_follower_reads|results_buffer_size|statement_timeout|idle_in_transaction_session_timeout)=([^\s]+)"
    )
    existing = str(config.get("options") or "")
    for match in pattern.finditer(existing):
        managed[match.group(1)] = match.group(2)
    remaining = pattern.sub("", existing).strip()
    if "default_transaction_use_follower_reads" in config:
        value = config.pop("default_transaction_use_follower_reads")
        if not isinstance(value, bool):
            msg = "default_transaction_use_follower_reads must be a boolean"
            raise ImproperConfigurationError(msg)
        managed["default_transaction_use_follower_reads"] = "on" if value else "off"
    for key in ("results_buffer_size", "statement_timeout", "idle_in_transaction_session_timeout"):
        if key not in config:
            continue
        value = config.pop(key)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            msg = f"{key} must be a non-negative integer"
            raise ImproperConfigurationError(msg)
        managed[key] = str(value)
    if existing or managed:
        option_parts: list[str] = []
        if remaining:
            option_parts.append(remaining)
        for key, value in managed.items():
            option_parts.append(f"-c {key}={value}")
        config["options"] = " ".join(option_parts)

    return config


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


def calculate_backoff_seconds(attempt: int, config: "CockroachPsycopgRetryConfig") -> float:
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
    format_sql = {"csv": "CSV", "parquet": "PARQUET"}[file_format]
    if not parameters:
        query = query.replace("%", "%%")
    elif "%" in query:
        query = _LITERAL_PERCENT_PATTERN.sub("%%", query)
    values: list[Any] = [uri]
    command = "EXPORT INTO " + format_sql + " " + "%s"
    if file_format == "csv" and "nullas" in options:
        values.append(options["nullas"])
        command += " WITH nullas = " + "%s"
    return command + " FROM (" + query.rstrip().removesuffix(";") + "\n)", [*values, *parameters]


def build_native_import(table: str, uri: str, file_format: str, options: "dict[str, Any]") -> "tuple[str, list[Any]]":
    """Quote the target identifier and bind explicit CSV conventions."""
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise ValueError(msg)
    target = ".".join(quote_identifier(part) for part in parts).replace("%", "%%")
    format_sql = {"csv": "CSV", "parquet": "PARQUET"}[file_format]
    values: list[Any] = [uri]
    command = "IMPORT INTO " + target + " " + format_sql + " DATA (" + "%s" + ")"
    clauses = []
    if file_format == "csv":
        for key in ("skip", "nullif"):
            if key in options:
                values.append(str(options[key]))
                clauses.append(key + " = " + "%s")
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
    """Map CockroachDB psycopg exceptions to SQLSpec exceptions."""
    _ = logger
    if (has_sqlstate(error) and getattr(error, "sqlstate", None) == "25006") or type(error).__name__ in {
        "ReadOnlySqlTransaction",
        "ReadOnlySQLTransactionError",
    }:
        mapped = ReadOnlyTransactionError(str(error))
        mapped.__cause__ = error if isinstance(error, BaseException) else None
        return mapped

    error_type = type(error)
    mapped_error = _EXCEPTION_MAPPING.get(error_type)
    if mapped_error is None:
        mapped_error = _resolve_exception_mapping(error_type)
    if mapped_error is not None:
        mapped_error_code, error_class, description = mapped_error
        sqlstate_val = getattr(error, "sqlstate", None)
        sqlstate_code = str(sqlstate_val) if sqlstate_val else mapped_error_code
        if sqlstate_code == "57014":
            termination_class = _classify_timeout_or_cancellation(str(error)) or OperationalError
            return _create_postgres_error(error, sqlstate_code, termination_class, description)
        if sqlstate_code == "55P03":
            return _create_postgres_error(error, sqlstate_code, QueryTimeoutError, description)
        if sqlstate_code == "25006":
            return _create_postgres_error(error, sqlstate_code, ReadOnlyTransactionError, "read-only transaction")
        if sqlstate_code.startswith("25"):
            return _create_postgres_error(error, sqlstate_code, TransactionError, description)
        return _create_postgres_error(error, sqlstate_code, error_class, description)

    sqlstate_attr = getattr(error, "sqlstate", None) if has_sqlstate(error) else None
    error_code: str | None = str(sqlstate_attr) if sqlstate_attr is not None else None
    if error_code:
        if error_code == "57014":
            termination_class = _classify_timeout_or_cancellation(str(error)) or OperationalError
            return _create_postgres_error(error, error_code, termination_class, "query terminated")
        if error_code == "55P03":
            return _create_postgres_error(error, error_code, QueryTimeoutError, "lock timeout")
        if error_code == "25006":
            return _create_postgres_error(error, error_code, ReadOnlyTransactionError, "read-only transaction")
        if error_code.startswith("25"):
            return _create_postgres_error(error, error_code, TransactionError, "invalid transaction state")
        exc_class = map_sqlstate_to_exception(error_code)
        if exc_class:
            return _create_postgres_error(error, error_code, exc_class, "database error")

    return _create_postgres_error(error, error_code, SQLSpecError, "database error")


def _pop_alias(config: "dict[str, Any]", aliases: "tuple[str, ...]") -> Any:
    """Pop every alias key from config and return the first non-None value."""
    found = None
    for alias in aliases:
        value = config.pop(alias, None)
        if found is None and value is not None:
            found = value
    return found


def _compose_table_identifier(table: str) -> "CockroachPsycopgComposed":
    parts = split_qualified_identifier(table, quote_chars='"', allow_bracket_quotes=False)
    if not parts:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    identifiers = [CockroachPsycopgIdentifier(part) for part in parts]
    return CockroachPsycopgSQL(".").join(identifiers)


def _identity(value: Any) -> Any:
    return value


def _custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for CockroachDB psycopg."""
    return {dt.datetime: _identity, dt.date: _identity, dt.time: _identity, **build_uuid_coercions(native=True)}


def _parameter_config(
    profile: "DriverParameterProfile", serializer: "Callable[[Any], str]", deserializer: "Callable[[str], Any]"
) -> "ParameterStyleConfig":
    """Construct parameter configuration with shared JSON serializer support."""
    base_config = build_statement_config_from_profile(
        profile, json_serializer=serializer, json_deserializer=deserializer
    ).parameter_config

    updated_type_map = dict(base_config.type_coercion_map)
    updated_type_map[list] = build_json_list_converter(serializer)
    updated_type_map[tuple] = build_json_tuple_converter(serializer)

    return base_config.replace(type_coercion_map=updated_type_map)


def _create_postgres_error(
    error: Any, code: "str | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception wrapping a CockroachDB psycopg error."""
    msg = f"PostgreSQL {description} [{code}]: {error}" if code else f"PostgreSQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def _resolve_exception_mapping(error_type: type[Any]) -> "tuple[str, type[SQLSpecError], str] | None":
    mapped_error = _EXCEPTION_MAPPING_CACHE.get(error_type)
    if mapped_error is not None:
        return mapped_error

    for base_type in error_type.__mro__[1:]:
        mapped_error = _EXCEPTION_MAPPING.get(base_type)
        if mapped_error is not None:
            _EXCEPTION_MAPPING_CACHE[error_type] = mapped_error
            return mapped_error
    return None


def _register_exception_mappings() -> None:
    """Populate psycopg exception class mappings for CockroachDB error translation."""
    pool_timeout = import_optional_attr("psycopg_pool", "PoolTimeout")
    if isinstance(pool_timeout, type):
        _EXCEPTION_MAPPING[pool_timeout] = ("", ConnectionTimeoutError, "connection pool timeout")

    _EXCEPTION_MAPPING.update({
        pg_errors.ReadOnlySqlTransaction: ("25006", ReadOnlyTransactionError, "read-only transaction"),
        pg_errors.LockNotAvailable: ("55P03", QueryTimeoutError, "lock not available"),
        pg_errors.UniqueViolation: ("23505", UniqueViolationError, "unique constraint violation"),
        pg_errors.ForeignKeyViolation: ("23503", ForeignKeyViolationError, "foreign key constraint violation"),
        pg_errors.NotNullViolation: ("23502", NotNullViolationError, "not-null constraint violation"),
        pg_errors.CheckViolation: ("23514", CheckViolationError, "check constraint violation"),
        pg_errors.IntegrityError: ("23000", IntegrityError, "integrity constraint violation"),
        pg_errors.DeadlockDetected: ("40P01", DeadlockError, "deadlock detected"),
        pg_errors.SerializationFailure: ("40001", SerializationConflictError, "serialization failure"),
        pg_errors.QueryCanceled: ("57014", OperationCancelledError, "query canceled"),
        pg_errors.InsufficientPrivilege: ("42501", PermissionDeniedError, "insufficient privilege"),
        pg_errors.AdminShutdown: ("57P01", ConnectionTimeoutError, "admin shutdown"),
        pg_errors.CannotConnectNow: ("57P03", ConnectionTimeoutError, "cannot connect now"),
        pg_errors.SyntaxError: ("42601", SQLParsingError, "SQL syntax error"),
        psycopg.OperationalError: ("", DatabaseConnectionError, "operational error"),
        psycopg.DataError: ("", DataError, "data error"),
        psycopg.NotSupportedError: ("", SQLSpecError, "not supported"),
        psycopg.InterfaceError: ("", DatabaseConnectionError, "interface error"),
    })


driver_profile = build_profile()

default_statement_config = build_statement_config()

_register_exception_mappings()
