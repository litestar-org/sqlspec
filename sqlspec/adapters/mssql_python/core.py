"""mssql-python adapter core helpers."""

import re
from collections.abc import Sized
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlspec.core.parameters import ParameterStyle
from sqlspec.core.parameters._registry import build_statement_config_from_profile
from sqlspec.core.parameters._types import DriverParameterProfile
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SerializationConflictError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.config_tools import parse_odbc_connection_string
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from logging import Logger

    from sqlspec.core import StatementConfig

__all__ = (
    "MSSQL_PYTHON_VERSION",
    "apply_driver_features",
    "build_connection_config",
    "build_insert_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "escape_sql_literal",
    "extract_error_number",
    "format_identifier",
    "materialize_tuple_rows",
    "quote_bracket_identifier",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
    "split_schema_and_table",
)

_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:\(([-]?\d+)(?:,|\))|\bMsg\s+([-]?\d+)\b)")
_MSSQL_CONSTRAINT_547: Final[int] = 547
_COLUMN_CACHE_MAX_SIZE: Final[int] = 256
_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r"(\d+)")
_VERSION_PART_COUNT: Final[int] = 3
_CONNECTION_STRING_KEYS: Final[tuple[tuple[tuple[str, ...], str, bool], ...]] = (
    (("server",), "Server", False),
    (("database", "db"), "Database", False),
    (("uid", "user", "username"), "UID", False),
    (("pwd", "password"), "PWD", False),
    (("authentication",), "Authentication", False),
    (("trusted_connection",), "Trusted_Connection", False),
    (("encrypt",), "Encrypt", False),
    (("trust_server_certificate", "trust"), "TrustServerCertificate", False),
    (("hostname_in_certificate", "hostnameincertificate"), "HostnameInCertificate", False),
    (("server_certificate", "servercertificate"), "ServerCertificate", False),
    (("server_spn", "serverspn"), "ServerSPN", False),
    (("multi_subnet_failover", "multisubnetfailover"), "MultiSubnetFailover", False),
    (("application_intent", "applicationintent"), "ApplicationIntent", False),
    (("connect_retry_count", "connectretrycount"), "ConnectRetryCount", False),
    (("connect_retry_interval", "connectretryinterval"), "ConnectRetryInterval", False),
    (("keep_alive", "keepalive"), "KeepAlive", False),
    (("keep_alive_interval", "keepaliveinterval"), "KeepAliveInterval", False),
    (("ip_address_preference", "ipaddresspreference"), "IpAddressPreference", False),
    (("packet_size", "packetsize", "packet size"), "PacketSize", False),
)
_CANONICAL_KEY_LOOKUP: Final[dict[str, str]] = {
    "address": "Server",
    "addr": "Server",
    **{alias.lower(): opt for aliases, opt, _ in _CONNECTION_STRING_KEYS for alias in aliases},
    **{opt.lower(): opt for _, opt, _ in _CONNECTION_STRING_KEYS},
}
_CONNECT_KWARG_KEYS: Final[set[str]] = {"autocommit", "attrs_before", "native_uuid", "token_provider"}
_CONNECT_TIMEOUT_KEYS: Final[tuple[str, ...]] = ("timeout", "connection_timeout", "login_timeout", "command_timeout")
_POOL_CONFIG_KEYS: Final[set[str]] = {"pool_size", "pool_idle_timeout", "pool_enabled"}
_IGNORED_CONNECTION_CONFIG_KEYS: Final[set[str]] = {
    "application_name",
    "app",
    "driver",
    "multiple_active_result_sets",
    "workstation_id",
}
_ERROR_CODE_MAPPING: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    2601: (UniqueViolationError, "unique constraint violation"),
    2627: (UniqueViolationError, "unique constraint violation"),
    515: (NotNullViolationError, "not-null constraint violation"),
    18456: (PermissionDeniedError, "permission denied"),
    229: (PermissionDeniedError, "permission denied"),
    230: (PermissionDeniedError, "permission denied"),
    262: (PermissionDeniedError, "permission denied"),
    4060: (DatabaseConnectionError, "database connection error"),
    53: (DatabaseConnectionError, "database connection error"),
    1205: (DeadlockError, "deadlock detected"),
    3960: (SerializationConflictError, "snapshot isolation conflict"),
    -2: (QueryTimeoutError, "query timeout"),
    1222: (QueryTimeoutError, "lock request time out period exceeded"),
    102: (SQLParsingError, "SQL syntax error"),
    105: (SQLParsingError, "unclosed quotation mark"),
    156: (SQLParsingError, "incorrect syntax near keyword"),
    207: (SQLParsingError, "invalid column name"),
    208: (SQLParsingError, "invalid object name"),
    241: (DataError, "data conversion error"),
    242: (DataError, "datetime conversion out of range"),
    245: (DataError, "data conversion error"),
    2628: (DataError, "string or binary data would be truncated"),
    8114: (DataError, "data conversion error"),
    8115: (DataError, "arithmetic overflow error"),
    8152: (DataError, "string or binary data would be truncated"),
    1105: (OperationalError, "operational error"),
}


def quote_bracket_identifier(identifier: str) -> str:
    """Quote a T-SQL identifier with square brackets."""
    cleaned = identifier.strip()
    if (cleaned.startswith("[") and cleaned.endswith("]")) or (cleaned.startswith('"') and cleaned.endswith('"')):
        cleaned = cleaned[1:-1].replace("]]", "]").replace('""', '"')
    return f"[{cleaned.replace(']', ']]')}]"


def escape_sql_literal(value: str) -> str:
    """Escape single quotes inside a SQL string literal."""
    return value.replace("'", "''")


def split_schema_and_table(identifier: str, *, default_schema: str = "dbo") -> tuple[str, str]:
    """Split a table reference into unquoted ``(schema, table)`` names."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=True)
    if not parts:
        return default_schema, cleaned
    if len(parts) == 1:
        return default_schema, parts[0]
    return parts[-2], parts[-1]


def format_identifier(identifier: str) -> str:
    """Format a T-SQL identifier with bracket quoting."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=True)
    return ".".join(quote_bracket_identifier(part) for part in parts)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    """Build an mssql-python INSERT statement with positional qmark placeholders."""
    column_clause = ", ".join(quote_bracket_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def resolve_column_names(
    description: "Sequence[Any] | None", column_name_cache: "dict[int, tuple[Any, list[str]]] | None" = None
) -> "list[str]":
    """Resolve ordered column names from cursor metadata."""
    if not description:
        return []
    if column_name_cache is None:
        return [str(desc[0]) for desc in description]
    cache_key = id(description)
    cached = column_name_cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]
    column_names = [str(desc[0]) for desc in description]
    if len(column_name_cache) >= _COLUMN_CACHE_MAX_SIZE:
        column_name_cache.pop(next(iter(column_name_cache)))
    column_name_cache[cache_key] = (description, column_names)
    return column_names


def collect_rows(
    fetched_data: "Sequence[Any] | None",
    description: "Sequence[Any] | None",
    column_name_cache: "dict[int, tuple[Any, list[str]]] | None" = None,
) -> "tuple[list[tuple[Any, ...]], list[str], Literal['tuple']]":
    """Collect mssql-python rows as plain tuples alongside column names."""
    column_names = resolve_column_names(description, column_name_cache)
    if not fetched_data:
        return [], column_names, "tuple"
    return materialize_tuple_rows(fetched_data), column_names, "tuple"


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from an mssql-python cursor."""
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount >= 0:
        return rowcount
    return 0


def resolve_many_rowcount(cursor: Any, parameters: Any, *, fallback_count: "int | None" = None) -> int:
    """Resolve executemany rowcount using cursor metadata with payload fallback."""
    rowcount = resolve_rowcount(cursor)
    if rowcount > 0:
        return rowcount
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameters, Sized):
        return len(parameters)
    return 0


def extract_error_number(exc: BaseException | None) -> int | None:
    """Extract numeric SQL Server error code using fast attribute/string parsing before regex fallback."""
    if exc is None:
        return None
    for attr in ("number", "error_code", "errno"):
        val = getattr(exc, attr, None)
        if isinstance(val, int) and not isinstance(val, bool) and val != 0:
            return val
    ddbc_err = getattr(exc, "ddbc_error", None)
    if isinstance(ddbc_err, str) and ddbc_err.startswith("("):
        end_idx = ddbc_err.find(",")
        if end_idx == -1:
            end_idx = ddbc_err.find(")")
        if end_idx != -1:
            num_str = ddbc_err[1:end_idx].strip()
            try:
                return int(num_str)
            except ValueError:
                pass

    if exc.args:
        first_arg = exc.args[0]
        if isinstance(first_arg, int) and not isinstance(first_arg, bool) and first_arg != 0:
            return first_arg
        if isinstance(first_arg, tuple) and first_arg:
            nested = first_arg[0]
            if isinstance(nested, int) and not isinstance(nested, bool) and nested != 0:
                return nested
        if isinstance(first_arg, str):
            msg = first_arg
            start_idx = msg.rfind("(")
            if start_idx != -1:
                end_idx = msg.find(",", start_idx)
                if end_idx == -1:
                    end_idx = msg.find(")", start_idx)
                if end_idx != -1:
                    num_str = msg[start_idx + 1 : end_idx].strip()
                    try:
                        return int(num_str)
                    except ValueError:
                        pass

    matches = _ERROR_NUMBER_PATTERN.findall(str(exc))
    if not matches:
        return None
    last_match = matches[-1]
    return int(last_match[0] or last_match[1])


def create_mapped_exception(error: BaseException, *, logger: "Logger | None" = None) -> SQLSpecError:
    """Map a SQL Server exception to SQLSpec's exception hierarchy."""
    error_number = extract_error_number(error)

    if error_number == _MSSQL_CONSTRAINT_547:
        message = str(error)
        if "check constraint" in message.lower():
            return CheckViolationError(f"SQL Server error 547: check constraint violation. Original error: {error}")
        return ForeignKeyViolationError(
            f"SQL Server error 547: foreign key constraint violation. Original error: {error}"
        )
    if error_number is not None:
        mapping = _ERROR_CODE_MAPPING.get(error_number)
        if mapping is not None:
            error_class, description = mapping
            return error_class(f"SQL Server error {error_number}: {description}. Original error: {error}")
        if logger is not None:
            logger.debug("Unmapped SQL Server error number: %s", error_number)

    constraint_exception = _constraint_exception_from_message(error)
    if constraint_exception is not None:
        return constraint_exception

    exc_name = type(error).__name__
    if exc_name == "IntegrityError":
        return IntegrityError(f"SQL Server integrity error. Original error: {error}")
    if exc_name == "OperationalError":
        return OperationalError(f"SQL Server operational error. Original error: {error}")
    if exc_name == "DataError":
        return DataError(f"SQL Server data error. Original error: {error}")
    return SQLSpecError(f"SQL Server database error. Original error: {error}")


def materialize_tuple_rows(fetched: "Sequence[Any] | None") -> "list[tuple[Any, ...]]":
    """Materialize mssql-python Row objects into plain tuples.

    Uses native tuple storage when available, avoiding a tuple copy per row.
    """
    if not fetched:
        return []
    first = fetched[0]
    if isinstance(first, tuple):
        return list(fetched) if not isinstance(fetched, list) else fetched
    if hasattr(first, "_values"):
        return [tuple(row._values) if not isinstance(row._values, tuple) else row._values for row in fetched]
    return [tuple(row) for row in fetched]


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Merge mssql-python driver-feature defaults with caller overrides."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("use_pool", True)
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None and (
        json_serializer is not to_json
        or json_deserializer is not from_json
        or statement_config.parameter_config.json_serializer is not to_json
    ):
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def build_connection_config(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Build an ODBC connection string and mssql-python connect kwargs.

    When both ``connection_string`` and discrete connection fields are provided,
    discrete fields take precedence and override matching keys in the connection
    string. Key names are normalized case-insensitively to prevent duplicate
    keywords, satisfying mssql-python driver requirements.

    Args:
        params: Raw connection configuration dictionary.

    Returns:
        ODBC connection string and connect kwargs.
    """
    config = dict(params)
    connect_kwargs = {key: config.pop(key) for key in tuple(config) if key in _CONNECT_KWARG_KEYS}
    timeout = _pop_alias(config, _CONNECT_TIMEOUT_KEYS, "timeout", strict=False)
    if timeout is not None:
        connect_kwargs["timeout"] = timeout
    for key in _POOL_CONFIG_KEYS:
        config.pop(key, None)
    for key in _IGNORED_CONNECTION_CONFIG_KEYS:
        config.pop(key, None)

    connection_string = config.pop("connection_string", None)
    if connection_string is not None:
        if not config:
            return str(connection_string), connect_kwargs

        options: dict[str, tuple[str, str]] = {}
        for raw_key, raw_value in parse_odbc_connection_string(str(connection_string)):
            canonical_key = _CANONICAL_KEY_LOOKUP.get(raw_key.lower(), raw_key)
            options[canonical_key.lower()] = (canonical_key, raw_value)

        server = _pop_alias(config, ("server", "address", "addr"), "server")
        port = config.pop("port", None)
        if server is not None:
            options["server"] = ("Server", _append_port(str(server), port))
        elif port is not None and "server" in options:
            disp, val = options["server"]
            options["server"] = (disp, _append_port(val, port))

        for keys, option_name, strict in _CONNECTION_STRING_KEYS:
            if option_name == "Server":
                continue
            value = _pop_alias(config, keys, option_name, strict=strict)
            if value is not None:
                options[option_name.lower()] = (option_name, _format_connection_value(value))

        extra = config.pop("extra", None)
        if isinstance(extra, dict):
            for extra_key, extra_value in extra.items():
                if extra_value is not None:
                    extra_key_str = str(extra_key)
                    canonical_extra_key = _CANONICAL_KEY_LOOKUP.get(extra_key_str.lower(), extra_key_str)
                    options[canonical_extra_key.lower()] = (canonical_extra_key, _format_connection_value(extra_value))

        for remaining_key, remaining_value in list(config.items()):
            if remaining_value is not None:
                canonical_remaining_key = _CANONICAL_KEY_LOOKUP.get(remaining_key.lower(), remaining_key)
                options[canonical_remaining_key.lower()] = (
                    canonical_remaining_key,
                    _format_connection_value(remaining_value),
                )
            config.pop(remaining_key, None)

        merged_parts = [f"{name}={val}" for name, val in options.values()]
        return ";".join(merged_parts) + ";", connect_kwargs

    server = _pop_alias(config, ("server", "address", "addr"), "server")
    if not server:
        msg = "mssql-python connection_config requires 'server' or 'connection_string'."
        raise ValueError(msg)
    config["server"] = _append_port(str(server), config.pop("port", None))

    parts: list[str] = []
    for keys, option_name, strict in _CONNECTION_STRING_KEYS:
        value = _pop_alias(config, keys, option_name, strict=strict)
        if value is None:
            continue
        parts.append(f"{option_name}={_format_connection_value(value)}")

    extra = config.pop("extra", None)
    if isinstance(extra, dict):
        parts.extend(f"{key}={_format_connection_value(value)}" for key, value in extra.items() if value is not None)

    for remaining_key, remaining_value in list(config.items()):
        if remaining_value is not None:
            canonical_remaining_key = _CANONICAL_KEY_LOOKUP.get(remaining_key.lower(), remaining_key)
            parts.append(f"{canonical_remaining_key}={_format_connection_value(remaining_value)}")
        config.pop(remaining_key, None)

    return ";".join(parts) + ";", connect_kwargs


def build_profile() -> "DriverParameterProfile":
    """Create the mssql-python driver parameter profile."""
    return DriverParameterProfile(
        name="mssql_python",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_PYFORMAT},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="tsql",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the mssql-python statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "tsql"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def _constraint_exception_from_message(error: BaseException) -> "SQLSpecError | None":
    """Classify SQL Server constraint messages when a driver omits the native error number."""
    message = str(error)
    normalized = message.lower()
    if "unique key constraint" in normalized or "duplicate key" in normalized:
        return UniqueViolationError(f"SQL Server unique constraint violation. Original error: {message}")
    if "cannot insert the value null" in normalized or "does not allow nulls" in normalized:
        return NotNullViolationError(f"SQL Server not-null constraint violation. Original error: {message}")
    if "check constraint" in normalized:
        return CheckViolationError(f"SQL Server check constraint violation. Original error: {message}")
    if "foreign key constraint" in normalized:
        return ForeignKeyViolationError(f"SQL Server foreign key constraint violation. Original error: {message}")
    return None


def _custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for mssql-python."""
    return {bool: _identity, int: _identity, float: _identity, bytes: _identity, **build_uuid_coercions(native=True)}


def _parse_version() -> tuple[int, int, int]:
    try:
        raw_version = version("mssql-python")
    except PackageNotFoundError:
        return 0, 0, 0
    parts = [int(value) for value in _VERSION_PATTERN.findall(raw_version)]
    while len(parts) < _VERSION_PART_COUNT:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def _identity(value: Any) -> Any:
    return value


def _format_connection_value(value: Any) -> str:
    if isinstance(value, bool):
        return "yes" if value else "no"
    return str(value)


def _pop_alias(config: dict[str, Any], aliases: tuple[str, ...], option_name: str, *, strict: bool = True) -> Any:
    found: list[tuple[str, Any]] = [(key, config[key]) for key in aliases if key in config and config[key] is not None]
    if not found:
        return None
    if strict:
        first_key, first_value = found[0]
        for key, value in found[1:]:
            if value != first_value:
                msg = f"Conflicting mssql-python connection aliases for {option_name}: {first_key} and {key}."
                raise ValueError(msg)
    _, value = found[0]
    for key, _ in found:
        config.pop(key, None)
    return value


def _append_port(server: str, port: Any) -> str:
    if not port or "," in server or ":" in server:
        return server
    return f"{server},{port}"


MSSQL_PYTHON_VERSION: Final[tuple[int, int, int]] = _parse_version()
driver_profile = build_profile()
default_statement_config = build_statement_config()
