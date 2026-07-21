"""mssql-python adapter core helpers."""

import re
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, Final

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
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_uuid_coercions

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from logging import Logger

    from sqlspec.core import StatementConfig

__all__ = (
    "MSSQL_PYTHON_VERSION",
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "build_statement_config",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "materialize_tuple_rows",
)

_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(([-]?\d+)(?:,|\))")
_MSSQL_CONSTRAINT_547: Final[int] = 547
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
_CONNECT_KWARG_KEYS: Final[set[str]] = {"autocommit", "attrs_before", "native_uuid"}
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
    4060: (DatabaseConnectionError, "database connection error"),
    53: (DatabaseConnectionError, "database connection error"),
    1205: (DeadlockError, "deadlock detected"),
    -2: (QueryTimeoutError, "query timeout"),
    8114: (DataError, "data conversion error"),
    1105: (OperationalError, "operational error"),
}


def create_mapped_exception(error: Exception, *, logger: "Logger | None" = None) -> SQLSpecError:
    """Map a mssql-python exception to SQLSpec's exception hierarchy."""
    error_number = _extract_error_number(error)
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
    """Materialize mssql-python ``Row`` objects into plain tuples.

    ``mssql-python`` returns ``mssql_python.Row`` objects that are iterable and
    indexable but are not ``tuple`` subclasses. The driver reports
    ``row_format="tuple"``, so fetched rows are converted to real tuples to keep
    that contract accurate when results are materialized.
    """
    if not fetched:
        return []
    return [tuple(row) for row in fetched]


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Merge mssql-python driver-feature defaults with caller overrides."""
    defaults: dict[str, Any] = {"use_pool": True, "json_serializer": to_json, "json_deserializer": from_json}
    defaults.update(driver_features or {})
    return statement_config, defaults


def build_connection_config(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Build an ODBC connection string and mssql-python connect kwargs."""
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
        return str(connection_string), connect_kwargs

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

    extra = config.get("extra")
    if isinstance(extra, dict):
        parts.extend(f"{key}={_format_connection_value(value)}" for key, value in extra.items() if value is not None)

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


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> "StatementConfig":
    """Construct the mssql-python statement configuration."""
    return build_statement_config_from_profile(
        driver_profile, statement_overrides={"dialect": "tsql"}, json_serializer=json_serializer or to_json
    )


def _constraint_exception_from_message(error: Exception) -> "SQLSpecError | None":
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


def _extract_error_number(exc: Exception) -> "int | None":
    matches = _ERROR_NUMBER_PATTERN.findall(str(exc))
    if not matches:
        return None
    try:
        return int(matches[-1])
    except ValueError:
        return None


MSSQL_PYTHON_VERSION: Final[tuple[int, int, int]] = _parse_version()
driver_profile = build_profile()
default_statement_config = build_statement_config()
