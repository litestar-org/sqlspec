"""mssql-python adapter core helpers."""

import re
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, Final

from sqlspec.core.parameters import ParameterStyle
from sqlspec.core.parameters._registry import build_statement_config_from_profile
from sqlspec.core.parameters._types import DriverParameterProfile
from sqlspec.exceptions import (
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
    from collections.abc import Callable
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
)

_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"\(([-]?\d+)\)")
_VERSION_PATTERN: Final[re.Pattern[str]] = re.compile(r"(\d+)")
_VERSION_PART_COUNT: Final[int] = 3
_CONNECTION_STRING_KEYS: Final[tuple[tuple[str, str], ...]] = (
    ("server", "Server"),
    ("database", "Database"),
    ("uid", "UID"),
    ("user", "UID"),
    ("pwd", "PWD"),
    ("password", "PWD"),
    ("authentication", "Authentication"),
    ("encrypt", "Encrypt"),
    ("trust_server_certificate", "TrustServerCertificate"),
    ("connection_timeout", "Connection Timeout"),
    ("command_timeout", "Command Timeout"),
    ("application_name", "Application Name"),
    ("workstation_id", "Workstation ID"),
    ("multiple_active_result_sets", "MultipleActiveResultSets"),
    ("application_intent", "ApplicationIntent"),
)
_CONNECT_KWARG_KEYS: Final[set[str]] = {"autocommit", "attrs_before", "timeout", "native_uuid"}
_POOL_CONFIG_KEYS: Final[set[str]] = {"pool_size", "pool_idle_timeout", "pool_enabled"}
_ERROR_CODE_MAPPING: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    2601: (UniqueViolationError, "unique constraint violation"),
    2627: (UniqueViolationError, "unique constraint violation"),
    547: (ForeignKeyViolationError, "foreign key or check constraint violation"),
    515: (NotNullViolationError, "not-null constraint violation"),
    18456: (PermissionDeniedError, "permission denied"),
    4060: (DatabaseConnectionError, "database connection error"),
    53: (DatabaseConnectionError, "database connection error"),
    1205: (DeadlockError, "deadlock detected"),
    -2: (QueryTimeoutError, "query timeout"),
    8114: (DataError, "data conversion error"),
    1105: (OperationalError, "operational error"),
}


def create_mapped_exception(exc: Exception, logger: "Logger | None" = None) -> Exception:
    """Map a mssql-python exception to SQLSpec's exception hierarchy."""
    error_number = _extract_error_number(exc)
    if error_number is not None:
        mapping = _ERROR_CODE_MAPPING.get(error_number)
        if mapping is not None:
            error_class, description = mapping
            return error_class(f"SQL Server error {error_number}: {description}. Original error: {exc}")
        if logger is not None:
            logger.debug("Unmapped SQL Server error number: %s", error_number)

    exc_name = type(exc).__name__
    if exc_name == "IntegrityError":
        return IntegrityError(f"SQL Server integrity error. Original error: {exc}")
    if exc_name == "OperationalError":
        return OperationalError(f"SQL Server operational error. Original error: {exc}")
    if exc_name == "DataError":
        return DataError(f"SQL Server data error. Original error: {exc}")
    return SQLSpecError(f"SQL Server database error. Original error: {exc}")


def apply_driver_features(features: "dict[str, Any] | None") -> dict[str, Any]:
    """Merge mssql-python driver-feature defaults with caller overrides."""
    defaults: dict[str, Any] = {"use_pool": True, "json_serializer": to_json, "json_deserializer": from_json}
    defaults.update(features or {})
    return defaults


def build_connection_config(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Build an ODBC connection string and mssql-python connect kwargs."""
    config = dict(params)
    connect_kwargs = {key: config.pop(key) for key in tuple(config) if key in _CONNECT_KWARG_KEYS}
    for key in _POOL_CONFIG_KEYS:
        config.pop(key, None)

    connection_string = config.pop("connection_string", None)
    if connection_string is not None:
        return str(connection_string), connect_kwargs

    server = config.get("server")
    if not server:
        msg = "mssql-python connection_config requires 'server' or 'connection_string'."
        raise ValueError(msg)
    config["server"] = _append_port(str(server), config.pop("port", None))

    parts: list[str] = []
    consumed: set[str] = set()
    for key, option_name in _CONNECTION_STRING_KEYS:
        if key not in config:
            continue
        value = config[key]
        if value is None:
            continue
        parts.append(f"{option_name}={_format_connection_value(value)}")
        consumed.add(key)

    extra = config.get("extra")
    if isinstance(extra, dict):
        parts.extend(f"{key}={_format_connection_value(value)}" for key, value in extra.items() if value is not None)
        consumed.add("extra")

    for key, value in config.items():
        if key in consumed or value is None:
            continue
        parts.append(f"{key}={_format_connection_value(value)}")

    return ";".join(parts) + ";", connect_kwargs


def _build_mssql_python_custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for mssql-python."""
    return {bool: _identity, int: _identity, float: _identity, bytes: _identity, **build_uuid_coercions(native=True)}


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
        custom_type_coercions=_build_mssql_python_custom_type_coercions(),
        default_dialect="tsql",
    )


def build_statement_config(*, json_serializer: "Callable[[Any], str] | None" = None) -> "StatementConfig":
    """Construct the mssql-python statement configuration."""
    return build_statement_config_from_profile(
        driver_profile, statement_overrides={"dialect": "tsql"}, json_serializer=json_serializer or to_json
    )


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
