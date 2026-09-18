"""arrow-odbc adapter core helpers."""

import re
from typing import TYPE_CHECKING, Any, Final

from sqlspec.core import DriverParameterProfile, ParameterStyle, build_statement_config_from_profile
from sqlspec.exceptions import (
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    ImproperConfigurationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_uuid_coercions

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlspec.core import StatementConfig

__all__ = (
    "apply_driver_features",
    "build_connection_config",
    "build_profile",
    "build_statement_config",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "resolve_dialect_from_dbms_name",
)


_CONNECT_KWARG_KEYS: Final[set[str]] = {"user", "password", "login_timeout_sec", "packet_size", "autocommit"}
_CONNECTION_STRING_KEYS: Final[tuple[tuple[str, str], ...]] = (
    ("dsn", "DSN"),
    ("driver", "Driver"),
    ("server", "Server"),
    ("host", "Server"),
    ("database", "Database"),
    ("uid", "UID"),
    ("pwd", "PWD"),
    ("trusted_connection", "Trusted_Connection"),
    ("trust_server_certificate", "TrustServerCertificate"),
    ("encrypt", "Encrypt"),
)
_DIALECT_PATTERNS: Final[tuple[tuple[str, tuple[str, ...]], ...]] = (
    ("mssql", ("sql server", "sqlserver", "microsoft sql", "msodbcsql")),
    ("oracle", ("oracle",)),
    ("mysql", ("mysql", "mariadb")),
    ("postgres", ("postgres", "postgresql")),
    ("sqlite", ("sqlite",)),
    ("duckdb", ("duckdb",)),
    ("snowflake", ("snowflake",)),
)
_ARROW_ODBC_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"Native error:\s*(-?\d+)")
_ODBC_PUNCTUATION: Final[str] = "[]{}(),;?*=!@"
_SQL_SERVER_DIAGNOSTIC_MARKERS: Final[tuple[str, ...]] = ("sql server", "msodbcsql")
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
    102: (SQLParsingError, "syntax error"),
}
_ARROW_ODBC_SQLSTATE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\b([0-9A-Z]{2}[0-9A-Z]{3})\b")
_SQLSTATE_CLASS_CODE_LEN: Final[int] = 2
_SQLSTATE_CLASS_MAPPING: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "08": (DatabaseConnectionError, "connection error"),
    "22": (DataError, "data error"),
    "23": (IntegrityError, "integrity constraint violation"),
    "28": (PermissionDeniedError, "invalid authorization"),
    "40": (TransactionError, "transaction rollback"),
    "42": (SQLParsingError, "syntax error or access rule violation"),
    "53": (OperationalError, "insufficient resources"),
    "57": (OperationalError, "operator intervention"),
}


def resolve_dialect_from_dbms_name(dbms_name: str | None) -> str:
    """Resolve an ODBC DBMS or driver name to a SQLSpec dialect name."""
    if not dbms_name:
        return "sqlite"
    lowered = dbms_name.lower()
    for dialect, patterns in _DIALECT_PATTERNS:
        if any(pattern in lowered for pattern in patterns):
            return dialect
    return "sqlite"


def create_mapped_exception(error: Exception, *, logger: Any | None = None) -> SQLSpecError:
    """Map an arrow-odbc exception to SQLSpec's exception hierarchy."""
    del logger
    message = str(error)
    if _is_sql_server_diagnostic(message):
        error_number = _extract_error_number(error)
        if error_number is not None:
            mapping = _ERROR_CODE_MAPPING.get(error_number)
            if mapping is not None:
                error_class, description = mapping
                return error_class(f"ODBC SQL Server error {error_number}: {description}. Original error: {error}")

    sqlstate = _extract_sqlstate(message)
    if sqlstate is not None:
        mapped = _SQLSTATE_CLASS_MAPPING.get(sqlstate[:_SQLSTATE_CLASS_CODE_LEN])
        if mapped is not None:
            error_class, description = mapped
            return error_class(f"ODBC error {sqlstate}: {description}. Original error: {error}")

    if "Incorrect syntax near" in message:
        return SQLParsingError(f"ODBC SQL parsing error. Original error: {error}")
    return SQLSpecError(f"ODBC database error. Original error: {error}")


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Merge arrow-odbc driver feature defaults with caller overrides."""
    defaults: dict[str, Any] = {
        "chunk_size": 65_536,
        "max_bytes_per_batch": 512 * 1024 * 1024,
        "max_text_size": 1024 * 1024,
        "max_binary_size": 1024 * 1024,
        "fetch_concurrently": True,
        "query_timeout_sec": None,
        "json_serializer": to_json,
        "json_deserializer": from_json,
    }
    defaults.update(driver_features or {})
    return statement_config, defaults


def build_connection_config(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Build arrow-odbc connection arguments using the 10.4 keyword names.

    Individual ODBC fields and ``extra`` entries are emitted first and an
    explicit ``connection_string`` follows them, so a field set on the config
    wins over the same option written inside the string. ODBC resolves a
    repeated keyword in favour of its first occurrence.

    Args:
        params: Raw connection configuration.

    Returns:
        The ODBC connection string and the ``connect`` keyword arguments.
    """
    config = dict(params)
    extra = config.pop("extra", None)
    if isinstance(extra, dict):
        config.update(extra)

    login_timeout = config.pop("login_timeout", None)
    if login_timeout is not None and "login_timeout_sec" not in config:
        config["login_timeout_sec"] = login_timeout

    connect_kwargs = {key: config.pop(key) for key in tuple(config) if key in _CONNECT_KWARG_KEYS}
    connection_string = config.pop("connection_string", None)

    parts: list[str] = []
    consumed: set[str] = set()
    for key, option_name in _CONNECTION_STRING_KEYS:
        value = config.get(key)
        if value is None:
            continue
        parts.append(f"{option_name}={_format_connection_value(option_name, value)}")
        consumed.add(key)

    for key, value in config.items():
        if key in consumed or value is None:
            continue
        parts.append(f"{key}={_format_connection_value(key, value)}")

    if connection_string is not None:
        suffix = str(connection_string)
        if not parts:
            return suffix, connect_kwargs
        return ";".join(parts) + ";" + suffix, connect_kwargs

    if not parts:
        msg = "arrow-odbc connection_config requires 'connection_string' or ODBC connection fields."
        raise ImproperConfigurationError(msg)

    return ";".join(parts) + ";", connect_kwargs


def build_profile() -> "DriverParameterProfile":
    """Create the arrow-odbc driver parameter profile."""
    return DriverParameterProfile(
        name="arrow_odbc",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="sqlite",
    )


def build_statement_config(*, dialect: str = "sqlite", json_serializer: "Any" = to_json) -> "StatementConfig":
    """Construct the arrow-odbc statement configuration."""
    return build_statement_config_from_profile(
        driver_profile, statement_overrides={"dialect": dialect}, json_serializer=json_serializer
    )


def _custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    """Return custom type coercions for arrow-odbc."""
    return {
        bool: _identity,
        int: _identity,
        float: _identity,
        str: _identity,
        bytes: _identity,
        **build_uuid_coercions(native=False),
    }


def _extract_sqlstate(message: str) -> "str | None":
    match = _ARROW_ODBC_SQLSTATE_PATTERN.search(message)
    return match.group(1) if match is not None else None


def _extract_error_number(error: Exception) -> "int | None":
    match = _ARROW_ODBC_ERROR_NUMBER_PATTERN.search(str(error))
    if match is None:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _identity(value: Any) -> Any:
    return value


def _is_sql_server_diagnostic(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in _SQL_SERVER_DIAGNOSTIC_MARKERS)


def _format_connection_value(option_name: str, value: Any) -> str:
    """Render a value for an ODBC connection string.

    A value the caller already brace-quoted is emitted unchanged, since the
    braced driver spelling is the usual way to write one. Any other value that
    carries edge whitespace or a character ODBC treats as punctuation is
    wrapped in braces so it cannot be read as the start of another keyword.

    Args:
        option_name: The connection string keyword this value belongs to.
        value: The configured value.

    Returns:
        The value rendered for inclusion in the connection string.

    Raises:
        ImproperConfigurationError: If the value contains a closing brace that
            does not terminate an already-quoted value. ODBC defines no escape
            for one, so quoting it would silently truncate the value.
    """
    if isinstance(value, bool):
        return "yes" if value else "no"
    text = str(value)
    if len(text) > 1 and text.startswith("{") and text.endswith("}") and text.count("}") == 1:
        return text
    if "}" in text:
        msg = (
            f"The arrow-odbc value for {option_name!r} contains a closing brace, "
            "which an ODBC connection string cannot represent."
        )
        raise ImproperConfigurationError(msg)
    if text and text == text.strip() and not any(character in text for character in _ODBC_PUNCTUATION):
        return text
    return "{" + text + "}"


driver_profile = build_profile()

default_statement_config = build_statement_config()
