"""IBM Db2 adapter compiled helpers and exception translation."""

import re
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
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
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount

if TYPE_CHECKING:
    from logging import Logger

__all__ = (
    "apply_driver_features",
    "build_connection_config",
    "build_dsn_string",
    "build_insert_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "parse_db2_dsn",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
)

_SQLSTATE_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?:SQLSTATE[=:\s]+|state[:=\s]+)([0-9A-Z]{5})\b", re.IGNORECASE
)
_SQLCODE_PATTERN: Final[re.Pattern[str]] = re.compile(r"\b(SQL-?[0-9]{3,5}[A-Z]?)\b", re.IGNORECASE)

_SQLSTATE_MAP: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "23505": (UniqueViolationError, "unique constraint violation"),
    "23503": (ForeignKeyViolationError, "foreign key constraint violation"),
    "23502": (NotNullViolationError, "not-null constraint violation"),
    "23513": (CheckViolationError, "check constraint violation"),
    "40001": (DeadlockError, "deadlock or serialization failure"),
    "42501": (PermissionDeniedError, "permission denied"),
    "08001": (DatabaseConnectionError, "database connection unavailable"),
    "08003": (DatabaseConnectionError, "database connection does not exist"),
    "08004": (DatabaseConnectionError, "server rejected connection"),
    "57014": (QueryTimeoutError, "statement execution canceled or timed out"),
    "22001": (DataError, "character data right truncation"),
    "22003": (DataError, "numeric value out of range"),
    "22007": (DataError, "invalid datetime format"),
    "22012": (DataError, "division by zero"),
}

_SQLCODE_MAP: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "SQL0803N": (UniqueViolationError, "unique constraint violation"),
    "SQL0530N": (ForeignKeyViolationError, "foreign key constraint violation"),
    "SQL0407N": (NotNullViolationError, "not-null constraint violation"),
    "SQL0545N": (CheckViolationError, "check constraint violation"),
    "SQL0911N": (DeadlockError, "deadlock or timeout occurred"),
    "SQL0551N": (PermissionDeniedError, "permission denied"),
    "SQL0552N": (PermissionDeniedError, "authorization error"),
    "SQL0900N": (DatabaseConnectionError, "connection does not exist or was severed"),
    "SQL1042C": (DatabaseConnectionError, "unexpected system error occurred"),
    "SQL30081N": (DatabaseConnectionError, "communication failure"),
}


_SQLSTATE_LENGTH: Final[int] = 5


def _extract_sqlstate(error: Exception) -> str | None:
    """Extract 5-character SQLSTATE code from exception attributes or message text."""
    explicit_state = getattr(error, "sqlstate", None) or getattr(error, "state", None)
    if isinstance(explicit_state, str) and len(explicit_state.strip()) == _SQLSTATE_LENGTH:
        return explicit_state.strip().upper()
    matches = _SQLSTATE_PATTERN.findall(str(error))
    if matches:
        return str(matches[-1]).upper()
    return None


def _extract_sqlcode(error: Exception) -> str | None:
    """Extract standard IBM Db2 SQLCODE identifier (e.g. SQL0803N) from exception."""
    explicit_code = getattr(error, "error_code", None) or getattr(error, "sqlcode", None)
    if isinstance(explicit_code, str) and explicit_code.strip():
        code = explicit_code.strip().upper()
        if not code.startswith("SQL"):
            code = f"SQL{code}"
        return code
    if isinstance(explicit_code, int):
        return f"SQL{abs(explicit_code):04d}N"
    matches = _SQLCODE_PATTERN.findall(str(error))
    if matches:
        return str(matches[-1]).upper()
    return None


def create_mapped_exception(error: Exception, *, logger: "Logger | None" = None) -> SQLSpecError:
    """Map a Db2 database exception to the appropriate SQLSpec exception class.

    Args:
        error: Caught Db2 driver exception.
        logger: Optional logger for diagnostic messages.

    Returns:
        SQLSpecError: Mapped domain exception wrapping the original error.
    """
    sqlstate = _extract_sqlstate(error)
    if sqlstate and sqlstate in _SQLSTATE_MAP:
        error_cls, description = _SQLSTATE_MAP[sqlstate]
        return error_cls(f"Db2 SQLSTATE {sqlstate}: {description}. Original error: {error}")

    sqlcode = _extract_sqlcode(error)
    if sqlcode and sqlcode in _SQLCODE_MAP:
        error_cls, description = _SQLCODE_MAP[sqlcode]
        return error_cls(f"Db2 SQLCODE {sqlcode}: {description}. Original error: {error}")

    if logger is not None and (sqlstate or sqlcode):
        logger.debug("Unmapped Db2 SQLSTATE: %s, SQLCODE: %s", sqlstate, sqlcode)

    message = str(error).lower()
    if "unique" in message or "duplicate" in message:
        return UniqueViolationError(f"Db2 unique constraint violation. Original error: {error}")
    if "foreign key" in message:
        return ForeignKeyViolationError(f"Db2 foreign key constraint violation. Original error: {error}")
    if "null" in message and "cannot be null" in message:
        return NotNullViolationError(f"Db2 not-null constraint violation. Original error: {error}")
    if "deadlock" in message or "lock timeout" in message:
        return DeadlockError(f"Db2 deadlock or lock timeout. Original error: {error}")
    if "connection" in message or "communication" in message:
        return DatabaseConnectionError(f"Db2 connection error. Original error: {error}")
    if "permission" in message or "not authorized" in message:
        return PermissionDeniedError(f"Db2 permission denied. Original error: {error}")

    for cls in type(error).__mro__:
        name = cls.__name__.lower()
        if "integrityerror" in name:
            return IntegrityError(f"Db2 integrity error. Original error: {error}")
        if "operationalerror" in name:
            return OperationalError(f"Db2 operational error. Original error: {error}")
        if "dataerror" in name:
            return DataError(f"Db2 data error. Original error: {error}")

    return SQLSpecError(f"Db2 database error. Original error: {error}")


def format_identifier(identifier: str) -> str:
    """Format a Db2 SQL identifier with standard double quotes."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Identifier name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"')
    return ".".join(quote_identifier(part) for part in parts)


def build_insert_statement(table: str, columns: list[str]) -> str:
    """Build a parameterized INSERT statement for Db2."""
    column_clause = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize query parameters for Db2 cursor execution."""
    if parameters is None:
        return None
    if isinstance(parameters, list):
        return tuple(parameters)
    return parameters


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize batch execution parameters for Db2 executemany."""
    if parameters is None:
        return None
    if isinstance(parameters, list):
        return tuple(tuple(item) if isinstance(item, list) else item for item in parameters)
    return parameters


def resolve_column_names(
    description: Sequence[Any] | None,
    column_name_cache: dict[int, tuple[Any, list[str]]] | None = None,
) -> list[str]:
    """Extract ordered column names from Db2 cursor description metadata."""
    if not description:
        return []
    if column_name_cache is None:
        return [str(desc[0]) for desc in description]
    cache_key = id(description)
    cached = column_name_cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]
    names = [str(desc[0]) for desc in description]
    column_name_cache[cache_key] = (description, names)
    return names


def resolve_rowcount(cursor: Any) -> int:
    """Safely extract affected rowcount from a Db2 cursor."""
    if has_rowcount(cursor):
        count = cursor.rowcount
        return int(count) if count is not None and count >= 0 else -1
    return -1


def resolve_many_rowcount(cursor: Any, parameters: Any, fallback_count: int = 0) -> int:
    """Resolve rowcount for batch operations with fallback to parameter count."""
    count = resolve_rowcount(cursor)
    if count >= 0:
        return count
    if parameters is not None and hasattr(parameters, "__len__"):
        return len(parameters)
    return fallback_count


def collect_rows(
    fetched_data: Any,
    description: Sequence[Any] | None = None,
    column_name_cache: dict[int, tuple[Any, list[str]]] | None = None,
) -> tuple[list[Any], list[str], Literal["dict", "tuple", "record"]]:
    """Collect Db2 rows, preserving dictionary or tuple row shape."""
    if hasattr(fetched_data, "fetchall") and not isinstance(fetched_data, (list, tuple)):
        cursor = fetched_data
        fetched = cursor.fetchall() or []
        desc = getattr(cursor, "description", None)
        column_names = resolve_column_names(desc, column_name_cache)
        if not fetched:
            return [], column_names, "tuple"
        if isinstance(fetched[0], dict):
            return list(fetched), column_names, "dict"
        return list(fetched), column_names, "tuple"

    column_names = resolve_column_names(description, column_name_cache)
    if not fetched_data:
        return [], column_names, "tuple"
    if isinstance(fetched_data[0], dict):
        return list(fetched_data), column_names, "dict"
    return list(fetched_data), column_names, "tuple"


def _custom_type_coercions() -> dict[type, Callable[[Any], Any]]:
    """Return custom parameter type coercions for Db2."""
    return dict(build_uuid_coercions())


def build_profile() -> DriverParameterProfile:
    """Construct the Db2 driver parameter profile."""
    return DriverParameterProfile(
        name="db2",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="db2",
    )


def build_statement_config(
    *,
    json_serializer: Callable[[Any], str] | None = None,
    json_deserializer: Callable[[str], Any] | None = None,
) -> StatementConfig:
    """Construct the Db2 statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "db2"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def apply_driver_features(
    statement_config: StatementConfig,
    driver_features: Mapping[str, Any] | None,
) -> tuple[StatementConfig, dict[str, Any]]:
    """Apply Db2 driver feature flags to statement configuration."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def parse_db2_dsn(dsn: str) -> dict[str, Any]:
    """Parse a Db2 connection DSN or URL into keyword arguments.

    Supports:
        - URL format: db2://user:password@host:port/database
        - DSN format: DATABASE=name;HOSTNAME=host;PORT=port;PROTOCOL=TCPIP;UID=user;PWD=password;
    """
    params: dict[str, Any] = {}
    if "://" in dsn:
        from urllib.parse import parse_qs, unquote, urlsplit

        parsed = urlsplit(dsn)
        if parsed.username is not None:
            params["username"] = unquote(parsed.username)
        if parsed.password is not None:
            params["password"] = unquote(parsed.password)
        if parsed.hostname is not None:
            params["hostname"] = parsed.hostname
        if parsed.port is not None:
            params["port"] = parsed.port
        path = parsed.path.lstrip("/")
        if path:
            params["database"] = unquote(path)
        if parsed.query:
            query = parse_qs(parsed.query)
            for q_key, q_vals in query.items():
                if q_vals:
                    val_str = q_vals[-1]
                    if val_str.lower() == "true":
                        params[q_key] = True
                    elif val_str.lower() == "false":
                        params[q_key] = False
                    elif val_str.isdigit():
                        params[q_key] = int(val_str)
                    else:
                        params[q_key] = val_str
        return params

    for item in dsn.split(";"):
        if "=" in item:
            key_part, val_part = item.split("=", 1)
            key = key_part.strip().upper()
            val = val_part.strip()
            if key in ("DATABASE", "DB"):
                params["database"] = val
            elif key in ("HOSTNAME", "HOST", "SERVER"):
                params["hostname"] = val
            elif key == "PORT":
                params["port"] = int(val) if val.isdigit() else val
            elif key == "PROTOCOL":
                params["protocol"] = val
            elif key in ("UID", "USER", "USERNAME"):
                params["username"] = val
            elif key in ("PWD", "PASSWORD"):
                params["password"] = val
            else:
                params[key_part.strip()] = val
    return params


def build_connection_config(connection_config: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize raw connection configuration mapping for Db2.

    Args:
        connection_config: Raw connection parameters mapping.

    Returns:
        dict[str, Any]: Normalized configuration dictionary.
    """
    config = dict(connection_config)
    dsn = config.pop("dsn", None) or config.pop("url", None) or config.pop("connection_string", None)
    if dsn is not None and isinstance(dsn, str):
        dsn_params = parse_db2_dsn(dsn)
        for key, value in dsn_params.items():
            config.setdefault(key, value)

    database = config.pop("db", None) or config.pop("database", "SAMPLE")
    config["database"] = database

    hostname = config.pop("host", None) or config.pop("server", None) or config.pop("hostname", "localhost")
    config["hostname"] = hostname

    port = config.pop("port", 50000)
    config["port"] = int(port)

    protocol = config.pop("protocol", "TCPIP")
    config["protocol"] = protocol

    username = config.pop("user", None) or config.pop("uid", None) or config.pop("username", None)
    if username is not None:
        config["username"] = username

    password = config.pop("pwd", None) or config.pop("password", None)
    if password is not None:
        config["password"] = password

    return config


def build_dsn_string(config: Mapping[str, Any]) -> str:
    """Build a standard IBM CLI connection string (DSN) from configuration parameters.

    Args:
        config: Connection configuration dictionary.

    Returns:
        str: Formatted DSN string (e.g. DATABASE=sample;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;).
    """
    parts = [
        f"DATABASE={config.get('database', 'SAMPLE')}",
        f"HOSTNAME={config.get('hostname', 'localhost')}",
        f"PORT={config.get('port', 50000)}",
        f"PROTOCOL={config.get('protocol', 'TCPIP')}",
    ]
    if config.get("username"):
        parts.append(f"UID={config['username']}")
    if config.get("password"):
        parts.append(f"PWD={config['password']}")

    extra = config.get("extra", {})
    for key, value in extra.items():
        parts.append(f"{key}={value}")

    return ";".join(parts) + ";"


driver_profile = build_profile()
default_statement_config = build_statement_config()
