"""IBM Db2 adapter compiled helpers and exception translation."""

import re
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Final, Literal
from urllib.parse import parse_qsl, unquote, urlsplit

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
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
    UniqueViolationError,
)
from sqlspec.utils.config_tools import parse_odbc_connection_string
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
    "normalize_column_names",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "parse_db2_dsn",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
)

_CLI_KEYWORDS: Final[tuple[tuple[str, str], ...]] = (
    ("database", "DATABASE"),
    ("hostname", "HOSTNAME"),
    ("port", "PORT"),
    ("protocol", "PROTOCOL"),
    ("user", "UID"),
    ("password", "PWD"),
    ("current_schema", "CURRENTSCHEMA"),
    ("security", "SECURITY"),
    ("ssl_server_certificate", "SSLSERVERCERTIFICATE"),
    ("authentication", "AUTHENTICATION"),
    ("connect_timeout", "CONNECTTIMEOUT"),
)
_CLI_KEYWORD_TO_KEY: Final[dict[str, str]] = {keyword: key for key, keyword in _CLI_KEYWORDS}
_SUPPORTED_CONNECTION_KEYS: Final[frozenset[str]] = frozenset({
    *(key for key, _ in _CLI_KEYWORDS),
    "autocommit",
    "dsn",
    "extra",
    "health_check_interval",
    "pool_recycle_seconds",
})
_INTEGER_KEYS: Final[tuple[str, ...]] = ("port", "connect_timeout")
_TRUE_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES: Final[frozenset[str]] = frozenset({"0", "false", "no", "off"})
_CLI_KEYWORD_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z][A-Za-z0-9_]*")
_BRACED_MIN_LENGTH: Final[int] = 2
_DEFAULT_PORT: Final[int] = 50000
_DEFAULT_PROTOCOL: Final[str] = "TCPIP"
IMPLICIT_UPPER_COLUMN_PATTERN: Final[re.Pattern[str]] = re.compile(r"^(?!\d)(?:[A-Z0-9_]+)$")
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
    "42601": (SQLParsingError, "sql syntax error"),
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
    "SQL0104N": (SQLParsingError, "sql syntax error"),
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


def normalize_column_names(column_names: "list[str]", lowercase: bool) -> "list[str]":
    """Lowercase column names that Db2 folded to uppercase.

    Names made only of uppercase letters, digits and underscores (and not starting with a digit)
    are lowercased; any other name, such as a quoted mixed-case identifier, is returned unchanged.

    Args:
        column_names: Column names as reported by the cursor description.
        lowercase: Whether to lowercase implicit-uppercase names.

    Returns:
        Normalized column names in their original order.
    """
    if not lowercase:
        return column_names
    return [name.lower() if name and IMPLICIT_UPPER_COLUMN_PATTERN.fullmatch(name) else name for name in column_names]


def resolve_column_names(
    description: Sequence[Any] | None,
    column_name_cache: dict[int, tuple[Any, list[str]]] | None = None,
    *,
    lowercase: bool,
) -> list[str]:
    """Extract ordered column names from Db2 cursor description metadata.

    Args:
        description: DB-API cursor description.
        column_name_cache: Optional cache keyed by description identity; it stores normalized names.
        lowercase: Whether to lowercase names Db2 folded to uppercase.

    Returns:
        Column names in description order.
    """
    if not description:
        return []
    if column_name_cache is None:
        return normalize_column_names([str(desc[0]) for desc in description], lowercase)
    cache_key = id(description)
    cached = column_name_cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]
    names = normalize_column_names([str(desc[0]) for desc in description], lowercase)
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
    *,
    lowercase: bool,
) -> tuple[list[Any], list[str], Literal["dict", "tuple", "record"]]:
    """Collect Db2 rows, preserving dictionary or tuple row shape.

    Args:
        fetched_data: Fetched rows, or a cursor to fetch them from.
        description: DB-API cursor description used when ``fetched_data`` holds rows.
        column_name_cache: Optional cache of resolved column names.
        lowercase: Whether to lowercase names Db2 folded to uppercase.

    Returns:
        Rows, column names, and the row format.
    """
    if hasattr(fetched_data, "fetchall") and not isinstance(fetched_data, (list, tuple)):
        cursor = fetched_data
        fetched = cursor.fetchall() or []
        desc = getattr(cursor, "description", None)
        column_names = resolve_column_names(desc, column_name_cache, lowercase=lowercase)
        if not fetched:
            return [], column_names, "tuple"
        if isinstance(fetched[0], dict):
            return list(fetched), column_names, "dict"
        return list(fetched), column_names, "tuple"

    column_names = resolve_column_names(description, column_name_cache, lowercase=lowercase)
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
    *, json_serializer: Callable[[Any], str] | None = None, json_deserializer: Callable[[str], Any] | None = None
) -> StatementConfig:
    """Construct the Db2 statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "db2"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def apply_driver_features(
    statement_config: StatementConfig, driver_features: Mapping[str, Any] | None
) -> tuple[StatementConfig, dict[str, Any]]:
    """Apply Db2 driver feature flags to statement configuration."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)
    features.setdefault("enable_lowercase_column_names", True)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def _parse_bool(key: str, value: str) -> bool:
    """Parse a boolean connection value.

    Args:
        key: Connection keyword the value belongs to.
        value: Raw text value.

    Returns:
        bool: The parsed boolean.

    Raises:
        ImproperConfigurationError: When the text is not a recognized boolean.
    """
    lowered = value.strip().lower()
    if lowered in _TRUE_VALUES:
        return True
    if lowered in _FALSE_VALUES:
        return False
    msg = f"Db2 connection parameter {key!r} must be a boolean"
    raise ImproperConfigurationError(msg)


def _parse_int(key: str, value: Any) -> int:
    """Parse an integer connection value.

    Args:
        key: Canonical connection key the value belongs to.
        value: Raw value.

    Returns:
        int: The parsed integer.

    Raises:
        ImproperConfigurationError: When the value is not an integer.
    """
    if isinstance(value, bool):
        msg = f"Db2 connection parameter {key!r} must be an integer"
        raise ImproperConfigurationError(msg)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text.isdigit():
        msg = f"Db2 connection parameter {key!r} must be an integer"
        raise ImproperConfigurationError(msg)
    return int(text)


def _assign_dsn_value(params: dict[str, Any], extra: dict[str, Any], keyword: str, value: str) -> None:
    """Store one DSN keyword under its canonical key, or in ``extra`` when it is not modeled.

    Args:
        params: Canonical parameters collected so far.
        extra: Additional CLI keywords collected so far, keyed by original spelling.
        keyword: Keyword as written in the DSN.
        value: Unquoted keyword value.
    """
    upper = keyword.upper()
    if upper == "AUTOCOMMIT":
        params["autocommit"] = _parse_bool("autocommit", value)
        return
    key = _CLI_KEYWORD_TO_KEY.get(upper)
    if key is None:
        extra[keyword] = value
    elif key in _INTEGER_KEYS:
        params[key] = _parse_int(key, value)
    else:
        params[key] = value


def parse_db2_dsn(dsn: str) -> dict[str, Any]:
    """Parse a Db2 CLI connection string or ``db2://`` URL into canonical connection keys.

    Supported forms:
        - ``KEY=VALUE;...`` CLI keywords. Values wrapped in ``{...}`` may contain ``;``.
        - ``db2://user:password@host:port/database?Keyword=Value``.

    Known CLI keywords (``DATABASE``, ``HOSTNAME``, ``PORT``, ``PROTOCOL``, ``UID``, ``PWD``,
    ``CURRENTSCHEMA``, ``SECURITY``, ``SSLSERVERCERTIFICATE``, ``AUTHENTICATION``,
    ``CONNECTTIMEOUT``) map to their canonical keys case-insensitively and ``AUTOCOMMIT`` maps to
    the ``autocommit`` flag. Every other keyword is returned under ``extra`` with its original
    spelling.

    Args:
        dsn: Connection string or URL.

    Returns:
        dict[str, Any]: Canonical connection parameters, with ``extra`` present only when
        unmodeled keywords were found.
    """
    params: dict[str, Any] = {}
    extra: dict[str, Any] = {}
    if "://" in dsn:
        parsed = urlsplit(dsn)
        if parsed.username is not None:
            params["user"] = unquote(parsed.username)
        if parsed.password is not None:
            params["password"] = unquote(parsed.password)
        if parsed.hostname is not None:
            params["hostname"] = parsed.hostname
        if parsed.port is not None:
            params["port"] = parsed.port
        path = parsed.path.lstrip("/")
        if path:
            params["database"] = unquote(path)
        for keyword, value in parse_qsl(parsed.query, keep_blank_values=True):
            _assign_dsn_value(params, extra, keyword, value)
    else:
        for keyword, raw_value in parse_odbc_connection_string(dsn):
            value = raw_value
            if len(value) >= _BRACED_MIN_LENGTH and value[0] == "{" and value[-1] == "}":
                value = value[1:-1].replace("}}", "}")
            _assign_dsn_value(params, extra, keyword, value)
    if extra:
        params["extra"] = extra
    return params


def _merge_extra(dsn_extra: Mapping[str, Any], explicit_extra: Mapping[str, Any]) -> dict[str, Any]:
    """Merge additional CLI keywords case-insensitively, letting explicit entries win.

    Args:
        dsn_extra: Keywords parsed from a DSN.
        explicit_extra: Keywords configured directly.

    Returns:
        dict[str, Any]: Merged keywords keyed by the winning spelling.

    Raises:
        ImproperConfigurationError: When a keyword has a dedicated connection parameter.
    """
    merged: dict[str, tuple[str, Any]] = {}
    for source in (dsn_extra, explicit_extra):
        for keyword, value in source.items():
            upper = str(keyword).upper()
            if upper in _CLI_KEYWORD_TO_KEY or upper == "AUTOCOMMIT":
                canonical = _CLI_KEYWORD_TO_KEY.get(upper, "autocommit")
                msg = f"Db2 connection keyword {keyword!r} must be configured with the {canonical!r} parameter"
                raise ImproperConfigurationError(msg)
            merged[upper] = (str(keyword), value)
    return dict(merged.values())


def build_connection_config(connection_config: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize a Db2 connection configuration.

    A ``dsn`` is parsed and merged underneath the explicit keys (explicit keys and explicit
    ``extra`` entries win). ``port`` defaults to ``50000`` and ``protocol`` to ``TCPIP`` only when
    ``hostname`` is set; without a hostname the database name is treated as a cataloged alias.

    Args:
        connection_config: Connection parameters using the canonical keys of
            ``Db2PoolParams``.

    Returns:
        dict[str, Any]: Normalized parameters without ``dsn``.

    Raises:
        ImproperConfigurationError: When an unsupported key is present, ``database`` is missing,
            or a keyword or value cannot be rendered into a CLI connection string.
    """
    config = {key: value for key, value in connection_config.items() if value is not None}
    unsupported = sorted(key for key in config if key not in _SUPPORTED_CONNECTION_KEYS)
    if unsupported:
        msg = f"Unsupported Db2 connection parameter(s): {', '.join(unsupported)}"
        raise ImproperConfigurationError(msg)

    explicit_extra = config.pop("extra", None) or {}
    if not isinstance(explicit_extra, Mapping):
        msg = "Db2 connection parameter 'extra' must be a mapping of CLI keywords to values"
        raise ImproperConfigurationError(msg)
    dsn_extra: dict[str, Any] = {}
    dsn = config.pop("dsn", None)
    if dsn is not None:
        if not isinstance(dsn, str):
            msg = "Db2 connection parameter 'dsn' must be a string"
            raise ImproperConfigurationError(msg)
        dsn_params = parse_db2_dsn(dsn)
        dsn_extra = dsn_params.pop("extra", {})
        for key, value in dsn_params.items():
            config.setdefault(key, value)

    if not config.get("database"):
        msg = "Db2 connection requires a 'database' name"
        raise ImproperConfigurationError(msg)
    if config.get("hostname"):
        config.setdefault("port", _DEFAULT_PORT)
        config.setdefault("protocol", _DEFAULT_PROTOCOL)
    for key in _INTEGER_KEYS:
        if key in config:
            config[key] = _parse_int(key, config[key])

    extra = _merge_extra(dsn_extra, explicit_extra)
    if extra:
        config["extra"] = extra
    build_dsn_string(config)
    return config


def _format_cli_value(keyword: str, value: Any) -> str:
    """Render one ``KEYWORD=value`` pair for a Db2 CLI connection string.

    Values containing ``;`` or ``{`` or leading/trailing whitespace are wrapped in braces; booleans
    render as ``1``/``0``.

    Args:
        keyword: CLI keyword.
        value: Value to render.

    Returns:
        str: The rendered pair without a trailing ``;``.

    Raises:
        ImproperConfigurationError: When the keyword is not a plain CLI keyword or the value
            contains ``}``, which CLI connection strings cannot represent.
    """
    if _CLI_KEYWORD_PATTERN.fullmatch(keyword) is None:
        msg = f"Invalid Db2 connection keyword: {keyword!r}"
        raise ImproperConfigurationError(msg)
    if isinstance(value, bool):
        return f"{keyword}={'1' if value else '0'}"
    text = str(value)
    if "}" in text:
        msg = f"Db2 connection value for {keyword} cannot contain '}}'"
        raise ImproperConfigurationError(msg)
    if ";" in text or "{" in text or text != text.strip():
        return f"{keyword}={{{text}}}"
    return f"{keyword}={text}"


def build_dsn_string(config: Mapping[str, Any]) -> str:
    """Render a normalized Db2 configuration as an IBM CLI connection string.

    Modeled parameters render first in a fixed keyword order, followed by ``extra`` keywords in
    insertion order. ``autocommit`` and pool settings are never rendered.

    Args:
        config: Normalized connection configuration.

    Returns:
        str: Connection string such as ``DATABASE=sample;HOSTNAME=db;PORT=50000;PROTOCOL=TCPIP;``.
    """
    parts: list[str] = []
    for key, keyword in _CLI_KEYWORDS:
        value = config.get(key)
        if value is not None:
            parts.append(_format_cli_value(keyword, value))
    extra = config.get("extra") or {}
    for extra_keyword, extra_value in extra.items():
        if extra_value is not None:
            parts.append(_format_cli_value(str(extra_keyword), extra_value))
    return "".join(f"{part};" for part in parts)


driver_profile = build_profile()
default_statement_config = build_statement_config()
