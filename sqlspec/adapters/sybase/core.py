"""Sybase ASE adapter compiled helpers."""

import re
from collections.abc import Callable, Mapping, Sized
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.dialects.sybase import Sybase as _SybaseDialect
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_rowcount

if TYPE_CHECKING:
    from collections.abc import Sequence
    from logging import Logger

    from sqlspec.core.parameters import ParameterValidator

__all__ = (
    "DEFAULT_SYBASE_CONN_PROPERTIES",
    "DEFAULT_SYBASE_TDS_VERSION",
    "apply_driver_features",
    "build_connection_config",
    "build_insert_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "escape_literal_percent",
    "escape_sql_literal",
    "extract_error_number",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "quote_bracket_identifier",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
    "split_schema_and_table",
)

DEFAULT_SYBASE_CONN_PROPERTIES: Final[str] = "SET QUOTED_IDENTIFIER ON; SET ANSINULL ON; SET TEXTSIZE 2147483647;"
DEFAULT_SYBASE_TDS_VERSION: Final[str] = "5.0"

_ERROR_NUMBER_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:\(([-]?\d+)(?:,|\))|\bMsg\s+([-]?\d+)\b)")
_SYBASE_CONSTRAINT_547: Final[int] = 547
_COLUMN_CACHE_MAX_SIZE: Final[int] = 256
_ERROR_CODE_MAPPING: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    2601: (UniqueViolationError, "unique index violation"),
    2627: (UniqueViolationError, "unique constraint violation"),
    546: (ForeignKeyViolationError, "foreign key constraint violation"),
    548: (CheckViolationError, "check constraint violation"),
    233: (NotNullViolationError, "not-null constraint violation"),
    515: (NotNullViolationError, "not-null constraint violation"),
    1205: (DeadlockError, "deadlock detected"),
    12205: (QueryTimeoutError, "lock wait period exceeded"),
    -2: (QueryTimeoutError, "query timeout"),
    1222: (QueryTimeoutError, "lock request time out period exceeded"),
    102: (SQLParsingError, "SQL syntax error"),
    105: (SQLParsingError, "unclosed quotation mark"),
    156: (SQLParsingError, "incorrect syntax near keyword"),
    11021: (SQLParsingError, "invalid function invocation"),
    207: (NotFoundError, "invalid column name"),
    208: (NotFoundError, "object not found"),
    4002: (DatabaseConnectionError, "login failed"),
    4060: (DatabaseConnectionError, "cannot open database"),
    18456: (DatabaseConnectionError, "login failed"),
    20002: (DatabaseConnectionError, "Adaptive Server connection failed"),
    20009: (DatabaseConnectionError, "Adaptive Server is unavailable or does not exist"),
    1105: (OperationalError, "cannot allocate space in database"),
    2762: (OperationalError, "DDL command is not allowed within a multi-statement transaction"),
    20047: (OperationalError, "DBPROCESS is dead or not enabled"),
    168: (DataError, "numeric scale conversion error"),
    220: (DataError, "arithmetic overflow error"),
    241: (DataError, "datetime conversion error"),
    242: (DataError, "datetime conversion out of range"),
    245: (DataError, "data conversion error"),
    247: (DataError, "arithmetic overflow during conversion"),
    3606: (DataError, "arithmetic overflow occurred"),
    9502: (DataError, "character or binary data would be truncated"),
    229: (PermissionDeniedError, "permission denied"),
    230: (PermissionDeniedError, "permission denied"),
    262: (PermissionDeniedError, "permission denied"),
    10330: (PermissionDeniedError, "object permission denied"),
    10331: (PermissionDeniedError, "database permission denied"),
    10332: (PermissionDeniedError, "execute permission denied"),
}


def quote_bracket_identifier(identifier: str) -> str:
    """Quote a Sybase T-SQL identifier with square brackets."""
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
    """Format a Sybase T-SQL identifier with bracket quoting."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=True)
    return ".".join(quote_bracket_identifier(part) for part in parts)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    """Build a pymssql-compatible INSERT statement for Sybase ASE."""
    column_clause = ", ".join(quote_bracket_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def escape_literal_percent(sql: str, parameters: Any, validator: "ParameterValidator") -> str:
    """Escape literal percent characters before driver-side interpolation."""
    if not parameters or "%" not in sql:
        return sql
    keep = {info.position for info in validator.extract_parameters(sql)}
    return "".join("%%" if char == "%" and index not in keep else char for index, char in enumerate(sql))


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for Sybase pymssql execute calls."""
    if parameters is None:
        return None
    if isinstance(parameters, list):
        return tuple(parameters)
    return parameters


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for Sybase pymssql executemany calls."""
    return parameters


def build_profile() -> "DriverParameterProfile":
    """Create the Sybase ASE driver parameter profile."""
    return DriverParameterProfile(
        name="sybase",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_PYFORMAT},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="sybase",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the Sybase ASE statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "sybase"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply Sybase ASE driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
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


def create_mapped_exception(error: BaseException, *, logger: "Logger | None" = None) -> SQLSpecError:
    """Map a Sybase ASE exception to SQLSpec's exception hierarchy."""
    error_number = extract_error_number(error)

    if error_number == _SYBASE_CONSTRAINT_547:
        message = str(error)
        if "check constraint" in message.lower():
            return CheckViolationError(f"Sybase ASE error 547: check constraint violation. Original error: {error}")
        return ForeignKeyViolationError(
            f"Sybase ASE error 547: foreign key constraint violation. Original error: {error}"
        )
    if error_number is not None:
        mapping = _ERROR_CODE_MAPPING.get(error_number)
        if mapping is not None:
            error_class, description = mapping
            return error_class(f"Sybase ASE error {error_number}: {description}. Original error: {error}")
        if logger is not None:
            logger.debug("Unmapped Sybase ASE error number: %s", error_number)

    constraint_exception = _constraint_exception_from_message(error)
    if constraint_exception is not None:
        return constraint_exception

    exc_name = type(error).__name__
    if exc_name == "IntegrityError":
        return IntegrityError(f"Sybase ASE integrity error. Original error: {error}")
    if exc_name == "OperationalError":
        return OperationalError(f"Sybase ASE operational error. Original error: {error}")
    if exc_name == "DataError":
        return DataError(f"Sybase ASE data error. Original error: {error}")
    return SQLSpecError(f"Sybase ASE database error. Original error: {error}")


def resolve_column_names(
    description: "Sequence[Any] | None", column_name_cache: "dict[int, tuple[Any, list[str]]] | None" = None
) -> "list[str]":
    """Resolve ordered column names from cursor metadata."""
    if not description:
        return []
    if column_name_cache is None:
        return [desc[0] for desc in description]
    cache_key = id(description)
    cached = column_name_cache.get(cache_key)
    if cached is not None and cached[0] is description:
        return cached[1]
    column_names = [desc[0] for desc in description]
    if len(column_name_cache) >= _COLUMN_CACHE_MAX_SIZE:
        column_name_cache.pop(next(iter(column_name_cache)))
    column_name_cache[cache_key] = (description, column_names)
    return column_names


def collect_rows(
    fetched_data: "Sequence[Any] | None",
    description: "Sequence[Any] | None",
    column_name_cache: "dict[int, tuple[Any, list[str]]] | None" = None,
) -> "tuple[list[Any], list[str], Literal['dict', 'tuple', 'record']]":
    """Collect Sybase pymssql rows, preserving dictionary or tuple row shape."""
    column_names = resolve_column_names(description, column_name_cache)
    if not fetched_data:
        return [], column_names, "tuple"
    rows = fetched_data if isinstance(fetched_data, list) else list(fetched_data)
    if isinstance(rows[0], dict):
        return rows, column_names, "dict"
    return rows, column_names, "tuple"


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a Sybase pymssql cursor."""
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
    """Extract integer Sybase ASE error code from an exception if present.

    Checks native integer attributes (number, error_code, errno) or args[0] before regex.
    """
    if exc is None:
        return None
    for attr in ("number", "error_code", "errno"):
        val = getattr(exc, attr, None)
        if isinstance(val, int) and not isinstance(val, bool) and val != 0:
            return val
    if exc.args:
        first = exc.args[0]
        if isinstance(first, int) and not isinstance(first, bool) and first != 0:
            return first
        if isinstance(first, tuple) and first:
            nested = first[0]
            if isinstance(nested, int) and not isinstance(nested, bool) and nested != 0:
                return nested
    matches = _ERROR_NUMBER_PATTERN.findall(str(exc))
    if not matches:
        return None
    last_match = matches[-1]
    return int(last_match[0] or last_match[1])


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build a normalized Sybase ASE connection configuration dictionary.

    Injects ``DEFAULT_SYBASE_CONN_PROPERTIES``, ``autocommit=True``, ``port=5000``,
    and ``charset='UTF-8'`` by default, and strips ``tds_version`` into
    ``_sybase_tds_version`` so ``SybaseConnectionPool`` can negotiate TDS 5.0
    via ``os.environ['TDSVER']`` without triggering ``pymssql._mssql``'s Cython
    ``tds_version`` validator.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Normalized connection parameters.
    """
    config = dict(connection_config)
    extra = config.pop("extra", None)
    if isinstance(extra, Mapping):
        for extra_key, extra_value in extra.items():
            if extra_value is not None:
                config.setdefault(str(extra_key), extra_value)
    config.setdefault("server", config.pop("host", "localhost"))
    config.setdefault("port", 5000)
    config.setdefault("charset", "UTF-8")
    config.setdefault("autocommit", True)
    config.setdefault("conn_properties", DEFAULT_SYBASE_CONN_PROPERTIES)
    db_alias = config.pop("db", None)
    if "database" not in config and db_alias is not None:
        config["database"] = db_alias
    user_alias = config.pop("username", None)
    if "user" not in config and user_alias is not None:
        config["user"] = user_alias
    tds_version = str(config.pop("tds_version", config.pop("_sybase_tds_version", DEFAULT_SYBASE_TDS_VERSION)))
    config["_sybase_tds_version"] = tds_version
    return config


def _bool_to_int(value: bool) -> int:
    return int(value)


def _constraint_exception_from_message(error: BaseException) -> "SQLSpecError | None":
    """Classify Sybase ASE constraint messages when a driver omits the native error number."""
    message = str(error)
    normalized = message.lower()
    if "unique key constraint" in normalized or "duplicate key" in normalized:
        return UniqueViolationError(f"Sybase ASE unique constraint violation. Original error: {message}")
    if (
        "cannot insert the value null" in normalized
        or "does not allow nulls" in normalized
        or "does not allow null values" in normalized
    ):
        return NotNullViolationError(f"Sybase ASE not-null constraint violation. Original error: {message}")
    if "check constraint" in normalized:
        return CheckViolationError(f"Sybase ASE check constraint violation. Original error: {message}")
    if "foreign key constraint" in normalized:
        return ForeignKeyViolationError(f"Sybase ASE foreign key constraint violation. Original error: {message}")
    return None


def _custom_type_coercions() -> dict[type, Callable[[Any], Any]]:
    """Return custom type coercions for Sybase ASE."""
    coercions: dict[type, Callable[[Any], Any]] = {bool: _bool_to_int}
    coercions.update(build_uuid_coercions())
    return coercions


_ = _SybaseDialect
driver_profile = build_profile()
default_statement_config = build_statement_config()
