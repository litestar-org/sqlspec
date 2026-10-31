"""SAP SQL Anywhere adapter compiled helpers."""

from collections.abc import Callable, Mapping, Sized
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.dialects.sybase import SQLAnywhere as _SQLAnywhereDialect
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
    SerializationConflictError,
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

__all__ = (
    "DEFAULT_SQLANYWHERE_PORT",
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
    "extract_sqlstate_and_sqlcode",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "quote_double_identifier",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
    "split_schema_and_table",
)

DEFAULT_SQLANYWHERE_PORT: Final[int] = 2638
_COLUMN_CACHE_MAX_SIZE: Final[int] = 256
_SQLANYDB_FULL_ARGS_LEN: Final[int] = 3
_SQLANYDB_PAIR_ARGS_LEN: Final[int] = 2
_POOL_CONFIG_KEYS: Final[frozenset[str]] = frozenset({
    "pool_min_size",
    "pool_max_size",
    "pool_timeout",
    "pool_recycle_seconds",
    "health_check_interval",
})
_SQLCODE_MAPPING: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    -193: (UniqueViolationError, "primary key is not unique"),
    -196: (UniqueViolationError, "unique index violation"),
    -194: (ForeignKeyViolationError, "foreign key constraint violation"),
    -198: (ForeignKeyViolationError, "primary key is referenced by foreign key"),
    -195: (NotNullViolationError, "column cannot be NULL"),
    -209: (CheckViolationError, "invalid value for column check constraint"),
    -1091: (CheckViolationError, "check constraint violation"),
    -306: (DeadlockError, "deadlock detected"),
    -307: (DeadlockError, "all threads are blocked"),
    -684: (DeadlockError, "rollback occurred due to deadlock"),
    -210: (SerializationConflictError, "row is locked by another user"),
    -1175: (SerializationConflictError, "snapshot isolation conflict"),
    -131: (SQLParsingError, "SQL syntax error"),
    -141: (NotFoundError, "table not found"),
    -143: (NotFoundError, "column not found"),
    -265: (NotFoundError, "procedure not found"),
    -83: (DatabaseConnectionError, "specified database not found"),
    -100: (DatabaseConnectionError, "database server not found"),
    -103: (DatabaseConnectionError, "invalid user ID or password"),
    -308: (DatabaseConnectionError, "connection was terminated"),
    -832: (DatabaseConnectionError, "connection error"),
    -101: (OperationalError, "not connected to a database"),
    -157: (DataError, "data conversion error"),
    -158: (DataError, "numeric value out of range"),
    -638: (DataError, "right truncation of string data"),
    -121: (PermissionDeniedError, "permission denied"),
}
_SQLSTATE_EXACT_MAPPING: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "23505": (UniqueViolationError, "unique constraint violation"),
    "23503": (ForeignKeyViolationError, "foreign key constraint violation"),
    "23502": (NotNullViolationError, "not-null constraint violation"),
    "23506": (CheckViolationError, "check constraint violation"),
    "23514": (CheckViolationError, "check constraint violation"),
    "40001": (DeadlockError, "serialization/deadlock failure"),
    "42000": (SQLParsingError, "SQL syntax error"),
    "42S02": (NotFoundError, "table not found"),
    "42S22": (NotFoundError, "column not found"),
    "42501": (PermissionDeniedError, "permission denied"),
    "28000": (DatabaseConnectionError, "invalid authorization specification"),
}


def quote_double_identifier(identifier: str) -> str:
    """Quote a SQL Anywhere identifier with double quotes."""
    cleaned = identifier.strip()
    if (cleaned.startswith('"') and cleaned.endswith('"')) or (cleaned.startswith("[") and cleaned.endswith("]")):
        cleaned = cleaned[1:-1].replace('""', '"').replace("]]", "]")
    return f'"{cleaned.replace(chr(34), chr(34) * 2)}"'


def escape_sql_literal(value: str) -> str:
    """Escape single quotes inside a SQL string literal."""
    return value.replace("'", "''")


def split_schema_and_table(identifier: str, *, default_schema: str = "DBA") -> tuple[str, str]:
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
    """Format a SQL Anywhere identifier with double-quote quoting."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars='"', allow_bracket_quotes=True)
    return ".".join(quote_double_identifier(part) for part in parts)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    """Build a QMARK-parameterized INSERT statement for SQL Anywhere."""
    column_clause = ", ".join(quote_double_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for SQL Anywhere sqlanydb execute calls."""
    if parameters is None:
        return ()
    if isinstance(parameters, list):
        return tuple(parameters)
    return parameters


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for SQL Anywhere sqlanydb executemany calls."""
    return parameters


def build_profile() -> "DriverParameterProfile":
    """Create the SAP SQL Anywhere driver parameter profile."""
    return DriverParameterProfile(
        name="sqlanywhere",
        default_style=ParameterStyle.QMARK,
        supported_styles={
            ParameterStyle.QMARK,
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_COLON,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.POSITIONAL_PYFORMAT,
        },
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=_custom_type_coercions(),
        default_dialect="sqlanywhere",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the SAP SQL Anywhere statement configuration."""
    return build_statement_config_from_profile(
        driver_profile,
        statement_overrides={"dialect": "sqlanywhere"},
        json_serializer=json_serializer or to_json,
        json_deserializer=json_deserializer or from_json,
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply SQL Anywhere driver feature overrides to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Normalize connection config into ``sqlanydb.connect(**kwargs)`` parameters.

    Maps friendly SQLSpec keys into SAP SQL Anywhere native connection string keys:
    - ``user`` / ``username`` -> ``uid``
    - ``password`` -> ``pwd``
    - ``database`` / ``db`` -> ``dbn``
    - ``server`` / ``engine`` / ``server_name`` -> ``eng``
    - ``database_file`` -> ``dbf``
    - ``host`` + ``port`` -> ``host="host:port"`` (default port ``2638``)
    """
    normalized: dict[str, Any] = dict(connection_config)

    extra = normalized.pop("extra", None)
    if isinstance(extra, Mapping):
        for key, value in extra.items():
            if value is not None:
                normalized.setdefault(key, value)

    for pool_key in _POOL_CONFIG_KEYS:
        normalized.pop(pool_key, None)

    for alias_key in ("user", "username"):
        alias_val = normalized.pop(alias_key, None)
        if alias_val is not None and "uid" not in normalized:
            normalized["uid"] = alias_val

    password_val = normalized.pop("password", None)
    if password_val is not None and "pwd" not in normalized:
        normalized["pwd"] = password_val

    for db_key in ("database", "db"):
        db_val = normalized.pop(db_key, None)
        if db_val is not None and "dbn" not in normalized:
            normalized["dbn"] = db_val

    for eng_key in ("server", "engine", "server_name"):
        eng_val = normalized.pop(eng_key, None)
        if eng_val is not None and "eng" not in normalized:
            normalized["eng"] = eng_val

    dbf_val = normalized.pop("database_file", None)
    if dbf_val is not None and "dbf" not in normalized:
        normalized["dbf"] = dbf_val

    port_val = normalized.pop("port", None)
    host_val = normalized.get("host")
    if isinstance(host_val, str) and host_val and ":" not in host_val:
        resolved_port = int(port_val) if port_val is not None else DEFAULT_SQLANYWHERE_PORT
        normalized["host"] = f"{host_val}:{resolved_port}"

    return {key: value for key, value in normalized.items() if value is not None}


def resolve_column_names(
    description: "Sequence[Any] | None", cache: "dict[int, tuple[Sequence[Any], list[str]]]"
) -> "list[str]":
    """Resolve and cache column names from cursor description."""
    if not description:
        return []
    desc_id = id(description)
    cached = cache.get(desc_id)
    if cached is not None and cached[0] is description:
        return cached[1]
    names = [col[0] for col in description]
    if len(cache) >= _COLUMN_CACHE_MAX_SIZE:
        cache.pop(next(iter(cache)))
    cache[desc_id] = (description, names)
    return names


def collect_rows(
    fetched_data: "Sequence[Any] | None",
    description: "Sequence[Any] | None",
    cache: "dict[int, tuple[Sequence[Any], list[str]]]",
) -> "tuple[list[Any], list[str], Literal['dict', 'tuple']]":
    """Collect SQL Anywhere fetched rows into a row list, column names, and row format."""
    if not description:
        return [], [], "tuple"
    column_names = resolve_column_names(description, cache)
    if not fetched_data:
        return [], column_names, "tuple"
    data = fetched_data if isinstance(fetched_data, list) else list(fetched_data)
    row_format: Literal["dict", "tuple"] = "dict" if isinstance(data[0], dict) else "tuple"
    return data, column_names, row_format


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a sqlanydb cursor."""
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int):
        return max(rowcount, 0)
    return 0


def resolve_many_rowcount(cursor: Any, parameters: Any, *, fallback_count: int | None = None) -> int:
    """Resolve affected row count for SQL Anywhere executemany."""
    rowcount = resolve_rowcount(cursor)
    if rowcount > 0:
        return rowcount
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameters, Sized):
        return len(parameters)
    return 0


def extract_sqlstate_and_sqlcode(error: BaseException) -> tuple[str | None, int | None]:
    """Extract ``(sqlstate, sqlcode)`` from a ``sqlanydb.Error`` instance."""
    sqlstate: str | None = getattr(error, "sqlstate", None)
    sqlcode: int | None = getattr(error, "errorcode", getattr(error, "sqlcode", None))

    raw_args = getattr(error, "args", None)
    args: list[Any] = list(raw_args) if isinstance(raw_args, tuple) else []
    if len(args) >= _SQLANYDB_FULL_ARGS_LEN:
        if sqlstate is None and isinstance(args[0], str):
            sqlstate = args[0]
        if sqlcode is None and isinstance(args[1], int):
            sqlcode = args[1]
    elif len(args) == _SQLANYDB_PAIR_ARGS_LEN:
        if isinstance(args[0], int) and sqlcode is None:
            sqlcode = args[0]
        elif isinstance(args[0], str) and sqlstate is None:
            sqlstate = args[0]
        if isinstance(args[1], int) and sqlcode is None:
            sqlcode = args[1]
    elif len(args) == 1 and isinstance(args[0], int) and sqlcode is None:
        sqlcode = args[0]

    return sqlstate, sqlcode


def create_mapped_exception(error: BaseException, *, logger: "Logger | None" = None) -> SQLSpecError:
    """Map a sqlanydb exception into the SQLSpec exception hierarchy."""
    _ = logger
    sqlstate, sqlcode = extract_sqlstate_and_sqlcode(error)

    if sqlcode is not None and sqlcode in _SQLCODE_MAPPING:
        exc_cls, label = _SQLCODE_MAPPING[sqlcode]
        return _create_error(error, sqlcode, sqlstate, exc_cls, label)

    if sqlstate is not None:
        upper_state = sqlstate.upper()
        if upper_state in _SQLSTATE_EXACT_MAPPING:
            exc_cls, label = _SQLSTATE_EXACT_MAPPING[upper_state]
            return _create_error(error, sqlcode, sqlstate, exc_cls, label)
        if upper_state.startswith("08"):
            return _create_error(error, sqlcode, sqlstate, DatabaseConnectionError, "connection error")
        if upper_state.startswith("22"):
            return _create_error(error, sqlcode, sqlstate, DataError, "data error")
        if upper_state.startswith("23"):
            return _create_error(error, sqlcode, sqlstate, IntegrityError, "integrity constraint violation")

    return _create_error(error, sqlcode, sqlstate, SQLSpecError, "database error")


def _bool_to_int(value: bool) -> int:
    return int(value)


def _custom_type_coercions() -> "dict[type, Callable[[Any], Any]]":
    return {bool: _bool_to_int, Decimal: str, **build_uuid_coercions()}


def _create_error(
    error: BaseException, sqlcode: int | None, sqlstate: str | None, error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    code_part = f"sqlcode={sqlcode}" if sqlcode is not None else ""
    state_part = f"sqlstate={sqlstate}" if sqlstate is not None else ""
    qualifier = ", ".join(part for part in (code_part, state_part) if part)
    prefix = f"SQL Anywhere {description} [{qualifier}]" if qualifier else f"SQL Anywhere {description}"
    msg = f"{prefix}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


_ = _SQLAnywhereDialect
driver_profile = build_profile()
default_statement_config = build_statement_config()
