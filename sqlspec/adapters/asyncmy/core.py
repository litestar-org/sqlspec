"""AsyncMy adapter compiled helpers."""

from collections.abc import Callable, Sized
from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_cursor_metadata, has_lastrowid, has_rowcount

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

__all__ = (
    "apply_driver_features",
    "build_insert_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "detect_json_columns",
    "detect_json_columns_from_description",
    "driver_profile",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "normalize_lastrowid",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_rowcount",
)

# MySQL error codes for constraint violations
MYSQL_ER_DUP_ENTRY = 1062
MYSQL_ER_NO_DEFAULT_FOR_FIELD = 1364
MYSQL_ER_CHECK_CONSTRAINT_VIOLATED = 3819

# MySQL error codes for permission/access errors
MYSQL_ER_DBACCESS_DENIED = 1044
MYSQL_ER_ACCESS_DENIED = 1045
MYSQL_ER_TABLEACCESS_DENIED = 1142

# MySQL error codes for transaction errors
MYSQL_ER_LOCK_WAIT_TIMEOUT = 1205
MYSQL_ER_LOCK_DEADLOCK = 1213

# MySQL error codes for connection errors
MYSQL_CR_CONNECTION_ERROR = 2002
MYSQL_CR_CONN_HOST_ERROR = 2003
MYSQL_CR_UNKNOWN_HOST = 2005
MYSQL_CR_SERVER_GONE_ERROR = 2006
MYSQL_CR_SERVER_LOST = 2013
MYSQL_SYNTAX_ERROR_MIN = 1064
MYSQL_SYNTAX_ERROR_MAX_EXCLUSIVE = 1100

_MYSQL_MIGRATION_ERROR_CODES = frozenset((1061, 1091))
_MYSQL_SQLSTATE_EXACT_DISPATCH: dict[str, tuple[type[SQLSpecError], str]] = {
    "23505": (UniqueViolationError, "unique constraint violation"),
    "23503": (ForeignKeyViolationError, "foreign key constraint violation"),
    "23502": (NotNullViolationError, "not-null constraint violation"),
    "23514": (CheckViolationError, "check constraint violation"),
}
_MYSQL_SQLSTATE_PREFIX_DISPATCH: dict[str, tuple[type[SQLSpecError], str]] = {
    "23": (IntegrityError, "integrity constraint violation"),
    "28": (PermissionDeniedError, "authorization error"),
    "40": (TransactionError, "transaction error"),
    "42": (SQLParsingError, "SQL syntax error"),
    "08": (DatabaseConnectionError, "connection error"),
    "22": (DataError, "data error"),
}
_MYSQL_CONSTRAINT_ERROR_DISPATCH: dict[int, tuple[type[SQLSpecError], str]] = {
    MYSQL_ER_DUP_ENTRY: (UniqueViolationError, "unique constraint violation"),
    1216: (ForeignKeyViolationError, "foreign key constraint violation"),
    1217: (ForeignKeyViolationError, "foreign key constraint violation"),
    1451: (ForeignKeyViolationError, "foreign key constraint violation"),
    1452: (ForeignKeyViolationError, "foreign key constraint violation"),
    1048: (NotNullViolationError, "not-null constraint violation"),
    MYSQL_ER_NO_DEFAULT_FOR_FIELD: (NotNullViolationError, "not-null constraint violation"),
    MYSQL_ER_CHECK_CONSTRAINT_VIOLATED: (CheckViolationError, "check constraint violation"),
}
_MYSQL_ACCESS_ERROR_DISPATCH: dict[int, tuple[type[SQLSpecError], str]] = {
    MYSQL_ER_DBACCESS_DENIED: (PermissionDeniedError, "access denied"),
    MYSQL_ER_ACCESS_DENIED: (PermissionDeniedError, "access denied"),
    MYSQL_ER_TABLEACCESS_DENIED: (PermissionDeniedError, "access denied"),
}
_MYSQL_TRANSACTION_ERROR_DISPATCH: dict[int, tuple[type[SQLSpecError], str]] = {
    MYSQL_ER_LOCK_DEADLOCK: (DeadlockError, "deadlock detected"),
    MYSQL_ER_LOCK_WAIT_TIMEOUT: (QueryTimeoutError, "lock wait timeout"),
}
_MYSQL_CONNECTION_ERROR_DISPATCH: dict[int, tuple[type[SQLSpecError], str]] = {
    MYSQL_CR_SERVER_LOST: (ConnectionTimeoutError, "connection lost"),
    MYSQL_CR_CONNECTION_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_CONN_HOST_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_UNKNOWN_HOST: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_SERVER_GONE_ERROR: (DatabaseConnectionError, "connection error"),
}


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_mysql_identifier(identifier: str) -> str:
    normalized = identifier.replace("`", "``")
    return f"`{normalized}`"


def format_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = [part for part in cleaned.split(".") if part]
    formatted = ".".join(_quote_mysql_identifier(part) for part in parts)
    return formatted or _quote_mysql_identifier(cleaned)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_mysql_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for AsyncMy execute calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    return parameters or None


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for AsyncMy executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.

    Raises:
        ValueError: When parameters are missing for executemany.
    """
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    return parameters


def build_profile() -> "DriverParameterProfile":
    """Create the AsyncMy driver parameter profile."""
    coercions: dict[type, Callable[[Any], Any]] = {bool: _bool_to_int, **build_uuid_coercions()}
    return DriverParameterProfile(
        name="AsyncMy",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=True,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=coercions,
        default_dialect="mysql",
    )


driver_profile = build_profile()


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AsyncMy statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "mysql"}, json_serializer=serializer, json_deserializer=deserializer
    )


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply AsyncMy driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def _create_mysql_error(
    error: Any, sqlstate: "str | None", code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a MySQL error instance without raising it."""
    code_str = f"[{sqlstate or code}]" if sqlstate or code else ""
    msg = f"MySQL {description} {code_str}: {error}" if code_str else f"MySQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> "SQLSpecError | bool":
    """Map AsyncMy exceptions to SQLSpec errors.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
    1. Specific error codes (most reliable for MySQL)
    2. SQLSTATE codes (where available)
    3. Generic error code ranges
    4. Default SQLSpecError fallback

    Args:
        error: The AsyncMy exception to map
        logger: Optional logger for migration warnings

    Returns:
        True to suppress expected migration errors, or a SQLSpec exception
    """
    error_args = getattr(error, "args", ())
    error_code = error_args[0] if error_args and isinstance(error_args[0], int) else None
    sqlstate_attr = getattr(error, "sqlstate", None)
    sqlstate = sqlstate_attr if isinstance(sqlstate_attr, str) else None
    sqlstate_prefix = sqlstate[:2] if isinstance(sqlstate, str) and sqlstate else None

    # Migration-specific errors to suppress
    if error_code in _MYSQL_MIGRATION_ERROR_CODES:
        if logger is not None:
            logger.warning("AsyncMy MySQL expected migration error (ignoring): %s", error)
        return True

    dispatch = _MYSQL_SQLSTATE_EXACT_DISPATCH.get(sqlstate) if sqlstate is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    dispatch = _MYSQL_CONSTRAINT_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    if sqlstate_prefix == "23":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["23"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    dispatch = _MYSQL_ACCESS_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])
    if sqlstate_prefix == "28":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["28"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    dispatch = _MYSQL_TRANSACTION_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])
    if sqlstate_prefix == "40":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["40"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    if sqlstate_prefix == "42":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["42"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])
    if isinstance(error_code, int) and MYSQL_SYNTAX_ERROR_MIN <= error_code < MYSQL_SYNTAX_ERROR_MAX_EXCLUSIVE:
        return _create_mysql_error(error, sqlstate, error_code, SQLParsingError, "SQL syntax error")

    if sqlstate_prefix == "08":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["08"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])
    dispatch = _MYSQL_CONNECTION_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    if sqlstate_prefix == "22":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["22"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    return _create_mysql_error(error, sqlstate, error_code, SQLSpecError, "database error")


def resolve_column_names(description: "Sequence[Any] | None") -> "list[str]":
    """Resolve ordered column names from cursor metadata."""
    if not description:
        return []
    return [desc[0] for desc in description]


def detect_json_columns_from_description(
    description: "Sequence[Any] | None", json_type_codes: "set[int]"
) -> "list[int]":
    """Identify JSON column indexes from pre-fetched cursor description metadata."""
    if not description or not json_type_codes:
        return []

    json_indexes: list[int] = []
    append = json_indexes.append
    for index, column in enumerate(description):
        if isinstance(column, (tuple, list)):
            type_code = column[1] if len(column) > 1 else None
        else:
            type_code = getattr(column, "type_code", None)
        if type_code in json_type_codes:
            append(index)
    return json_indexes


def detect_json_columns(
    cursor: Any, json_type_codes: "set[int]", description: "Sequence[Any] | None" = None
) -> "list[int]":
    """Identify JSON column indexes from cursor metadata.

    Args:
        cursor: Database cursor with description metadata available.
        json_type_codes: Set of type codes identifying JSON columns.
        description: Optional pre-fetched cursor description metadata.

    Returns:
        List of index positions where JSON values are present.
    """
    if description is None:
        if not has_cursor_metadata(cursor):
            return []
        description = cursor.description
    return detect_json_columns_from_description(description, json_type_codes)


def _deserialize_asyncmy_json_dict_rows(
    column_names: "list[str]",
    rows: "list[dict[str, Any]]",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[dict[str, Any]]":
    """Apply JSON deserialization to dict rows (DictCursor path).

    Args:
        column_names: Ordered column names from the cursor description.
        rows: Result rows represented as dictionaries.
        json_indexes: Column indexes to deserialize.
        deserializer: Callable used to decode JSON values.
        logger: Optional logger for debug output.

    Returns:
        Rows with JSON columns decoded when possible.
    """
    if not rows or not column_names or not json_indexes:
        return rows

    target_columns = [column_names[index] for index in json_indexes if index < len(column_names)]
    if not target_columns:
        return rows

    for row in rows:
        for column in target_columns:
            if column not in row:
                continue
            raw_value = row[column]
            if raw_value is None:
                continue
            if isinstance(raw_value, bytearray):
                raw_value = bytes(raw_value)
            if not isinstance(raw_value, (str, bytes)):
                continue
            try:
                row[column] = deserializer(raw_value)
            except Exception:
                if logger is not None:
                    logger.debug("Failed to deserialize JSON column %s", column, exc_info=True)
    return rows


def _deserialize_asyncmy_json_tuple_rows(
    rows: "list[Any]", json_indexes: "list[int]", deserializer: "Callable[[Any], Any]", *, logger: Any | None = None
) -> "list[Any]":
    """Apply JSON deserialization to tuple rows using index-based access.

    Args:
        rows: Result rows as tuples.
        json_indexes: Column indexes to deserialize.
        deserializer: Callable used to decode JSON values.
        logger: Optional logger for debug output.

    Returns:
        Rows with JSON columns decoded when possible.
    """
    if not rows or not json_indexes:
        return rows

    result: list[Any] = []
    for row in rows:
        row_list = list(row)
        mutated = False
        for idx in json_indexes:
            if idx >= len(row_list):
                continue
            raw_value = row_list[idx]
            if raw_value is None:
                continue
            if isinstance(raw_value, bytearray):
                raw_value = bytes(raw_value)
            if not isinstance(raw_value, (str, bytes)):
                continue
            try:
                row_list[idx] = deserializer(raw_value)
                mutated = True
            except Exception:
                if logger is not None:
                    logger.debug("Failed to deserialize JSON column index %d", idx, exc_info=True)
        result.append(tuple(row_list) if mutated else row)
    return result


def collect_rows(
    fetched_data: "Sequence[Any] | None",
    description: "Sequence[Any] | None",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    column_names: "list[str] | None" = None,
    logger: Any | None = None,
) -> "tuple[list[Any], list[str], str]":
    """Collect AsyncMy rows with JSON decoding, preserving raw format.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        json_indexes: Column indexes containing JSON values.
        deserializer: JSON deserializer function.
        column_names: Optional precomputed ordered column names.
        logger: Optional logger for debug output.

    Returns:
        Tuple of (rows, column_names, row_format).
    """
    if not description:
        return [], [], "tuple"
    resolved_column_names = resolve_column_names(description) if column_names is None else column_names
    if not fetched_data:
        return [], resolved_column_names, "tuple"

    first_row = fetched_data[0]
    if isinstance(first_row, dict):
        if not json_indexes:
            rows = fetched_data if isinstance(fetched_data, list) else list(fetched_data)
            return rows, resolved_column_names, "dict"
        rows = [dict(row) for row in fetched_data]
        rows = _deserialize_asyncmy_json_dict_rows(
            resolved_column_names, rows, json_indexes, deserializer, logger=logger
        )
        return rows, resolved_column_names, "dict"
    rows = fetched_data if isinstance(fetched_data, list) else list(fetched_data)
    if json_indexes:
        rows = _deserialize_asyncmy_json_tuple_rows(rows, json_indexes, deserializer, logger=logger)
    return rows, resolved_column_names, "tuple"


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from an AsyncMy cursor.

    Args:
        cursor: AsyncMy cursor with optional rowcount metadata.

    Returns:
        Rowcount value or 0 when unknown.
    """
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount >= 0:
        return rowcount
    return 0


def resolve_many_rowcount(cursor: Any, parameters: Any, *, fallback_count: "int | None" = None) -> int:
    """Resolve execute_many rowcount using cursor metadata with payload fallback."""
    rowcount = resolve_rowcount(cursor)
    if rowcount > 0:
        return rowcount
    if fallback_count is not None:
        return fallback_count
    if isinstance(parameters, Sized):
        return len(parameters)
    return 0


def normalize_lastrowid(cursor: Any) -> int | None:
    """Normalize lastrowid for AsyncMy when rowcount indicates success.

    Args:
        cursor: AsyncMy cursor with optional lastrowid metadata.

    Returns:
        Last inserted id or None when unavailable.
    """
    if not has_rowcount(cursor):
        return None
    rowcount = cursor.rowcount
    if not isinstance(rowcount, int) or rowcount <= 0:
        return None
    if not has_lastrowid(cursor):
        return None
    last_id = cursor.lastrowid
    return last_id if isinstance(last_id, int) else None
