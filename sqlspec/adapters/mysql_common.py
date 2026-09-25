"""Shared MySQL-family adapter helpers, constants, and error mappings."""

from collections.abc import Callable, Sequence, Sized
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlspec.driver import rows_to_dicts
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
from sqlspec.utils.text import quote_backtick_identifier, split_qualified_identifier
from sqlspec.utils.type_guards import has_cursor_metadata, has_lastrowid, has_rowcount, has_sqlstate, has_type_code

if TYPE_CHECKING:
    from sqlspec.core.parameters import ParameterValidator

__all__ = (
    "MYSQL_CR_CONNECTION_ERROR",
    "MYSQL_CR_CONN_HOST_ERROR",
    "MYSQL_CR_SERVER_GONE_ERROR",
    "MYSQL_CR_SERVER_LOST",
    "MYSQL_CR_SSL_CONNECTION_ERROR",
    "MYSQL_CR_UNKNOWN_HOST",
    "MYSQL_ER_ACCESS_DENIED",
    "MYSQL_ER_CHECK_CONSTRAINT_VIOLATED",
    "MYSQL_ER_DBACCESS_DENIED",
    "MYSQL_ER_DUP_ENTRY",
    "MYSQL_ER_LOCK_DEADLOCK",
    "MYSQL_ER_LOCK_WAIT_TIMEOUT",
    "MYSQL_ER_NO_DEFAULT_FOR_FIELD",
    "MYSQL_ER_TABLEACCESS_DENIED",
    "MYSQL_SYNTAX_ERROR_MAX_EXCLUSIVE",
    "MYSQL_SYNTAX_ERROR_MIN",
    "_MYSQL_ACCESS_ERROR_DISPATCH",
    "_MYSQL_CONNECTION_ERROR_DISPATCH",
    "_MYSQL_CONSTRAINT_ERROR_DISPATCH",
    "_MYSQL_MIGRATION_ERROR_CODES",
    "_MYSQL_SQLSTATE_EXACT_DISPATCH",
    "_MYSQL_SQLSTATE_PREFIX_DISPATCH",
    "_MYSQL_TRANSACTION_ERROR_DISPATCH",
    "_bool_to_int",
    "_create_mysql_error",
    "_deserialize_json_dict_rows",
    "_deserialize_json_tuple_rows",
    "_deserialize_json_value",
    "build_insert_statement",
    "build_load_data_statement",
    "collect_rows",
    "collect_stream_rows",
    "create_mapped_exception",
    "detect_json_columns",
    "detect_json_columns_from_description",
    "encode_records_for_local_infile",
    "escape_literal_percent",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "normalize_lastrowid",
    "resolve_column_names",
    "resolve_many_rowcount",
    "resolve_row_plan",
    "resolve_rowcount",
)

MYSQL_ER_DUP_ENTRY: Final[int] = 1062
MYSQL_ER_NO_DEFAULT_FOR_FIELD: Final[int] = 1364
MYSQL_ER_CHECK_CONSTRAINT_VIOLATED: Final[int] = 3819
MYSQL_ER_DBACCESS_DENIED: Final[int] = 1044
MYSQL_ER_ACCESS_DENIED: Final[int] = 1045
MYSQL_ER_TABLEACCESS_DENIED: Final[int] = 1142
MYSQL_ER_LOCK_DEADLOCK: Final[int] = 1213
MYSQL_ER_LOCK_WAIT_TIMEOUT: Final[int] = 1205

MYSQL_CR_CONNECTION_ERROR: Final[int] = 2002
MYSQL_CR_CONN_HOST_ERROR: Final[int] = 2003
MYSQL_CR_UNKNOWN_HOST: Final[int] = 2005
MYSQL_CR_SERVER_GONE_ERROR: Final[int] = 2006
MYSQL_CR_SERVER_LOST: Final[int] = 2013
MYSQL_CR_SSL_CONNECTION_ERROR: Final[int] = 2026
MYSQL_SYNTAX_ERROR_MIN: Final[int] = 1064
MYSQL_SYNTAX_ERROR_MAX_EXCLUSIVE: Final[int] = 1100

_MYSQL_MIGRATION_ERROR_CODES: Final[frozenset[int]] = frozenset((1061, 1091))
_MYSQL_SQLSTATE_EXACT_DISPATCH: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "23505": (UniqueViolationError, "unique constraint violation"),
    "23503": (ForeignKeyViolationError, "foreign key constraint violation"),
    "23502": (NotNullViolationError, "not-null constraint violation"),
    "23514": (CheckViolationError, "check constraint violation"),
}
_MYSQL_SQLSTATE_PREFIX_DISPATCH: Final[dict[str, tuple[type[SQLSpecError], str]]] = {
    "23": (IntegrityError, "integrity constraint violation"),
    "28": (PermissionDeniedError, "authorization error"),
    "40": (TransactionError, "transaction error"),
    "42": (SQLParsingError, "SQL syntax error"),
    "08": (DatabaseConnectionError, "connection error"),
    "22": (DataError, "data error"),
}
_MYSQL_CONSTRAINT_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_ER_DUP_ENTRY: (UniqueViolationError, "unique constraint violation"),
    1216: (ForeignKeyViolationError, "foreign key constraint violation"),
    1217: (ForeignKeyViolationError, "foreign key constraint violation"),
    1451: (ForeignKeyViolationError, "foreign key constraint violation"),
    1452: (ForeignKeyViolationError, "foreign key constraint violation"),
    1048: (NotNullViolationError, "not-null constraint violation"),
    MYSQL_ER_NO_DEFAULT_FOR_FIELD: (NotNullViolationError, "not-null constraint violation"),
    MYSQL_ER_CHECK_CONSTRAINT_VIOLATED: (CheckViolationError, "check constraint violation"),
}
_MYSQL_ACCESS_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_ER_DBACCESS_DENIED: (PermissionDeniedError, "access denied"),
    MYSQL_ER_ACCESS_DENIED: (PermissionDeniedError, "access denied"),
    MYSQL_ER_TABLEACCESS_DENIED: (PermissionDeniedError, "access denied"),
}
_MYSQL_TRANSACTION_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_ER_LOCK_DEADLOCK: (DeadlockError, "deadlock detected"),
    MYSQL_ER_LOCK_WAIT_TIMEOUT: (QueryTimeoutError, "lock wait timeout"),
}
_MYSQL_CONNECTION_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_CR_SERVER_LOST: (ConnectionTimeoutError, "connection lost"),
    MYSQL_CR_CONNECTION_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_CONN_HOST_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_UNKNOWN_HOST: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_SERVER_GONE_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_SSL_CONNECTION_ERROR: (DatabaseConnectionError, "ssl connection error"),
}


def format_identifier(identifier: str) -> str:
    """Format an SQL identifier with MySQL backtick quoting."""
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = split_qualified_identifier(cleaned, quote_chars="`", allow_bracket_quotes=False)
    formatted = ".".join(quote_backtick_identifier(part) for part in parts)
    return formatted or quote_backtick_identifier(cleaned)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    """Construct an INSERT statement with positional placeholders."""
    column_clause = ", ".join(quote_backtick_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def encode_records_for_local_infile(records: "list[tuple[Any, ...]]") -> bytes:
    """Encode row tuples into MySQL LOCAL INFILE TSV byte payload."""
    lines: list[str] = []
    for record in records:
        fields: list[str] = []
        for value in record:
            if value is None:
                fields.append("\\N")
                continue
            if isinstance(value, bool):
                value = int(value)
            text = value if isinstance(value, str) else str(value)
            text = text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
            fields.append(text)
        lines.append("\t".join(fields))
    return ("\n".join(lines) + "\n").encode("utf-8")


def build_load_data_statement(
    table: str, columns: "list[str]", placeholder: str = "%s", *, escape_percent: bool = False
) -> str:
    """Build native LOAD DATA SQL with a bound filename.

    Args:
        table: Destination table identifier.
        columns: Destination column names.
        placeholder: Filename placeholder parameter style.
        escape_percent: Whether to escape literal percent characters in identifiers.

    Returns:
        SQL statement for LOCAL INFILE bulk load.
    """
    table_sql = format_identifier(table)
    if escape_percent:
        table_sql = table_sql.replace("%", "%%")
    column_list = ", ".join(
        format_identifier(column).replace("%", "%%") if escape_percent else format_identifier(column)
        for column in columns
    )
    return (
        f"LOAD DATA LOCAL INFILE {placeholder} INTO TABLE {table_sql} "
        "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "
        f"LINES TERMINATED BY '\\n' ({column_list})"
    )


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for MySQL execute calls."""
    return parameters or None


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for MySQL executemany calls."""
    return parameters


def resolve_column_names(description: "Sequence[Any] | None") -> "list[str]":
    """Extract column names from cursor description."""
    if not description:
        return []
    return [col[0] for col in description]


def resolve_row_plan(
    description: "Sequence[Any] | None", json_type_codes: "set[int]"
) -> "tuple[list[str], list[int] | None]":
    """Resolve ordered column names and JSON column indexes in one pass."""
    if not description:
        return [], None

    column_names: list[str] = []
    if not json_type_codes:
        column_names.extend(column[0] for column in description)
        return column_names, None

    json_indexes: list[int] = []
    append_json = json_indexes.append
    for index, column in enumerate(description):
        column_names.append(column[0])
        if isinstance(column, (tuple, list)):
            type_code = column[1] if len(column) > 1 else None
        else:
            type_code = column.type_code if has_type_code(column) else None
        if type_code in json_type_codes:
            append_json(index)
    return column_names, json_indexes or None


def detect_json_columns_from_description(
    description: "Sequence[Any] | None", json_type_codes: "set[int]"
) -> "list[int]":
    """Identify JSON column indexes from pre-fetched cursor description metadata."""
    return resolve_row_plan(description, json_type_codes)[1] or []


def detect_json_columns(
    cursor: Any, json_type_codes: "set[int]", description: "Sequence[Any] | None" = None
) -> "list[int]":
    """Identify JSON column indexes from cursor metadata."""
    if description is None:
        if not has_cursor_metadata(cursor):
            return []
        description = cursor.description
    return detect_json_columns_from_description(description, json_type_codes)


def _deserialize_json_value(value: Any, deserializer: "Callable[[Any], Any]", *, logger: Any | None = None) -> Any:
    """Safely decode JSON column values using the specified deserializer."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            return deserializer(bytes(value).decode("utf-8"))
        except Exception:
            return value
    if isinstance(value, str):
        try:
            return deserializer(value)
        except Exception:
            return value
    return value


def _deserialize_json_dict_rows(
    column_names: "Sequence[str]",
    rows: "Sequence[dict[str, Any]]",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[dict[str, Any]]":
    """Deserialize JSON fields in dictionary rows in-place."""
    if not json_indexes:
        return rows if isinstance(rows, list) else list(rows)
    json_columns = [column_names[idx] for idx in json_indexes if idx < len(column_names)]
    for row in rows:
        for column in json_columns:
            if column in row and row[column] is not None:
                row[column] = _deserialize_json_value(row[column], deserializer, logger=logger)
    return rows if isinstance(rows, list) else list(rows)


def _deserialize_json_tuple_rows(
    rows: "Sequence[tuple[Any, ...]]",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[tuple[Any, ...]]":
    """Deserialize JSON fields in tuple rows using direct tuple reconstruction."""
    if not json_indexes:
        return rows if isinstance(rows, list) else list(rows)
    indexes_set = frozenset(json_indexes)
    return [
        tuple(
            _deserialize_json_value(val, deserializer, logger=logger) if i in indexes_set and val is not None else val
            for i, val in enumerate(row)
        )
        for row in rows
    ]


def collect_stream_rows(
    rows: "Sequence[Any]",
    row_plan: "tuple[list[str], list[int] | None]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[dict[str, Any]]":
    """Convert streamed rows to dictionaries with JSON decoding applied."""
    column_names, json_indexes = row_plan
    if not column_names or not rows:
        return []
    dict_rows = rows if isinstance(rows[0], dict) else rows_to_dicts(list(rows), column_names)
    if json_indexes:
        dict_rows = _deserialize_json_dict_rows(column_names, dict_rows, json_indexes, deserializer, logger=logger)
    return dict_rows if isinstance(dict_rows, list) else list(dict_rows)


def collect_rows(
    fetched_data: "Sequence[Any] | None",
    row_plan: "tuple[list[str], list[int] | None]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "tuple[list[Any], list[str], Literal['dict', 'tuple', 'record']]":
    """Collect fetched rows with JSON decoding and zero-copy fast-path."""
    column_names, json_indexes = row_plan
    if not column_names:
        return [], [], "tuple"
    if not fetched_data:
        return [], column_names, "tuple"

    first_row = fetched_data[0]
    if isinstance(first_row, dict):
        rows = fetched_data if isinstance(fetched_data, list) else list(fetched_data)
        if json_indexes:
            rows = _deserialize_json_dict_rows(column_names, rows, json_indexes, deserializer, logger=logger)
        return rows, column_names, "dict"

    if not json_indexes:
        return fetched_data if isinstance(fetched_data, list) else list(fetched_data), column_names, "tuple"

    rows = fetched_data if isinstance(fetched_data, list) else list(fetched_data)
    rows = _deserialize_json_tuple_rows(rows, json_indexes, deserializer, logger=logger)
    return rows, column_names, "tuple"


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount safely from a cursor."""
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
    """Normalize lastrowid for MySQL when rowcount indicates success."""
    if not has_rowcount(cursor):
        return None
    rowcount = cursor.rowcount
    if not isinstance(rowcount, int) or rowcount <= 0:
        return None
    if not has_lastrowid(cursor):
        return None
    last_id = cursor.lastrowid
    return last_id if isinstance(last_id, int) else None


def _bool_to_int(value: bool) -> int:
    """Coerce boolean values to integer for MySQL parameter compatibility."""
    return int(value)


def _create_mysql_error(
    error: Any, sqlstate: str | None, code: int | None, error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a MySQL error instance without raising it."""
    code_str = f"[{sqlstate or code}]" if sqlstate or code else ""
    msg = f"MySQL {description} {code_str}: {error}" if code_str else f"MySQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> "SQLSpecError | bool":
    """Map MySQL driver exceptions to SQLSpec errors."""
    error_code = getattr(error, "errno", None)
    if error_code is None and isinstance(error, Exception) and error.args:
        value = error.args[0]
        if isinstance(value, int):
            error_code = value
    sqlstate = error.sqlstate if has_sqlstate(error) else None
    sqlstate_prefix = sqlstate[:2] if isinstance(sqlstate, str) and sqlstate else None

    if error_code in _MYSQL_MIGRATION_ERROR_CODES:
        if logger is not None:
            logger.warning("MySQL expected migration error (ignoring): %s", error)
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


def escape_literal_percent(sql: str, parameters: Any, validator: "ParameterValidator") -> str:
    """Escape literal percent characters before driver-side interpolation."""
    if not parameters or "%" not in sql:
        return sql
    keep = {info.position for info in validator.extract_parameters(sql)}
    return "".join("%%" if char == "%" and index not in keep else char for index, char in enumerate(sql))
