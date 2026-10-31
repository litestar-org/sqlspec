"""MysqlConnector adapter compiled helpers."""

import contextlib
from collections.abc import Callable, Mapping, Sequence, Sized
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, Literal

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
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
    OperationalError,
    PermissionDeniedError,
    QueryTimeoutError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_backtick_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_uuid_coercions
from sqlspec.utils.type_guards import has_lastrowid, has_rowcount, has_sqlstate, has_type_code

if TYPE_CHECKING:
    from sqlspec.core.parameters import ParameterValidator

__all__ = (
    "MysqlConnectorAsyncStreamSource",
    "MysqlConnectorSyncStreamSource",
    "_resolve_column_types",
    "apply_driver_features",
    "build_insert_statement",
    "build_load_data_statement",
    "build_profile",
    "build_statement_config",
    "can_use_local_infile_records",
    "collect_rows",
    "collect_stream_rows",
    "create_mapped_exception",
    "default_statement_config",
    "driver_profile",
    "encode_records_for_local_infile",
    "escape_literal_percent",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "normalize_lastrowid",
    "resolve_many_rowcount",
    "resolve_row_plan",
    "resolve_rowcount",
)

logger = get_logger("sqlspec.adapters.mysqlconnector")

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

_MYSQL_DDL_ERROR_DISPATCH: dict[int, tuple[type[SQLSpecError], str]] = {
    1061: (OperationalError, "duplicate key name"),
    1091: (OperationalError, "cannot drop column or key"),
}
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
    1169: (UniqueViolationError, "unique constraint violation"),
    1586: (UniqueViolationError, "unique constraint violation"),
    1216: (ForeignKeyViolationError, "foreign key constraint violation"),
    1217: (ForeignKeyViolationError, "foreign key constraint violation"),
    1451: (ForeignKeyViolationError, "foreign key constraint violation"),
    1452: (ForeignKeyViolationError, "foreign key constraint violation"),
    1048: (NotNullViolationError, "not-null constraint violation"),
    MYSQL_ER_NO_DEFAULT_FOR_FIELD: (NotNullViolationError, "not-null constraint violation"),
    MYSQL_ER_CHECK_CONSTRAINT_VIOLATED: (CheckViolationError, "check constraint violation"),
    4025: (CheckViolationError, "check constraint violation"),
}
_MYSQL_ACCESS_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_ER_DBACCESS_DENIED: (PermissionDeniedError, "access denied"),
    MYSQL_ER_ACCESS_DENIED: (PermissionDeniedError, "access denied"),
    MYSQL_ER_TABLEACCESS_DENIED: (PermissionDeniedError, "access denied"),
}
_MYSQL_TRANSACTION_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_ER_LOCK_DEADLOCK: (DeadlockError, "deadlock detected"),
    MYSQL_ER_LOCK_WAIT_TIMEOUT: (QueryTimeoutError, "lock wait timeout"),
    1317: (QueryTimeoutError, "query execution interrupted"),
    1969: (QueryTimeoutError, "query execution timeout"),
    3024: (QueryTimeoutError, "query execution timeout"),
}
_MYSQL_CONNECTION_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    MYSQL_CR_SERVER_LOST: (ConnectionTimeoutError, "connection lost"),
    MYSQL_CR_CONNECTION_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_CONN_HOST_ERROR: (DatabaseConnectionError, "connection error"),
    2004: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_UNKNOWN_HOST: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_SERVER_GONE_ERROR: (DatabaseConnectionError, "connection error"),
    MYSQL_CR_SSL_CONNECTION_ERROR: (DatabaseConnectionError, "ssl connection error"),
    2055: (DatabaseConnectionError, "connection error"),
    4031: (DatabaseConnectionError, "connection error"),
}
_MYSQL_DATA_ERROR_DISPATCH: Final[dict[int, tuple[type[SQLSpecError], str]]] = {
    1264: (DataError, "data error"),
    1265: (DataError, "data error"),
    1292: (DataError, "data error"),
    1366: (DataError, "data error"),
    1406: (DataError, "data error"),
    1411: (DataError, "data error"),
    3140: (DataError, "data error"),
    3141: (DataError, "data error"),
    3142: (DataError, "data error"),
    3143: (DataError, "data error"),
}


class MysqlConnectorSyncStreamSource:
    """Compiled chunk source streaming dict rows from an unbuffered mysql-connector cursor."""

    __slots__ = (
        "_chunk_size",
        "_cursor",
        "_cursor_options",
        "_deserializer",
        "_driver",
        "_json_type_codes",
        "_parameters",
        "_row_plan",
        "_sql",
    )

    def __init__(
        self,
        driver: Any,
        sql: str,
        parameters: Any,
        chunk_size: int,
        json_type_codes: "set[int]",
        cursor_options: "dict[str, Any] | None" = None,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._json_type_codes = json_type_codes
        self._cursor_options = dict(cursor_options or {})
        self._cursor_options["buffered"] = False
        self._row_plan: tuple[list[str], list[int] | None] | None = None
        deserializer = getattr(driver, "_json_deserializer", None)
        if deserializer is None and hasattr(driver, "driver_features") and isinstance(driver.driver_features, dict):
            deserializer = driver.driver_features.get("json_deserializer")
        self._deserializer: Callable[[Any], Any] = deserializer or from_json

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    def _start(self) -> None:
        try:
            cursor = self._driver.connection.cursor(**self._cursor_options)
            self._cursor = cursor
            cursor.execute(self._sql, normalize_execute_parameters(self._parameters))
            self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)
        except BaseException:
            self.close(error=True)
            raise

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows = self._driver._run_with_exception_handler(handler, self._cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        if self._row_plan is None:
            self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)
        return collect_stream_rows(rows, self._row_plan, self._deserializer)

    def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        connection = self._driver.connection
        raw_connection = getattr(connection, "_cnx", None) or connection
        try:
            with contextlib.suppress(Exception):
                if getattr(raw_connection, "unread_result", False):
                    raw_connection.consume_results()
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    cursor.close()


class MysqlConnectorAsyncStreamSource:
    """Compiled async chunk source streaming dict rows from an unbuffered mysql-connector cursor."""

    __slots__ = (
        "_chunk_size",
        "_cursor",
        "_cursor_options",
        "_deserializer",
        "_driver",
        "_json_type_codes",
        "_parameters",
        "_row_plan",
        "_sql",
    )

    def __init__(
        self,
        driver: Any,
        sql: str,
        parameters: Any,
        chunk_size: int,
        json_type_codes: "set[int]",
        cursor_options: "dict[str, Any] | None" = None,
    ) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._json_type_codes = json_type_codes
        self._cursor_options = dict(cursor_options or {})
        self._cursor_options["buffered"] = False
        self._row_plan: tuple[list[str], list[int] | None] | None = None
        deserializer = getattr(driver, "_json_deserializer", None)
        if deserializer is None and hasattr(driver, "driver_features") and isinstance(driver.driver_features, dict):
            deserializer = driver.driver_features.get("json_deserializer")
        self._deserializer: Callable[[Any], Any] = deserializer or from_json

    async def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        await self._driver._run_with_exception_handler(handler, self._start)
        self._driver._check_pending_exception(handler)

    async def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows = await self._driver._run_with_exception_handler(handler, self._cursor.fetchmany, self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        if self._row_plan is None:
            self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)
        return collect_stream_rows(rows, self._row_plan, self._deserializer)

    async def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        connection = self._driver.connection
        raw_connection = getattr(connection, "_cnx", None) or connection
        try:
            with contextlib.suppress(Exception):
                if getattr(raw_connection, "unread_result", False):
                    await raw_connection.consume_results()
        finally:
            if cursor is not None:
                with contextlib.suppress(Exception):
                    await cursor.close()

    async def _start(self) -> None:
        try:
            cursor = await self._driver.connection.cursor(**self._cursor_options)
            self._cursor = cursor
            await cursor.execute(self._sql, normalize_execute_parameters(self._parameters))
            self._row_plan = resolve_row_plan(self._cursor.description, self._json_type_codes)
        except BaseException:
            await self.close(error=True)
            raise


def build_profile() -> "DriverParameterProfile":
    """Create the mysql-connector driver parameter profile."""
    coercions: dict[type, Callable[[Any], Any]] = {bool: int, **build_uuid_coercions()}
    return DriverParameterProfile(
        name="mysql-connector",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions=coercions,
        default_dialect="mysql",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the mysql-connector statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "mysql"}, json_serializer=serializer, json_deserializer=deserializer
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply mysql-connector driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


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


def can_use_local_infile_records(arrow_table: Any, records: "Sequence[Sequence[Any]]") -> bool:
    """Return whether Arrow records can be safely serialized via MySQL LOCAL INFILE TSV."""
    schema = getattr(arrow_table, "schema", None)
    if schema is not None:
        all_safe_types = True
        for field in schema:
            field_type = getattr(field, "type", None)
            type_str = str(field_type).lower() if field_type is not None else ""
            if "binary" in type_str or "duration" in type_str:
                return False
            if not type_str or not type_str.startswith((
                "int",
                "uint",
                "float",
                "double",
                "halffloat",
                "bool",
                "string",
                "large_string",
                "utf8",
                "large_utf8",
                "date",
                "timestamp",
                "time",
                "decimal",
            )):
                all_safe_types = False
        if all_safe_types:
            return True
    return not any(isinstance(value, (bytes, bytearray, memoryview, timedelta)) for row in records for value in row)


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
            if isinstance(value, (bytes, bytearray, memoryview)):
                text = bytes(value).decode("utf-8", errors="replace")
            elif isinstance(value, str):
                text = value
            else:
                text = str(value)
            text = (
                text
                .replace("\\", "\\\\")
                .replace("\x00", "\\0")
                .replace("\t", "\\t")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\x1a", "\\Z")
            )
            fields.append(text)
        lines.append("\t".join(fields))
    return ("\n".join(lines) + "\n").encode("utf-8")


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for MySQL execute calls."""
    return parameters or None


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for MySQL executemany calls."""
    return parameters


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


def create_mapped_exception(error: Any, *, logger: Any | None = None) -> SQLSpecError:
    """Map MySQL driver exceptions to SQLSpec errors."""
    _ = logger
    type_name = type(error).__name__
    if type_name == "PoolError" or "PoolError" in str(type(error)):
        return ConnectionTimeoutError(str(error))
    if type_name == "InterfaceError" or getattr(error, "errno", None) == -1:
        return DatabaseConnectionError(str(error))

    error_code = getattr(error, "errno", None)
    if error_code is None and isinstance(error, Exception) and error.args:
        value = error.args[0]
        if isinstance(value, int):
            error_code = value
    sqlstate = error.sqlstate if has_sqlstate(error) else None
    sqlstate_prefix = sqlstate[:2] if isinstance(sqlstate, str) and sqlstate else None

    dispatch = _MYSQL_DDL_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

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

    dispatch = _MYSQL_CONNECTION_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])
    if sqlstate_prefix == "08":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["08"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    dispatch = _MYSQL_DATA_ERROR_DISPATCH.get(error_code) if error_code is not None else None
    if dispatch is not None:
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])
    if sqlstate_prefix == "22" or type(error).__name__ == "DataError":
        dispatch = _MYSQL_SQLSTATE_PREFIX_DISPATCH["22"]
        return _create_mysql_error(error, sqlstate, error_code, dispatch[0], dispatch[1])

    return _create_mysql_error(error, sqlstate, error_code, SQLSpecError, "database error")


def escape_literal_percent(sql: str, parameters: Any, validator: "ParameterValidator") -> str:
    """Escape literal percent characters before driver-side interpolation."""
    if not parameters or "%" not in sql:
        return sql
    keep = {info.position for info in validator.extract_parameters(sql)}
    return "".join("%%" if char == "%" and index not in keep else char for index, char in enumerate(sql))


def build_load_data_statement(
    table: str, columns: "list[str]", placeholder: str = "%s", *, escape_percent: bool = False
) -> str:
    """Build native LOAD DATA SQL with a bound filename."""
    table_sql = format_identifier(table)
    if escape_percent:
        table_sql = table_sql.replace("%", "%%")
    column_list = ", ".join(
        quote_backtick_identifier(column).replace("%", "%%") if escape_percent else quote_backtick_identifier(column)
        for column in columns
    )
    return (
        f"LOAD DATA LOCAL INFILE {placeholder} INTO TABLE {table_sql} "
        "CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' "
        f"LINES TERMINATED BY '\\n' ({column_list})"
    )


def _deserialize_json_value(value: Any, deserializer: "Callable[[Any], Any]", *, logger: Any | None = None) -> Any:
    """Safely decode JSON column values using the specified deserializer."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            raw_bytes = bytes(value) if isinstance(value, (bytearray, memoryview)) else value
            return deserializer(raw_bytes)
        except Exception:
            if logger is not None:
                logger.debug("Failed to deserialize JSON column", exc_info=True)
            return value
    if isinstance(value, str):
        try:
            return deserializer(value)
        except Exception:
            if logger is not None:
                logger.debug("Failed to deserialize JSON column", exc_info=True)
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
    """Deserialize JSON fields in tuple rows using sparse index iteration and lazy row mutation."""
    if not json_indexes:
        return rows if isinstance(rows, list) else list(rows)
    result: list[tuple[Any, ...]] = []
    for row in rows:
        row_len = len(row)
        row_list: list[Any] | None = None
        for idx in json_indexes:
            if idx >= row_len:
                continue
            raw_val = row[idx]
            if raw_val is None:
                continue
            decoded = _deserialize_json_value(raw_val, deserializer, logger=logger)
            if decoded is not raw_val:
                if row_list is None:
                    row_list = list(row)
                row_list[idx] = decoded
        result.append(tuple(row_list) if row_list is not None else row)
    return result


def _create_mysql_error(
    error: Any, sqlstate: str | None, code: int | None, error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a MySQL error instance without raising it."""
    code_str = f"[{sqlstate or code}]" if sqlstate or code else ""
    msg = f"MySQL {description} {code_str}: {error}" if code_str else f"MySQL {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = error
    return exc


driver_profile = build_profile()

_MYSQL_TYPE_CODE_TOKENS: Final[dict[int, str]] = {
    0: "decimal",
    1: "int32",
    2: "int32",
    3: "int64",
    4: "float32",
    5: "float64",
    7: "timestamp",
    8: "int64",
    10: "date",
    11: "time",
    12: "timestamp",
    246: "decimal",
    252: "binary",
    253: "string",
    254: "string",
}


def _resolve_column_types(description: Any) -> "dict[str, str] | None":
    """Map MySQL cursor column FIELD_TYPE codes to neutral Arrow type tokens.

    Returns ``None`` when the cursor has no description or reports no
    recognizable type codes.
    """
    if not description:
        return None
    column_types: dict[str, str] = {}
    for col in description:
        token = _MYSQL_TYPE_CODE_TOKENS.get(col[1])
        if token is not None:
            column_types[col[0]] = token
    return column_types or None


default_statement_config = build_statement_config()
