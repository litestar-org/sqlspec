"""AsyncMy adapter compiled helpers."""

from typing import TYPE_CHECKING, Any

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    SQLParsingError,
    SQLSpecError,
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_guards import has_cursor_metadata, has_lastrowid, has_rowcount, has_sqlstate, has_type_code

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

__all__ = (
    "apply_asyncmy_driver_features",
    "asyncmy_statement_config",
    "build_asyncmy_insert_statement",
    "build_asyncmy_profile",
    "build_asyncmy_statement_config",
    "collect_asyncmy_rows",
    "detect_asyncmy_json_columns",
    "format_mysql_identifier",
    "map_asyncmy_exception",
    "normalize_asyncmy_lastrowid",
    "normalize_asyncmy_rowcount",
)

MYSQL_ER_DUP_ENTRY = 1062
MYSQL_ER_NO_DEFAULT_FOR_FIELD = 1364
MYSQL_ER_CHECK_CONSTRAINT_VIOLATED = 3819


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_mysql_identifier(identifier: str) -> str:
    normalized = identifier.replace("`", "``")
    return f"`{normalized}`"


def format_mysql_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = [part for part in cleaned.split(".") if part]
    formatted = ".".join(_quote_mysql_identifier(part) for part in parts)
    return formatted or _quote_mysql_identifier(cleaned)


def build_asyncmy_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_mysql_identifier(column) for column in columns)
    placeholders = ", ".join("%s" for _ in columns)
    return f"INSERT INTO {format_mysql_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def build_asyncmy_profile() -> "DriverParameterProfile":
    """Create the AsyncMy driver parameter profile."""

    return DriverParameterProfile(
        name="AsyncMy",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=True,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={bool: _bool_to_int},
        default_dialect="mysql",
    )


def build_asyncmy_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AsyncMy statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = build_asyncmy_profile()
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "mysql"}, json_serializer=serializer, json_deserializer=deserializer
    )


asyncmy_statement_config = build_asyncmy_statement_config()


def apply_asyncmy_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply AsyncMy driver feature defaults to statement config."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = processed_driver_features.setdefault("json_serializer", to_json)
    json_deserializer = processed_driver_features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_driver_features


def _raise_mysql_error(
    error: Any, sqlstate: "str | None", code: "int | None", error_class: type[SQLSpecError], description: str
) -> None:
    code_str = f"[{sqlstate or code}]" if sqlstate or code else ""
    msg = f"MySQL {description} {code_str}: {error}" if code_str else f"MySQL {description}: {error}"
    raise error_class(msg) from error


def map_asyncmy_exception(error: Any, *, logger: Any | None = None) -> "bool | None":
    """Map AsyncMy exceptions to SQLSpec errors.

    Returns True to suppress expected migration errors.
    """
    error_code = error.args[0] if len(error.args) >= 1 and isinstance(error.args[0], int) else None
    sqlstate = error.sqlstate if has_sqlstate(error) and error.sqlstate is not None else None

    if error_code in {1061, 1091}:
        if logger is not None:
            logger.warning("AsyncMy MySQL expected migration error (ignoring): %s", error)
        return True

    if sqlstate == "23505" or error_code == MYSQL_ER_DUP_ENTRY:
        _raise_mysql_error(error, sqlstate, error_code, UniqueViolationError, "unique constraint violation")
    elif sqlstate == "23503" or error_code in {1216, 1217, 1451, 1452}:
        _raise_mysql_error(error, sqlstate, error_code, ForeignKeyViolationError, "foreign key constraint violation")
    elif sqlstate == "23502" or error_code in {1048, MYSQL_ER_NO_DEFAULT_FOR_FIELD}:
        _raise_mysql_error(error, sqlstate, error_code, NotNullViolationError, "not-null constraint violation")
    elif sqlstate == "23514" or error_code == MYSQL_ER_CHECK_CONSTRAINT_VIOLATED:
        _raise_mysql_error(error, sqlstate, error_code, CheckViolationError, "check constraint violation")
    elif sqlstate and sqlstate.startswith("23"):
        _raise_mysql_error(error, sqlstate, error_code, IntegrityError, "integrity constraint violation")
    elif sqlstate and sqlstate.startswith("42"):
        _raise_mysql_error(error, sqlstate, error_code, SQLParsingError, "SQL syntax error")
    elif sqlstate and sqlstate.startswith("08"):
        _raise_mysql_error(error, sqlstate, error_code, DatabaseConnectionError, "connection error")
    elif sqlstate and sqlstate.startswith("40"):
        _raise_mysql_error(error, sqlstate, error_code, TransactionError, "transaction error")
    elif sqlstate and sqlstate.startswith("22"):
        _raise_mysql_error(error, sqlstate, error_code, DataError, "data error")
    elif error_code in {2002, 2003, 2005, 2006, 2013}:
        _raise_mysql_error(error, sqlstate, error_code, DatabaseConnectionError, "connection error")
    elif error_code in {1205, 1213}:
        _raise_mysql_error(error, sqlstate, error_code, TransactionError, "transaction error")
    elif error_code in range(1064, 1100):
        _raise_mysql_error(error, sqlstate, error_code, SQLParsingError, "SQL syntax error")
    else:
        _raise_mysql_error(error, sqlstate, error_code, SQLSpecError, "database error")
    return None


def detect_asyncmy_json_columns(cursor: Any, json_type_codes: "set[int]") -> "list[int]":
    """Identify JSON column indexes from cursor metadata.

    Args:
        cursor: Database cursor with description metadata available.
        json_type_codes: Set of type codes identifying JSON columns.

    Returns:
        List of index positions where JSON values are present.
    """
    if not has_cursor_metadata(cursor):
        return []
    description = cursor.description
    if not description or not json_type_codes:
        return []

    json_indexes: list[int] = []
    for index, column in enumerate(description):
        if has_type_code(column):
            type_code = column.type_code
        elif isinstance(column, (tuple, list)) and len(column) > 1:
            type_code = column[1]
        else:
            type_code = None
        if type_code in json_type_codes:
            json_indexes.append(index)
    return json_indexes


def _deserialize_asyncmy_json_rows(
    column_names: "list[str]",
    rows: "list[dict[str, Any]]",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[dict[str, Any]]":
    """Apply JSON deserialization to selected columns.

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


def collect_asyncmy_rows(
    fetched_data: "Sequence[Any] | None",
    description: "Sequence[Any] | None",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect AsyncMy rows into dictionaries with JSON decoding.

    Args:
        fetched_data: Rows returned from cursor.fetchall().
        description: Cursor description metadata.
        json_indexes: Column indexes containing JSON values.
        deserializer: JSON deserializer function.
        logger: Optional logger for debug output.

    Returns:
        Tuple of (rows, column_names).
    """
    if not description:
        return [], []
    column_names = [desc[0] for desc in description]
    if not fetched_data:
        return [], column_names
    if not isinstance(fetched_data[0], dict):
        rows = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
    else:
        rows = [dict(row) for row in fetched_data]
    rows = _deserialize_asyncmy_json_rows(column_names, rows, json_indexes, deserializer, logger=logger)
    return rows, column_names


def normalize_asyncmy_rowcount(cursor: Any) -> int:
    """Normalize rowcount from an AsyncMy cursor.

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


def normalize_asyncmy_lastrowid(cursor: Any) -> int | None:
    """Normalize lastrowid for AsyncMy when rowcount indicates success.

    Args:
        cursor: AsyncMy cursor with optional lastrowid metadata.

    Returns:
        Last inserted id or None when unavailable.
    """
    if not has_rowcount(cursor) or not has_lastrowid(cursor):
        return None
    rowcount = cursor.rowcount
    if not isinstance(rowcount, int) or rowcount <= 0:
        return None
    last_id = cursor.lastrowid
    return last_id if isinstance(last_id, int) else None
