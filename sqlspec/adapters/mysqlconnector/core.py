"""MysqlConnector adapter compiled helpers."""

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
    "apply_driver_features",
    "build_insert_statement",
    "build_profile",
    "build_statement_config",
    "collect_rows",
    "create_mapped_exception",
    "default_statement_config",
    "detect_json_columns",
    "driver_profile",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "normalize_lastrowid",
    "resolve_rowcount",
)

MYSQL_ER_DUP_ENTRY = 1062
MYSQL_ER_NO_DEFAULT_FOR_FIELD = 1364
MYSQL_ER_CHECK_CONSTRAINT_VIOLATED = 3819


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
    """Normalize parameters for mysql-connector execute calls."""
    return parameters or None


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for mysql-connector executemany calls."""
    if not parameters:
        msg = "execute_many requires parameters"
        raise ValueError(msg)
    return parameters


def build_profile() -> "DriverParameterProfile":
    """Create the mysql-connector driver parameter profile."""

    return DriverParameterProfile(
        name="mysql-connector",
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


driver_profile = build_profile()


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


default_statement_config = build_statement_config()


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
    """Map mysql-connector exceptions to SQLSpec errors.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The mysql-connector exception to map
        logger: Optional logger for migration warnings

    Returns:
        True to suppress expected migration errors, or a SQLSpec exception
    """
    error_code = getattr(error, "errno", None)
    if error_code is None and hasattr(error, "args") and error.args:
        value = error.args[0]
        if isinstance(value, int):
            error_code = value
    sqlstate = error.sqlstate if has_sqlstate(error) and error.sqlstate is not None else None

    if error_code in {1061, 1091}:
        if logger is not None:
            logger.warning("MysqlConnector MySQL expected migration error (ignoring): %s", error)
        return True

    if sqlstate == "23505" or error_code == MYSQL_ER_DUP_ENTRY:
        return _create_mysql_error(error, sqlstate, error_code, UniqueViolationError, "unique constraint violation")
    if sqlstate == "23503" or error_code in {1216, 1217, 1451, 1452}:
        return _create_mysql_error(
            error, sqlstate, error_code, ForeignKeyViolationError, "foreign key constraint violation"
        )
    if sqlstate == "23502" or error_code in {1048, MYSQL_ER_NO_DEFAULT_FOR_FIELD}:
        return _create_mysql_error(error, sqlstate, error_code, NotNullViolationError, "not-null constraint violation")
    if sqlstate == "23514" or error_code == MYSQL_ER_CHECK_CONSTRAINT_VIOLATED:
        return _create_mysql_error(error, sqlstate, error_code, CheckViolationError, "check constraint violation")
    if sqlstate and sqlstate.startswith("23"):
        return _create_mysql_error(error, sqlstate, error_code, IntegrityError, "integrity constraint violation")
    if sqlstate and sqlstate.startswith("42"):
        return _create_mysql_error(error, sqlstate, error_code, SQLParsingError, "SQL syntax error")
    if sqlstate and sqlstate.startswith("08"):
        return _create_mysql_error(error, sqlstate, error_code, DatabaseConnectionError, "connection error")
    if sqlstate and sqlstate.startswith("40"):
        return _create_mysql_error(error, sqlstate, error_code, TransactionError, "transaction error")
    if sqlstate and sqlstate.startswith("22"):
        return _create_mysql_error(error, sqlstate, error_code, DataError, "data error")
    if error_code in {2002, 2003, 2005, 2006, 2013}:
        return _create_mysql_error(error, sqlstate, error_code, DatabaseConnectionError, "connection error")
    if error_code in {1205, 1213}:
        return _create_mysql_error(error, sqlstate, error_code, TransactionError, "transaction error")
    if error_code in range(1064, 1100):
        return _create_mysql_error(error, sqlstate, error_code, SQLParsingError, "SQL syntax error")
    return _create_mysql_error(error, sqlstate, error_code, SQLSpecError, "database error")


def detect_json_columns(cursor: Any, json_type_codes: "set[int]") -> "list[int]":
    """Identify JSON column indexes from cursor metadata."""
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


def _deserialize_mysqlconnector_json_rows(
    column_names: "list[str]",
    rows: "list[dict[str, Any]]",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "list[dict[str, Any]]":
    """Apply JSON deserialization to selected columns."""
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


def collect_rows(
    fetched_data: "Sequence[Any] | None",
    description: "Sequence[Any] | None",
    json_indexes: "list[int]",
    deserializer: "Callable[[Any], Any]",
    *,
    logger: Any | None = None,
) -> "tuple[list[dict[str, Any]], list[str]]":
    """Collect mysql-connector rows into dictionaries with JSON decoding."""
    if not description:
        return [], []
    column_names = [desc[0] for desc in description]
    if not fetched_data:
        return [], column_names
    if not isinstance(fetched_data[0], dict):
        rows = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
    else:
        rows = [dict(row) for row in fetched_data]
    rows = _deserialize_mysqlconnector_json_rows(column_names, rows, json_indexes, deserializer, logger=logger)
    return rows, column_names


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a mysql-connector cursor."""
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount >= 0:
        return rowcount
    return 0


def normalize_lastrowid(cursor: Any) -> int | None:
    """Normalize lastrowid for mysql-connector when rowcount indicates success."""
    if not has_rowcount(cursor):
        return None
    rowcount = cursor.rowcount
    if not isinstance(rowcount, int) or rowcount <= 0:
        return None
    if not has_lastrowid(cursor):
        return None
    last_id = cursor.lastrowid
    return last_id if isinstance(last_id, int) else None
