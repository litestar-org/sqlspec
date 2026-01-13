"""Mock adapter compiled helpers.

This module provides utility functions for the mock adapter, reusing
SQLite-compatible helpers since mock uses SQLite as its execution backend.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.exceptions import (
    CheckViolationError,
    DatabaseConnectionError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.type_converters import build_decimal_converter, build_time_iso_converter
from sqlspec.utils.type_guards import has_rowcount, has_sqlite_error

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
    "driver_profile",
    "format_identifier",
    "normalize_execute_many_parameters",
    "normalize_execute_parameters",
    "resolve_rowcount",
)

SQLITE_CONSTRAINT_UNIQUE_CODE = 2067
SQLITE_CONSTRAINT_FOREIGNKEY_CODE = 787
SQLITE_CONSTRAINT_NOTNULL_CODE = 1811
SQLITE_CONSTRAINT_CHECK_CODE = 531
SQLITE_CONSTRAINT_CODE = 19
SQLITE_CANTOPEN_CODE = 14
SQLITE_IOERR_CODE = 10
SQLITE_MISMATCH_CODE = 20


_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_sqlite_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def format_identifier(identifier: str) -> str:
    """Format an identifier for SQLite.

    Args:
        identifier: Table or column name to format.

    Returns:
        Properly quoted identifier.

    Raises:
        SQLSpecError: If identifier is empty.
    """
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)

    if "." not in cleaned:
        return _quote_sqlite_identifier(cleaned)

    return ".".join(_quote_sqlite_identifier(part) for part in cleaned.split(".") if part)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    """Build an INSERT statement for the given table and columns.

    Args:
        table: Table name.
        columns: List of column names.

    Returns:
        INSERT SQL statement.
    """
    column_clause = ", ".join(_quote_sqlite_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def collect_rows(
    fetched_data: "list[Any]", description: "Sequence[Any] | None"
) -> "tuple[list[dict[str, Any]], list[str], int]":
    """Collect SQLite result rows into dictionaries.

    Args:
        fetched_data: Raw rows from cursor.fetchall()
        description: Cursor description (tuple of tuples)

    Returns:
        Tuple of (data, column_names, row_count)
    """
    if not description:
        return [], [], 0

    column_names = [col[0] for col in description]
    data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
    return data, column_names, len(data)


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a SQLite cursor.

    Args:
        cursor: SQLite cursor with optional rowcount metadata.

    Returns:
        Positive rowcount value or 0 when unknown.
    """
    if not has_rowcount(cursor):
        return 0
    rowcount = cursor.rowcount
    if isinstance(rowcount, int) and rowcount > 0:
        return rowcount
    return 0


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for SQLite execute calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    return parameters or ()


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for SQLite executemany calls.

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


def _create_sqlite_error(
    error: Any, code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLite error instance without raising it."""
    code_str = f"[code {code}]" if code else ""
    msg = f"SQLite {description} {code_str}: {error}" if code_str else f"SQLite {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = cast("BaseException", error)
    return exc


def create_mapped_exception(error: BaseException) -> SQLSpecError:
    """Map SQLite errors to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Args:
        error: The SQLite exception to map

    Returns:
        A SQLSpec exception that wraps the original error
    """
    if has_sqlite_error(error):
        error_code = error.sqlite_errorcode
        error_name = error.sqlite_errorname
    else:
        error_code = None
        error_name = None
    error_msg = str(error).lower()

    if "locked" in error_msg:
        return _create_sqlite_error(error, error_code or 0, OperationalError, "operational error")

    if not error_code:
        if "unique constraint" in error_msg:
            return _create_sqlite_error(error, 0, UniqueViolationError, "unique constraint violation")
        if "foreign key constraint" in error_msg:
            return _create_sqlite_error(error, 0, ForeignKeyViolationError, "foreign key constraint violation")
        if "not null constraint" in error_msg:
            return _create_sqlite_error(error, 0, NotNullViolationError, "not-null constraint violation")
        if "check constraint" in error_msg:
            return _create_sqlite_error(error, 0, CheckViolationError, "check constraint violation")
        if "syntax" in error_msg:
            return _create_sqlite_error(error, None, SQLParsingError, "SQL syntax error")
        return _create_sqlite_error(error, None, SQLSpecError, "database error")

    if error_code == SQLITE_CONSTRAINT_UNIQUE_CODE or error_name == "SQLITE_CONSTRAINT_UNIQUE":
        return _create_sqlite_error(error, error_code, UniqueViolationError, "unique constraint violation")
    if error_code == SQLITE_CONSTRAINT_FOREIGNKEY_CODE or error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
        return _create_sqlite_error(error, error_code, ForeignKeyViolationError, "foreign key constraint violation")
    if error_code == SQLITE_CONSTRAINT_NOTNULL_CODE or error_name == "SQLITE_CONSTRAINT_NOTNULL":
        return _create_sqlite_error(error, error_code, NotNullViolationError, "not-null constraint violation")
    if error_code == SQLITE_CONSTRAINT_CHECK_CODE or error_name == "SQLITE_CONSTRAINT_CHECK":
        return _create_sqlite_error(error, error_code, CheckViolationError, "check constraint violation")
    if error_code == SQLITE_CONSTRAINT_CODE or error_name == "SQLITE_CONSTRAINT":
        return _create_sqlite_error(error, error_code, IntegrityError, "integrity constraint violation")
    if error_code == SQLITE_CANTOPEN_CODE or error_name == "SQLITE_CANTOPEN":
        return _create_sqlite_error(error, error_code, DatabaseConnectionError, "connection error")
    if error_code == SQLITE_IOERR_CODE or error_name == "SQLITE_IOERR":
        return _create_sqlite_error(error, error_code, OperationalError, "operational error")
    if error_code == SQLITE_MISMATCH_CODE or error_name == "SQLITE_MISMATCH":
        return _create_sqlite_error(error, error_code, DataError, "data error")
    if error_code == 1 or "syntax" in error_msg:
        return _create_sqlite_error(error, error_code, SQLParsingError, "SQL syntax error")
    return _create_sqlite_error(error, error_code, SQLSpecError, "database error")


def build_profile() -> "DriverParameterProfile":
    """Create the Mock driver parameter profile.

    Returns:
        Driver parameter profile for Mock adapter.
    """
    return DriverParameterProfile(
        name="Mock",
        default_style=ParameterStyle.QMARK,
        supported_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        default_execution_style=ParameterStyle.QMARK,
        supported_execution_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        has_native_list_expansion=False,
        preserve_parameter_format=True,
        needs_static_script_compilation=False,
        allow_mixed_parameter_styles=False,
        preserve_original_params_for_many=False,
        json_serializer_strategy="helper",
        custom_type_coercions={
            bool: _bool_to_int,
            datetime: _TIME_TO_ISO,
            date: _TIME_TO_ISO,
            Decimal: _DECIMAL_TO_STRING,
        },
        default_dialect="sqlite",
    )


driver_profile = build_profile()


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the Mock statement configuration with optional JSON codecs.

    Args:
        json_serializer: Custom JSON serializer function.
        json_deserializer: Custom JSON deserializer function.

    Returns:
        StatementConfig for Mock adapter.
    """
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "sqlite"}, json_serializer=serializer, json_deserializer=deserializer
    )


default_statement_config = build_statement_config()


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply Mock driver feature defaults to statement config.

    Args:
        statement_config: Base statement configuration.
        driver_features: Driver feature overrides.

    Returns:
        Tuple of (updated statement config, driver features dict).
    """
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features
