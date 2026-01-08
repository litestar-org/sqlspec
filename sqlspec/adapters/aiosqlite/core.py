"""AIOSQLite adapter compiled helpers."""

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
from sqlspec.utils.type_guards import has_sqlite_error

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

__all__ = (
    "aiosqlite_statement_config",
    "apply_aiosqlite_driver_features",
    "build_aiosqlite_profile",
    "build_aiosqlite_statement_config",
    "build_sqlite_insert_statement",
    "format_sqlite_identifier",
    "process_sqlite_result",
    "raise_aiosqlite_exception",
)


_TIME_TO_ISO = build_time_iso_converter()
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")

SQLITE_CONSTRAINT_UNIQUE_CODE = 2067
SQLITE_CONSTRAINT_FOREIGNKEY_CODE = 787
SQLITE_CONSTRAINT_NOTNULL_CODE = 1811
SQLITE_CONSTRAINT_CHECK_CODE = 531
SQLITE_CONSTRAINT_CODE = 19
SQLITE_CANTOPEN_CODE = 14
SQLITE_IOERR_CODE = 10
SQLITE_MISMATCH_CODE = 20


def _bool_to_int(value: bool) -> int:
    return int(value)


def _quote_sqlite_identifier(identifier: str) -> str:
    normalized = identifier.replace('"', '""')
    return f'"{normalized}"'


def format_sqlite_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)
    parts = [part for part in cleaned.split(".") if part]
    formatted = ".".join(_quote_sqlite_identifier(part) for part in parts)
    return formatted or _quote_sqlite_identifier(cleaned)


def build_sqlite_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(_quote_sqlite_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_sqlite_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def process_sqlite_result(
    fetched_data: "Iterable[Any]", description: "Sequence[Any] | None"
) -> "tuple[list[dict[str, Any]], list[str], int]":
    """Process SQLite result rows into dictionaries.

    Optimized helper to convert raw rows and cursor description into list of dicts.

    Args:
        fetched_data: Raw rows from cursor.fetchall()
        description: Cursor description (tuple of tuples)

    Returns:
        Tuple of (data, column_names, row_count)
    """
    if not description:
        return [], [], 0

    column_names = [col[0] for col in description]
    # compiled list comp and zip is faster in mypyc
    data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
    return data, column_names, len(data)


def _raise_aiosqlite_error(error: Any, code: "int | None", error_class: type[SQLSpecError], description: str) -> None:
    code_str = f"[code {code}]" if code else ""
    msg = f"AIOSQLite {description} {code_str}: {error}" if code_str else f"AIOSQLite {description}: {error}"
    raise error_class(msg) from cast("BaseException", error)


def raise_aiosqlite_exception(error: BaseException) -> None:
    """Raise SQLSpec exceptions for aiosqlite errors."""
    if has_sqlite_error(error):
        error_code = error.sqlite_errorcode
        error_name = error.sqlite_errorname
        error_exc = cast("BaseException", error)
    else:
        error_code = None
        error_name = None
        error_exc = error
    error_msg = str(error).lower()

    if "locked" in error_msg:
        msg = f"AIOSQLite database locked: {error}. Consider enabling WAL mode or reducing concurrency."
        raise SQLSpecError(msg) from error_exc

    if not error_code:
        if "unique constraint" in error_msg:
            _raise_aiosqlite_error(error, 0, UniqueViolationError, "unique constraint violation")
        elif "foreign key constraint" in error_msg:
            _raise_aiosqlite_error(error, 0, ForeignKeyViolationError, "foreign key constraint violation")
        elif "not null constraint" in error_msg:
            _raise_aiosqlite_error(error, 0, NotNullViolationError, "not-null constraint violation")
        elif "check constraint" in error_msg:
            _raise_aiosqlite_error(error, 0, CheckViolationError, "check constraint violation")
        elif "syntax" in error_msg:
            _raise_aiosqlite_error(error, None, SQLParsingError, "SQL syntax error")
        else:
            _raise_aiosqlite_error(error, None, SQLSpecError, "database error")
        return

    if error_code == SQLITE_CONSTRAINT_UNIQUE_CODE or error_name == "SQLITE_CONSTRAINT_UNIQUE":
        _raise_aiosqlite_error(error, error_code, UniqueViolationError, "unique constraint violation")
    elif error_code == SQLITE_CONSTRAINT_FOREIGNKEY_CODE or error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
        _raise_aiosqlite_error(error, error_code, ForeignKeyViolationError, "foreign key constraint violation")
    elif error_code == SQLITE_CONSTRAINT_NOTNULL_CODE or error_name == "SQLITE_CONSTRAINT_NOTNULL":
        _raise_aiosqlite_error(error, error_code, NotNullViolationError, "not-null constraint violation")
    elif error_code == SQLITE_CONSTRAINT_CHECK_CODE or error_name == "SQLITE_CONSTRAINT_CHECK":
        _raise_aiosqlite_error(error, error_code, CheckViolationError, "check constraint violation")
    elif error_code == SQLITE_CONSTRAINT_CODE or error_name == "SQLITE_CONSTRAINT":
        _raise_aiosqlite_error(error, error_code, IntegrityError, "integrity constraint violation")
    elif error_code == SQLITE_CANTOPEN_CODE or error_name == "SQLITE_CANTOPEN":
        _raise_aiosqlite_error(error, error_code, DatabaseConnectionError, "connection error")
    elif error_code == SQLITE_IOERR_CODE or error_name == "SQLITE_IOERR":
        _raise_aiosqlite_error(error, error_code, OperationalError, "operational error")
    elif error_code == SQLITE_MISMATCH_CODE or error_name == "SQLITE_MISMATCH":
        _raise_aiosqlite_error(error, error_code, DataError, "data error")
    elif error_code == 1 or "syntax" in error_msg:
        _raise_aiosqlite_error(error, error_code, SQLParsingError, "SQL syntax error")
    else:
        _raise_aiosqlite_error(error, error_code, SQLSpecError, "database error")


def build_aiosqlite_profile() -> "DriverParameterProfile":
    """Create the AIOSQLite driver parameter profile."""

    return DriverParameterProfile(
        name="AIOSQLite",
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
        custom_type_coercions={
            bool: _bool_to_int,
            datetime: _TIME_TO_ISO,
            date: _TIME_TO_ISO,
            Decimal: _DECIMAL_TO_STRING,
        },
        default_dialect="sqlite",
    )


def build_aiosqlite_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the AIOSQLite statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = build_aiosqlite_profile()
    return build_statement_config_from_profile(
        profile, statement_overrides={"dialect": "sqlite"}, json_serializer=serializer, json_deserializer=deserializer
    )


aiosqlite_statement_config = build_aiosqlite_statement_config()


def apply_aiosqlite_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply AIOSQLite driver feature defaults to statement config."""
    processed_driver_features: dict[str, Any] = dict(driver_features) if driver_features else {}
    processed_driver_features.setdefault("enable_custom_adapters", True)
    json_serializer = processed_driver_features.setdefault("json_serializer", to_json)
    json_deserializer = processed_driver_features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, processed_driver_features
