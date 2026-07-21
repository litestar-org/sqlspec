"""SQLite adapter compiled helpers."""

import contextlib
import sqlite3
import sys
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, cast

from sqlglot import exp

from sqlspec.core import DriverParameterProfile, ParameterStyle, StatementConfig, build_statement_config_from_profile
from sqlspec.driver import rows_to_dicts
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
from sqlspec.utils.serializers import from_json, to_json
from sqlspec.utils.text import quote_identifier, split_qualified_identifier
from sqlspec.utils.type_converters import build_decimal_converter, build_uuid_coercions, time_iso_convert
from sqlspec.utils.type_guards import has_sqlite_error

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sqlspec.core.compiler import OperationType

__all__ = (
    "SqliteStreamSource",
    "apply_driver_features",
    "build_connection_config",
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
    "normalize_lastrowid",
    "require_python_version",
    "resolve_lastrowid",
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
SQLITE_BUSY_CODE = 5
SQLITE_LOCKED_CODE = 6
SQLITE_INTERRUPT_CODE = 9
SQLITE_PERM_CODE = 3
SQLITE_READONLY_CODE = 8
SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT = sys.version_info >= (3, 12)
SQLITE_DATABASE_LIST_MIN_COLUMNS = 2
SQLITE_TABLE_LIST_MIN_COLUMNS = 5
SQLITE_TABLE_INFO_MIN_COLUMNS = 2
SQLITE_ROWID_ALIASES = ("rowid", "_rowid_", "oid")


_TIME_TO_ISO = time_iso_convert
_DECIMAL_TO_STRING = build_decimal_converter(mode="string")


def format_identifier(identifier: str) -> str:
    cleaned = identifier.strip()
    if not cleaned:
        msg = "Table name must not be empty"
        raise SQLSpecError(msg)

    parts = split_qualified_identifier(cleaned, quote_chars='"')
    return ".".join(quote_identifier(part) for part in parts)


def build_insert_statement(table: str, columns: "list[str]") -> str:
    column_clause = ", ".join(quote_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return f"INSERT INTO {format_identifier(table)} ({column_clause}) VALUES ({placeholders})"


def collect_rows(fetched_data: "list[Any]", description: "Sequence[Any] | None") -> "tuple[list[Any], list[str], int]":
    """Collect SQLite result rows as raw tuples.

    Returns raw driver-native rows without dict conversion for lazy materialization.

    Args:
        fetched_data: Raw rows from cursor.fetchall()
        description: Cursor description (tuple of tuples)

    Returns:
        Tuple of (data, column_names, row_count)
    """
    if not description:
        return [], [], 0

    column_names = [col[0] for col in description]
    return fetched_data, column_names, len(fetched_data)


def resolve_rowcount(cursor: Any) -> int:
    """Resolve rowcount from a SQLite cursor.

    Args:
        cursor: SQLite cursor with optional rowcount metadata.

    Returns:
        Positive rowcount value or 0 when unknown.
    """
    try:
        rowcount = cursor.rowcount
    except AttributeError:
        return 0

    if isinstance(rowcount, int) and rowcount > 0:
        return rowcount
    return 0


def normalize_lastrowid(cursor: Any, operation_type: "OperationType", rowcount: int) -> "int | None":
    """Return SQLite lastrowid for a successful INSERT.

    Args:
        cursor: SQLite cursor with optional lastrowid metadata.
        operation_type: Compiled statement operation type.
        rowcount: Normalized affected-row count.

    Returns:
        Integer lastrowid, or None for non-INSERT or unsuccessful operations.
    """
    if operation_type != "INSERT" or rowcount <= 0:
        return None
    try:
        lastrowid = cursor.lastrowid
    except AttributeError:
        return None
    return lastrowid if isinstance(lastrowid, int) else None


def resolve_lastrowid(
    connection: Any,
    cursor: Any,
    operation_type: "OperationType",
    rowcount: int,
    expression: Any,
    eligibility_cache: "dict[tuple[str | None, str], bool]",
) -> "int | None":
    """Resolve lastrowid only when compiled target metadata proves rowid eligibility.

    SQLite leaves lastrowid unchanged after inserts into WITHOUT ROWID tables. A
    successful INSERT therefore performs one schema metadata lookup per target
    and caches it until the driver observes DDL; ambiguous targets return None.

    Args:
        connection: SQLite connection used for schema metadata.
        cursor: Executed SQLite cursor.
        operation_type: Compiled statement operation type.
        rowcount: Normalized affected-row count.
        expression: Compiled SQLGlot expression.
        eligibility_cache: Driver-local target eligibility cache.

    Returns:
        Integer lastrowid when the current target is a rowid table, otherwise None.
    """
    if operation_type != "INSERT" or rowcount <= 0:
        return None
    target = _resolve_insert_target(expression)
    if target is None:
        return None
    supports_rowid = eligibility_cache.get(target)
    if supports_rowid is None:
        supports_rowid = _target_supports_rowid(connection, target)
        eligibility_cache[target] = supports_rowid
    if not supports_rowid:
        return None
    return normalize_lastrowid(cursor, operation_type, rowcount)


def require_python_version(feature: str, minimum: "tuple[int, int]") -> None:
    """Raise when the running Python version does not provide a sqlite3 API."""
    if sys.version_info < minimum:
        msg = (
            f"{feature} requires Python {minimum[0]}.{minimum[1]} or newer; "
            f"running Python {sys.version_info[0]}.{sys.version_info[1]}."
        )
        raise ImproperConfigurationError(msg)


def normalize_execute_parameters(parameters: Any) -> Any:
    """Normalize parameters for SQLite execute calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    return parameters or ()


class SqliteStreamSource:
    """Compiled chunk source streaming dict rows from a SQLite cursor via ``fetchmany``."""

    __slots__ = ("_chunk_size", "_column_names", "_cursor", "_driver", "_parameters", "_sql")

    def __init__(self, driver: Any, sql: str, parameters: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._parameters = parameters
        self._chunk_size = chunk_size
        self._cursor: Any = None
        self._column_names: list[str] | None = None

    def start(self) -> None:
        handler = self._driver.handle_database_exceptions()
        with handler:
            cursor = self._driver.connection.cursor()
            cursor.arraysize = self._chunk_size
            self._cursor = cursor
            cursor.execute(self._sql, normalize_execute_parameters(self._parameters))
        self._driver._check_pending_exception(handler)

    def fetch_chunk(self) -> "list[dict[str, Any]]":
        handler = self._driver.handle_database_exceptions()
        rows: list[Any] = []
        with handler:
            rows = self._cursor.fetchmany(self._chunk_size)
        self._driver._check_pending_exception(handler)
        if not rows:
            return []
        if self._column_names is None:
            self._column_names = [description[0] for description in self._cursor.description]
        return rows_to_dicts(rows, self._column_names)

    def close(self, error: bool = False) -> None:
        cursor = self._cursor
        self._cursor = None
        if cursor is not None:
            with contextlib.suppress(Exception):
                cursor.close()


def normalize_execute_many_parameters(parameters: Any) -> Any:
    """Normalize parameters for SQLite executemany calls.

    Args:
        parameters: Prepared parameters payload.

    Returns:
        Normalized parameters payload.
    """
    return parameters


def build_connection_config(connection_config: "Mapping[str, Any]") -> "dict[str, Any]":
    """Build connection configuration for pool creation.

    Args:
        connection_config: Raw connection configuration mapping.

    Returns:
        Dictionary with connection parameters.
    """
    excluded_keys = {
        "enable_foreign_keys",
        "enable_optimizations",
        "health_check_interval",
        "pool_min_size",
        "pool_max_size",
        "pool_timeout",
        "pool_recycle_seconds",
        "extra",
    }
    connection_parameters = {
        key: value
        for key, value in connection_config.items()
        if key not in excluded_keys
        and (value is not None or key == "isolation_level")
        and (key != "autocommit" or SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT)
    }

    extra = connection_config.get("extra")
    if isinstance(extra, Mapping):
        connection_parameters.update({
            key: value
            for key, value in extra.items()
            if (value is not None or key == "isolation_level")
            and (key != "autocommit" or SQLITE_CONNECT_SUPPORTS_AUTOCOMMIT)
        })

    return connection_parameters


def create_mapped_exception(error: BaseException, *, logger: Any | None = None) -> SQLSpecError:
    """Map SQLite exceptions to SQLSpec exceptions.

    This is a factory function that returns an exception instance rather than
    raising. This pattern is more robust for use in __exit__ handlers and
    avoids issues with exception control flow in different Python versions.

    Mapping priority:
        1. SQLite extended error codes (most reliable)
        2. SQLite error names
        3. Error message patterns
        4. Default SQLSpecError fallback

    Args:
        error: The SQLite exception to map
        logger: Optional logger accepted for adapter signature parity.

    Returns:
        A SQLSpec exception that wraps the original error
    """
    del logger
    if has_sqlite_error(error):
        error_code = error.sqlite_errorcode
        error_name = error.sqlite_errorname
    else:
        error_code = None
        error_name = None
    error_msg = str(error).lower()

    # Check for busy/locked conditions first (deadlock-like scenarios in SQLite)
    # SQLITE_BUSY means another process has the database locked
    # SQLITE_LOCKED means another connection has the table/rows locked
    if error_code == SQLITE_BUSY_CODE or error_name == "SQLITE_BUSY":
        return _create_sqlite_error(error, error_code, DeadlockError, "database busy")
    if error_code == SQLITE_LOCKED_CODE or error_name == "SQLITE_LOCKED":
        return _create_sqlite_error(error, error_code, DeadlockError, "database locked")
    if "locked" in error_msg or "busy" in error_msg:
        return _create_sqlite_error(error, error_code or 0, DeadlockError, "database locked")

    # Query interruption (timeout-like behavior)
    if error_code == SQLITE_INTERRUPT_CODE or error_name == "SQLITE_INTERRUPT":
        return _create_sqlite_error(error, error_code, QueryTimeoutError, "query interrupted")
    if "interrupt" in error_msg:
        return _create_sqlite_error(error, error_code or 0, QueryTimeoutError, "query interrupted")

    # Permission errors
    if error_code == SQLITE_PERM_CODE or error_name == "SQLITE_PERM":
        return _create_sqlite_error(error, error_code, PermissionDeniedError, "permission denied")
    if error_code == SQLITE_READONLY_CODE or error_name == "SQLITE_READONLY":
        return _create_sqlite_error(error, error_code, PermissionDeniedError, "database is read-only")
    if "permission denied" in error_msg or "readonly" in error_msg:
        return _create_sqlite_error(error, error_code or 0, PermissionDeniedError, "permission denied")

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

    # Constraint violations (check extended error codes first)
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

    # Connection/file errors
    if error_code == SQLITE_CANTOPEN_CODE or error_name == "SQLITE_CANTOPEN":
        return _create_sqlite_error(error, error_code, DatabaseConnectionError, "connection error")
    if error_code == SQLITE_IOERR_CODE or error_name == "SQLITE_IOERR":
        return _create_sqlite_error(error, error_code, OperationalError, "operational error")

    # Data type errors
    if error_code == SQLITE_MISMATCH_CODE or error_name == "SQLITE_MISMATCH":
        return _create_sqlite_error(error, error_code, DataError, "data error")

    # SQL syntax errors
    if error_code == 1 or "syntax" in error_msg:
        return _create_sqlite_error(error, error_code, SQLParsingError, "SQL syntax error")

    return _create_sqlite_error(error, error_code, SQLSpecError, "database error")


def build_profile() -> "DriverParameterProfile":
    """Create the SQLite driver parameter profile."""

    return DriverParameterProfile(
        name="SQLite",
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
            **build_uuid_coercions(),
        },
        default_dialect="sqlite",
    )


def build_statement_config(
    *, json_serializer: "Callable[[Any], str] | None" = None, json_deserializer: "Callable[[str], Any] | None" = None
) -> "StatementConfig":
    """Construct the SQLite statement configuration with optional JSON codecs."""
    serializer = json_serializer or to_json
    deserializer = json_deserializer or from_json
    profile = driver_profile
    return build_statement_config_from_profile(
        profile,
        statement_overrides={"dialect": "sqlite", "enable_parameter_type_wrapping": False},
        json_serializer=serializer,
        json_deserializer=deserializer,
    )


def apply_driver_features(
    statement_config: "StatementConfig", driver_features: "Mapping[str, Any] | None"
) -> "tuple[StatementConfig, dict[str, Any]]":
    """Apply SQLite driver feature defaults to statement config."""
    features: dict[str, Any] = dict(driver_features) if driver_features else {}
    features.setdefault("enable_custom_adapters", False)
    json_serializer = features.setdefault("json_serializer", to_json)
    json_deserializer = features.setdefault("json_deserializer", from_json)

    if json_serializer is not None:
        parameter_config = statement_config.parameter_config.with_json_serializers(
            json_serializer, deserializer=json_deserializer
        )
        statement_config = statement_config.replace(parameter_config=parameter_config)

    return statement_config, features


def _resolve_insert_target(expression: Any) -> "tuple[str | None, str] | None":
    if not isinstance(expression, exp.Insert):
        return None
    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    if not isinstance(target, exp.Table) or target.catalog:
        return None
    table_name = target.name
    if not table_name:
        return None
    return target.db or None, table_name


def _target_supports_rowid(connection: Any, target: "tuple[str | None, str]") -> bool:
    target_schema, target_table = target
    try:
        table_cursor = connection.execute("PRAGMA table_list")
        try:
            table_rows = table_cursor.fetchall()
        finally:
            with contextlib.suppress(Exception):
                table_cursor.close()
    except sqlite3.Error:
        return _target_supports_rowid_legacy(connection, target)

    candidates = [
        row
        for row in table_rows
        if len(row) >= SQLITE_TABLE_LIST_MIN_COLUMNS
        and isinstance(row[0], str)
        and isinstance(row[1], str)
        and row[1].casefold() == target_table.casefold()
        and row[2] == "table"
    ]
    if target_schema is not None:
        candidates = [row for row in candidates if row[0].casefold() == target_schema.casefold()]
        if not candidates:
            return _target_supports_rowid_legacy(connection, target)
        return len(candidates) == 1 and candidates[0][4] == 0
    if not candidates:
        return _target_supports_rowid_legacy(connection, target)

    schema_order = ["temp", "main"]
    if not any(row[0].casefold() in {"temp", "main"} for row in candidates):
        try:
            database_cursor = connection.execute("PRAGMA database_list")
            try:
                schema_order.extend(
                    row[1]
                    for row in database_cursor.fetchall()
                    if len(row) >= SQLITE_DATABASE_LIST_MIN_COLUMNS and isinstance(row[1], str)
                )
            finally:
                with contextlib.suppress(Exception):
                    database_cursor.close()
        except sqlite3.Error:
            return False

    for schema_name in schema_order:
        for row in candidates:
            if row[0].casefold() == schema_name.casefold():
                return bool(row[4] == 0)
    return False


def _target_supports_rowid_legacy(connection: Any, target: "tuple[str | None, str]") -> bool:
    target_schema, target_table = target
    schema_order = [target_schema] if target_schema is not None else ["temp", "main"]
    if target_schema is None:
        try:
            database_cursor = connection.execute("PRAGMA database_list")
            try:
                schema_order.extend(
                    row[1]
                    for row in database_cursor.fetchall()
                    if len(row) >= SQLITE_DATABASE_LIST_MIN_COLUMNS
                    and isinstance(row[1], str)
                    and row[1] not in {"main", "temp"}
                )
            finally:
                with contextlib.suppress(Exception):
                    database_cursor.close()
        except sqlite3.Error:
            return False

    for schema_name in schema_order:
        if schema_name is None:
            continue
        quoted_schema = quote_identifier(schema_name)
        schema_cursor = None
        schema_row = None
        try:
            schema_cursor = connection.execute(
                f"SELECT type FROM {quoted_schema}.sqlite_master WHERE name = ? COLLATE NOCASE", (target_table,)
            )
            schema_row = schema_cursor.fetchone()
        except sqlite3.Error:
            pass
        finally:
            if schema_cursor is not None:
                with contextlib.suppress(Exception):
                    schema_cursor.close()
        if schema_row is None:
            continue
        if not schema_row or schema_row[0] != "table":
            return False
        qualified_target = f"{quoted_schema}.{quote_identifier(target_table)}"
        table_info_cursor = None
        try:
            table_info_cursor = connection.execute(
                f"PRAGMA {quoted_schema}.table_info({quote_identifier(target_table)})"
            )
            column_names = {
                row[1].casefold()
                for row in table_info_cursor.fetchall()
                if len(row) >= SQLITE_TABLE_INFO_MIN_COLUMNS and isinstance(row[1], str)
            }
        except sqlite3.Error:
            return False
        finally:
            if table_info_cursor is not None:
                with contextlib.suppress(Exception):
                    table_info_cursor.close()
        hidden_alias = next((alias for alias in SQLITE_ROWID_ALIASES if alias not in column_names), None)
        if hidden_alias is None:
            return False
        probe_cursor = None
        try:
            probe_cursor = connection.execute(f"SELECT {hidden_alias} FROM {qualified_target} LIMIT 0")
        except sqlite3.Error:
            return False
        finally:
            if probe_cursor is not None:
                with contextlib.suppress(Exception):
                    probe_cursor.close()
        return True
    return False


def _create_sqlite_error(
    error: Any, code: "int | None", error_class: type[SQLSpecError], description: str
) -> SQLSpecError:
    """Create a SQLSpec exception from a SQLite error.

    Args:
        error: The original SQLite exception
        code: SQLite extended error code
        error_class: The SQLSpec exception class to instantiate
        description: Human-readable description of the error type

    Returns:
        A new SQLSpec exception instance with the original as its cause
    """
    code_str = f"[code {code}]" if code else ""
    msg = f"SQLite {description} {code_str}: {error}" if code_str else f"SQLite {description}: {error}"
    exc = error_class(msg)
    exc.__cause__ = cast("BaseException", error)
    return exc


def _bool_to_int(value: bool) -> int:
    return int(value)


driver_profile = build_profile()

default_statement_config = build_statement_config()
