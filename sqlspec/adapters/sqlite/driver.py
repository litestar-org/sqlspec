"""SQLite driver implementation."""

import contextlib
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import (
    DriverParameterProfile,
    ParameterStyle,
    build_statement_config_from_profile,
    register_driver_profile,
)
from sqlspec.driver import SyncDriverAdapterBase
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
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from sqlspec.adapters.sqlite._types import SqliteConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase

__all__ = ("SqliteCursor", "SqliteDriver", "SqliteExceptionHandler", "sqlite_statement_config")

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


def _datetime_to_iso(value: datetime) -> str:
    return value.isoformat()


def _date_to_iso(value: date) -> str:
    return value.isoformat()


def _decimal_to_str(value: Decimal) -> str:
    return str(value)


_SQLITE_PROFILE = DriverParameterProfile(
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
        datetime: _datetime_to_iso,
        date: _date_to_iso,
        Decimal: _decimal_to_str,
    },
    default_dialect="sqlite",
)

register_driver_profile("sqlite", _SQLITE_PROFILE)

sqlite_statement_config = build_statement_config_from_profile(
    _SQLITE_PROFILE, statement_overrides={"dialect": "sqlite"}, json_serializer=to_json
)


class SqliteCursor:
    """Context manager for SQLite cursor management.

    Provides automatic cursor creation and cleanup for SQLite database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "SqliteConnection") -> None:
        """Initialize cursor manager.

        Args:
            connection: SQLite database connection
        """
        self.connection = connection
        self.cursor: sqlite3.Cursor | None = None

    def __enter__(self) -> "sqlite3.Cursor":
        """Create and return a new cursor.

        Returns:
            Active SQLite cursor object
        """
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        """Clean up cursor resources.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                self.cursor.close()


class SqliteExceptionHandler:
    """Context manager for handling SQLite database exceptions.

    Maps SQLite extended result codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return
        if issubclass(exc_type, sqlite3.Error):
            self._map_sqlite_exception(exc_val)

    def _map_sqlite_exception(self, e: Any) -> None:
        """Map SQLite exception to SQLSpec exception.

        Args:
            e: sqlite3.Error instance

        Raises:
            Specific SQLSpec exception based on error code
        """
        error_code = getattr(e, "sqlite_errorcode", None)
        error_name = getattr(e, "sqlite_errorname", None)
        error_msg = str(e).lower()

        if "locked" in error_msg:
            self._raise_operational_error(e, error_code or 0)

        if not error_code:
            if "unique constraint" in error_msg:
                self._raise_unique_violation(e, 0)
            elif "foreign key constraint" in error_msg:
                self._raise_foreign_key_violation(e, 0)
            elif "not null constraint" in error_msg:
                self._raise_not_null_violation(e, 0)
            elif "check constraint" in error_msg:
                self._raise_check_violation(e, 0)
            elif "syntax" in error_msg:
                self._raise_parsing_error(e, None)
            else:
                self._raise_generic_error(e)
            return

        if error_code == SQLITE_CONSTRAINT_UNIQUE_CODE or error_name == "SQLITE_CONSTRAINT_UNIQUE":
            self._raise_unique_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_FOREIGNKEY_CODE or error_name == "SQLITE_CONSTRAINT_FOREIGNKEY":
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_NOTNULL_CODE or error_name == "SQLITE_CONSTRAINT_NOTNULL":
            self._raise_not_null_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_CHECK_CODE or error_name == "SQLITE_CONSTRAINT_CHECK":
            self._raise_check_violation(e, error_code)
        elif error_code == SQLITE_CONSTRAINT_CODE or error_name == "SQLITE_CONSTRAINT":
            self._raise_integrity_error(e, error_code)
        elif error_code == SQLITE_CANTOPEN_CODE or error_name == "SQLITE_CANTOPEN":
            self._raise_connection_error(e, error_code)
        elif error_code == SQLITE_IOERR_CODE or error_name == "SQLITE_IOERR":
            self._raise_operational_error(e, error_code)
        elif error_code == SQLITE_MISMATCH_CODE or error_name == "SQLITE_MISMATCH":
            self._raise_data_error(e, error_code)
        elif error_code == 1 or "syntax" in error_msg:
            self._raise_parsing_error(e, error_code)
        else:
            self._raise_generic_error(e)

    def _raise_unique_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite unique constraint violation [code {code}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite foreign key constraint violation [code {code}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite not-null constraint violation [code {code}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: int) -> None:
        msg = f"SQLite check constraint violation [code {code}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: int) -> None:
        msg = f"SQLite integrity constraint violation [code {code}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: "int | None") -> None:
        code_str = f"[code {code}]" if code else ""
        msg = f"SQLite SQL syntax error {code_str}: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: int) -> None:
        msg = f"SQLite connection error [code {code}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_operational_error(self, e: Any, code: int) -> None:
        msg = f"SQLite operational error [code {code}]: {e}"
        raise OperationalError(msg) from e

    def _raise_data_error(self, e: Any, code: int) -> None:
        msg = f"SQLite data error [code {code}]: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"SQLite database error: {e}"
        raise SQLSpecError(msg) from e


class SqliteDriver(SyncDriverAdapterBase):
    """SQLite driver implementation.

    Provides SQL statement execution, transaction management, and result handling
    for SQLite databases using the standard sqlite3 module.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "sqlite"

    def __init__(
        self,
        connection: "SqliteConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        """Initialize SQLite driver.

        Args:
            connection: SQLite database connection
            statement_config: Statement configuration settings
            driver_features: Driver-specific feature flags
        """
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = sqlite_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="sqlite",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SyncDataDictionaryBase | None = None

    def with_cursor(self, connection: "SqliteConnection") -> "SqliteCursor":
        """Create context manager for SQLite cursor.

        Args:
            connection: SQLite database connection

        Returns:
            Cursor context manager for safe cursor operations
        """
        return SqliteCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Context manager that converts SQLite exceptions to SQLSpec exceptions
        """
        return SqliteExceptionHandler()

    def _try_special_handling(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "SQLResult | None":
        """Hook for SQLite-specific special operations.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for SQLite
        """
        return None

    def _execute_script(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        cursor.executemany(sql, prepared_parameters)

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_statement(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: SQLite cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    def begin(self) -> None:
        """Begin a database transaction.

        Raises:
            SQLSpecError: If transaction cannot be started
        """
        try:
            if not self.connection.in_transaction:
                self.connection.execute("BEGIN")
        except sqlite3.Error as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If transaction cannot be rolled back
        """
        try:
            self.connection.rollback()
        except sqlite3.Error as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If transaction cannot be committed
        """
        try:
            self.connection.commit()
        except sqlite3.Error as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            from sqlspec.adapters.sqlite.data_dictionary import SqliteSyncDataDictionary

            self._data_dictionary = SqliteSyncDataDictionary()
        return self._data_dictionary
