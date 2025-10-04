"""AsyncMy MySQL driver implementation.

Provides MySQL/MariaDB connectivity with parameter style conversion,
type coercion, error handling, and transaction management.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

import asyncmy
import asyncmy.errors  # pyright: ignore
from asyncmy.cursors import Cursor, DictCursor  # pyright: ignore

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase
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
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.asyncmy._types import AsyncmyConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._async import AsyncDataDictionaryBase

logger = logging.getLogger(__name__)

__all__ = ("AsyncmyCursor", "AsyncmyDriver", "AsyncmyExceptionHandler", "asyncmy_statement_config")

MYSQL_ER_DUP_ENTRY = 1062
MYSQL_ER_NO_DEFAULT_FOR_FIELD = 1364
MYSQL_ER_CHECK_CONSTRAINT_VIOLATED = 3819

asyncmy_statement_config = StatementConfig(
    dialect="mysql",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT},
        type_coercion_map={dict: to_json, list: to_json, tuple: lambda v: to_json(list(v)), bool: int},
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
        preserve_parameter_format=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


class AsyncmyCursor:
    """Context manager for AsyncMy cursor operations.

    Provides automatic cursor acquisition and cleanup for database operations.
    """

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "AsyncmyConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Union[Cursor, DictCursor]] = None

    async def __aenter__(self) -> Union[Cursor, DictCursor]:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: Any) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class AsyncmyExceptionHandler:
    """Async context manager for handling asyncmy (MySQL) database exceptions.

    Maps MySQL error codes and SQLSTATE to specific SQLSpec exceptions
    for better error handling in application code.
    """

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> "Optional[bool]":
        if exc_type is None:
            return None
        if issubclass(exc_type, asyncmy.errors.Error):
            return self._map_mysql_exception(exc_val)
        return None

    def _map_mysql_exception(self, e: Any) -> "Optional[bool]":
        """Map MySQL exception to SQLSpec exception.

        Args:
            e: MySQL error instance

        Returns:
            True to suppress migration-related errors, None otherwise

        Raises:
            Specific SQLSpec exception based on error code
        """
        error_code = None
        sqlstate = None

        if hasattr(e, "args") and len(e.args) >= 1 and isinstance(e.args[0], int):
            error_code = e.args[0]

        sqlstate = getattr(e, "sqlstate", None)

        if error_code in {1061, 1091}:
            logger.warning("AsyncMy MySQL expected migration error (ignoring): %s", e)
            return True

        if sqlstate == "23505" or error_code == MYSQL_ER_DUP_ENTRY:
            self._raise_unique_violation(e, sqlstate, error_code)
        elif sqlstate == "23503" or error_code in (1216, 1217, 1451, 1452):
            self._raise_foreign_key_violation(e, sqlstate, error_code)
        elif sqlstate == "23502" or error_code in (1048, MYSQL_ER_NO_DEFAULT_FOR_FIELD):
            self._raise_not_null_violation(e, sqlstate, error_code)
        elif sqlstate == "23514" or error_code == MYSQL_ER_CHECK_CONSTRAINT_VIOLATED:
            self._raise_check_violation(e, sqlstate, error_code)
        elif sqlstate and sqlstate.startswith("23"):
            self._raise_integrity_error(e, sqlstate, error_code)
        elif sqlstate and sqlstate.startswith("42"):
            self._raise_parsing_error(e, sqlstate, error_code)
        elif sqlstate and sqlstate.startswith("08"):
            self._raise_connection_error(e, sqlstate, error_code)
        elif sqlstate and sqlstate.startswith("40"):
            self._raise_transaction_error(e, sqlstate, error_code)
        elif sqlstate and sqlstate.startswith("22"):
            self._raise_data_error(e, sqlstate, error_code)
        elif error_code in (2002, 2003, 2005, 2006, 2013):
            self._raise_connection_error(e, sqlstate, error_code)
        elif error_code in (1205, 1213):
            self._raise_transaction_error(e, sqlstate, error_code)
        elif error_code in range(1064, 1100):
            self._raise_parsing_error(e, sqlstate, error_code)
        else:
            self._raise_generic_error(e, sqlstate, error_code)
        return None

    def _raise_unique_violation(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL unique constraint violation {code_str}: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL foreign key constraint violation {code_str}: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL not-null constraint violation {code_str}: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL check constraint violation {code_str}: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL integrity constraint violation {code_str}: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL SQL syntax error {code_str}: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL connection error {code_str}: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL transaction error {code_str}: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        code_str = f"[{sqlstate or code}]"
        msg = f"MySQL data error {code_str}: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any, sqlstate: "Optional[str]", code: "Optional[int]") -> None:
        if sqlstate and code:
            msg = f"MySQL database error [{sqlstate}:{code}]: {e}"
        elif sqlstate or code:
            msg = f"MySQL database error [{sqlstate or code}]: {e}"
        else:
            msg = f"MySQL database error: {e}"
        raise SQLSpecError(msg) from e


class AsyncmyDriver(AsyncDriverAdapterBase):
    """MySQL/MariaDB database driver using AsyncMy client library.

    Implements asynchronous database operations for MySQL and MariaDB servers
    with support for parameter style conversion, type coercion, error handling,
    and transaction management.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "mysql"

    def __init__(
        self,
        connection: "AsyncmyConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = asyncmy_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="mysql",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: Optional[AsyncDataDictionaryBase] = None

    def with_cursor(self, connection: "AsyncmyConnection") -> "AsyncmyCursor":
        """Create cursor context manager for the connection.

        Args:
            connection: AsyncMy database connection

        Returns:
            AsyncmyCursor: Context manager for cursor operations
        """
        return AsyncmyCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Provide exception handling context manager.

        Returns:
            AbstractAsyncContextManager[None]: Context manager for AsyncMy exception handling
        """
        return AsyncmyExceptionHandler()

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Handle AsyncMy-specific operations before standard execution.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL statement to analyze

        Returns:
            Optional[SQLResult]: None, always proceeds with standard execution
        """
        _ = (cursor, statement)
        return None

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Splits multi-statement scripts and executes each statement sequentially.
        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL script to execute

        Returns:
            ExecutionResult: Script execution results with statement count
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or None)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL statement with multiple parameter sets.

        Uses AsyncMy's executemany for batch operations with MySQL type conversion
        and parameter processing.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult: Batch execution results

        Raises:
            ValueError: If no parameters provided for executemany operation
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)

        affected_rows = len(prepared_parameters) if prepared_parameters else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Handles parameter processing, result fetching, and data transformation
        for MySQL/MariaDB operations.

        Args:
            cursor: AsyncMy cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult: Statement execution results with data or row counts
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters or None)

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description or []]

            if fetched_data and not isinstance(fetched_data[0], dict):
                data = [dict(zip(column_names, row)) for row in fetched_data]
            else:
                data = fetched_data

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        affected_rows = cursor.rowcount if cursor.rowcount is not None else -1
        last_id = getattr(cursor, "lastrowid", None) if cursor.rowcount and cursor.rowcount > 0 else None
        return self.create_execution_result(cursor, rowcount_override=affected_rows, last_inserted_id=last_id)

    async def begin(self) -> None:
        """Begin a database transaction.

        Explicitly starts a MySQL transaction to ensure proper transaction boundaries.

        Raises:
            SQLSpecError: If transaction initialization fails
        """
        try:
            async with AsyncmyCursor(self.connection) as cursor:
                await cursor.execute("BEGIN")
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to begin MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If transaction rollback fails
        """
        try:
            await self.connection.rollback()
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to rollback MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If transaction commit fails
        """
        try:
            await self.connection.commit()
        except asyncmy.errors.MySQLError as e:
            msg = f"Failed to commit MySQL transaction: {e}"
            raise SQLSpecError(msg) from e

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            from sqlspec.adapters.asyncmy.data_dictionary import MySQLAsyncDataDictionary

            self._data_dictionary = MySQLAsyncDataDictionary()
        return self._data_dictionary
