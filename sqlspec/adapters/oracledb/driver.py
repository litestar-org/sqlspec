"""Oracle Driver"""

import logging
from typing import TYPE_CHECKING, Any, Optional

import oracledb
from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver._common import ExecutionResult

logger = logging.getLogger(__name__)

__all__ = (
    "OracleAsyncDriver",
    "OracleAsyncExceptionHandler",
    "OracleSyncDriver",
    "OracleSyncExceptionHandler",
    "oracledb_statement_config",
)


oracledb_statement_config = StatementConfig(
    dialect="oracle",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.POSITIONAL_COLON,
        supported_execution_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        type_coercion_map={},
        has_native_list_expansion=False,
        needs_static_script_compilation=True,
        preserve_parameter_format=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


class OracleSyncCursor:
    """Sync context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleSyncConnection) -> None:
        self.connection = connection
        self.cursor: Optional[Cursor] = None

    def __enter__(self) -> Cursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)  # Mark as intentionally unused
        if self.cursor is not None:
            self.cursor.close()


class OracleAsyncCursor:
    """Async context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleAsyncConnection) -> None:
        self.connection = connection
        self.cursor: Optional[AsyncCursor] = None

    async def __aenter__(self) -> AsyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)  # Mark as intentionally unused
        if self.cursor is not None:
            self.cursor.close()  # Synchronous method - do not await


class OracleSyncExceptionHandler:
    """Context manager for handling Oracle database exceptions in synchronous operations."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb  # Mark as intentionally unused
        if exc_type is None:
            return

        if issubclass(exc_type, oracledb.IntegrityError):
            e = exc_val
            msg = f"Oracle integrity constraint violation: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.ProgrammingError):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "parse" in error_msg:
                msg = f"Oracle SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Oracle programming error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.OperationalError):
            e = exc_val
            msg = f"Oracle operational error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.DatabaseError):
            e = exc_val
            msg = f"Oracle database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.Error):
            e = exc_val
            msg = f"Oracle error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected database operation error: {e}"
            raise SQLSpecError(msg) from e


class OracleAsyncExceptionHandler:
    """Context manager for handling Oracle database exceptions in asynchronous operations."""

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb  # Mark as intentionally unused
        if exc_type is None:
            return

        if issubclass(exc_type, oracledb.IntegrityError):
            e = exc_val
            msg = f"Oracle integrity constraint violation: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.ProgrammingError):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "parse" in error_msg:
                msg = f"Oracle SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Oracle programming error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.OperationalError):
            e = exc_val
            msg = f"Oracle operational error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.DatabaseError):
            e = exc_val
            msg = f"Oracle database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, oracledb.Error):
            e = exc_val
            msg = f"Oracle error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected async database operation error: {e}"
            raise SQLSpecError(msg) from e


class OracleSyncDriver(SyncDriverAdapterBase):
    """Synchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management.
    """

    __slots__ = ()
    dialect = "oracle"

    def __init__(
        self,
        connection: OracleSyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = oracledb_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="oracle",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: OracleSyncConnection) -> OracleSyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations
        """
        return OracleSyncCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleSyncExceptionHandler()

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have complex special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for Oracle
        """
        _ = (cursor, statement)  # Mark as intentionally unused
        return None

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: Oracle cursor object
            statement: SQL script statement to execute

        Returns:
            Execution result containing statement count and success information
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            cursor.execute(stmt, prepared_parameters or {})
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count

        Raises:
            ValueError: If no parameters are provided
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Parameter validation for executemany
        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        cursor.executemany(sql, prepared_parameters)

        # Calculate affected rows based on parameter count
        affected_rows = len(prepared_parameters) if prepared_parameters else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            # Oracle returns tuples - convert to consistent dict format
            data = [dict(zip(column_names, row)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Non-SELECT result processing
        affected_rows = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    # Oracle transaction management
    def begin(self) -> None:
        """Begin a database transaction.

        Oracle handles transactions automatically, so this is a no-op.
        """
        # Oracle handles transactions implicitly

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """
        try:
            self.connection.rollback()
        except oracledb.Error as e:
            msg = f"Failed to rollback Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If commit fails
        """
        try:
            self.connection.commit()
        except oracledb.Error as e:
            msg = f"Failed to commit Oracle transaction: {e}"
            raise SQLSpecError(msg) from e


class OracleAsyncDriver(AsyncDriverAdapterBase):
    """Asynchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management for async operations.
    """

    __slots__ = ()
    dialect = "oracle"

    def __init__(
        self,
        connection: OracleAsyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = oracledb_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="oracle",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: OracleAsyncConnection) -> OracleAsyncCursor:
        """Create context manager for Oracle cursor.

        Args:
            connection: Oracle database connection

        Returns:
            Context manager for cursor operations
        """
        return OracleAsyncCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return OracleAsyncExceptionHandler()

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for Oracle-specific special operations.

        Oracle doesn't have complex special operations like PostgreSQL COPY,
        so this always returns None to proceed with standard execution.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to analyze

        Returns:
            None - always proceeds with standard execution for Oracle
        """
        _ = (cursor, statement)  # Mark as intentionally unused
        return None

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parameters are embedded as static values for script execution compatibility.

        Args:
            cursor: Oracle cursor object
            statement: SQL script statement to execute

        Returns:
            Execution result containing statement count and success information
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            await cursor.execute(stmt, prepared_parameters or {})
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Oracle batch processing.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            Execution result with affected row count

        Raises:
            ValueError: If no parameters are provided
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        # Parameter validation for executemany
        if not prepared_parameters:
            msg = "execute_many requires parameters"
            raise ValueError(msg)

        await cursor.executemany(sql, prepared_parameters)

        # Calculate affected rows based on parameter count
        affected_rows = len(prepared_parameters) if prepared_parameters else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement with Oracle data handling.

        Args:
            cursor: Oracle cursor object
            statement: SQL statement to execute

        Returns:
            Execution result containing data for SELECT statements or row count for others
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            # Oracle returns tuples - convert to consistent dict format
            data = [dict(zip(column_names, row)) for row in fetched_data]

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        # Non-SELECT result processing
        affected_rows = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    # Oracle transaction management
    async def begin(self) -> None:
        """Begin a database transaction.

        Oracle handles transactions automatically, so this is a no-op.
        """
        # Oracle handles transactions implicitly

    async def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            SQLSpecError: If rollback fails
        """
        try:
            await self.connection.rollback()
        except oracledb.Error as e:
            msg = f"Failed to rollback Oracle transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            SQLSpecError: If commit fails
        """
        try:
            await self.connection.commit()
        except oracledb.Error as e:
            msg = f"Failed to commit Oracle transaction: {e}"
            raise SQLSpecError(msg) from e
