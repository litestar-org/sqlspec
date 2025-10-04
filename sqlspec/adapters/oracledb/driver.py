"""Oracle Driver"""

import contextlib
import logging
from typing import TYPE_CHECKING, Any

import oracledb
from oracledb import AsyncCursor, Cursor

from sqlspec.adapters.oracledb._types import OracleAsyncConnection, OracleSyncConnection
from sqlspec.adapters.oracledb.data_dictionary import OracleAsyncDataDictionary, OracleSyncDataDictionary
from sqlspec.adapters.oracledb.type_converter import OracleTypeConverter
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
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
    TransactionError,
    UniqueViolationError,
)
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver._common import ExecutionResult

logger = logging.getLogger(__name__)

# Oracle-specific constants
LARGE_STRING_THRESHOLD = 3000  # Threshold for large string parameters to avoid ORA-01704

_type_converter = OracleTypeConverter()

__all__ = (
    "OracleAsyncDriver",
    "OracleAsyncExceptionHandler",
    "OracleSyncDriver",
    "OracleSyncExceptionHandler",
    "oracledb_statement_config",
)

ORA_CHECK_CONSTRAINT = 2290
ORA_INTEGRITY_RANGE_START = 2200
ORA_INTEGRITY_RANGE_END = 2300
ORA_PARSING_RANGE_START = 900
ORA_PARSING_RANGE_END = 1000
ORA_TABLESPACE_FULL = 1652


oracledb_statement_config = StatementConfig(
    dialect="oracle",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_COLON,
        supported_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON, ParameterStyle.QMARK},
        default_execution_parameter_style=ParameterStyle.NAMED_COLON,
        supported_execution_parameter_styles={ParameterStyle.NAMED_COLON, ParameterStyle.POSITIONAL_COLON},
        type_coercion_map={dict: to_json, list: to_json},
        has_native_list_expansion=False,
        needs_static_script_compilation=False,
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
        self.cursor: Cursor | None = None

    def __enter__(self) -> Cursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class OracleAsyncCursor:
    """Async context manager for Oracle cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: OracleAsyncConnection) -> None:
        self.connection = connection
        self.cursor: AsyncCursor | None = None

    async def __aenter__(self) -> AsyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)  # Mark as intentionally unused
        if self.cursor is not None:
            with contextlib.suppress(Exception):
                # Oracle async cursors have a synchronous close method
                # but we need to ensure proper cleanup in the event loop context
                self.cursor.close()


class OracleSyncExceptionHandler:
    """Context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        if issubclass(exc_type, oracledb.DatabaseError):
            self._map_oracle_exception(exc_val)

    def _map_oracle_exception(self, e: Any) -> None:
        """Map Oracle exception to SQLSpec exception.

        Args:
            e: oracledb.DatabaseError instance
        """
        error_obj = e.args[0] if e.args else None
        if not error_obj:
            self._raise_generic_error(e, None)

        error_code = getattr(error_obj, "code", None)

        if not error_code:
            self._raise_generic_error(e, None)

        if error_code == 1:
            self._raise_unique_violation(e, error_code)
        elif error_code in {2291, 2292}:
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == ORA_CHECK_CONSTRAINT:
            self._raise_check_violation(e, error_code)
        elif error_code in {1400, 1407}:
            self._raise_not_null_violation(e, error_code)
        elif error_code and ORA_INTEGRITY_RANGE_START <= error_code < ORA_INTEGRITY_RANGE_END:
            self._raise_integrity_error(e, error_code)
        elif error_code in {1017, 12154, 12541, 12545, 12514, 12505}:
            self._raise_connection_error(e, error_code)
        elif error_code in {60, 8176}:
            self._raise_transaction_error(e, error_code)
        elif error_code in {1722, 1858, 1840}:
            self._raise_data_error(e, error_code)
        elif error_code and ORA_PARSING_RANGE_START <= error_code < ORA_PARSING_RANGE_END:
            self._raise_parsing_error(e, error_code)
        elif error_code == ORA_TABLESPACE_FULL:
            self._raise_operational_error(e, error_code)
        else:
            self._raise_generic_error(e, error_code)

    def _raise_unique_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle unique constraint violation [ORA-{code:05d}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle foreign key constraint violation [ORA-{code:05d}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle check constraint violation [ORA-{code:05d}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle not-null constraint violation [ORA-{code:05d}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: int) -> None:
        msg = f"Oracle integrity constraint violation [ORA-{code:05d}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: int) -> None:
        msg = f"Oracle SQL syntax error [ORA-{code:05d}]: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: int) -> None:
        msg = f"Oracle connection error [ORA-{code:05d}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: int) -> None:
        msg = f"Oracle transaction error [ORA-{code:05d}]: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: int) -> None:
        msg = f"Oracle data error [ORA-{code:05d}]: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: int) -> None:
        msg = f"Oracle operational error [ORA-{code:05d}]: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "int | None") -> None:
        msg = f"Oracle database error [ORA-{code:05d}]: {e}" if code else f"Oracle database error: {e}"
        raise SQLSpecError(msg) from e


class OracleAsyncExceptionHandler:
    """Async context manager for handling Oracle database exceptions.

    Maps Oracle ORA-XXXXX error codes to specific SQLSpec exceptions
    for better error handling in application code.
    """

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        if issubclass(exc_type, oracledb.DatabaseError):
            self._map_oracle_exception(exc_val)

    def _map_oracle_exception(self, e: Any) -> None:
        """Map Oracle exception to SQLSpec exception.

        Args:
            e: oracledb.DatabaseError instance
        """
        error_obj = e.args[0] if e.args else None
        if not error_obj:
            self._raise_generic_error(e, None)

        error_code = getattr(error_obj, "code", None)

        if not error_code:
            self._raise_generic_error(e, None)

        if error_code == 1:
            self._raise_unique_violation(e, error_code)
        elif error_code in {2291, 2292}:
            self._raise_foreign_key_violation(e, error_code)
        elif error_code == ORA_CHECK_CONSTRAINT:
            self._raise_check_violation(e, error_code)
        elif error_code in {1400, 1407}:
            self._raise_not_null_violation(e, error_code)
        elif error_code and ORA_INTEGRITY_RANGE_START <= error_code < ORA_INTEGRITY_RANGE_END:
            self._raise_integrity_error(e, error_code)
        elif error_code in {1017, 12154, 12541, 12545, 12514, 12505}:
            self._raise_connection_error(e, error_code)
        elif error_code in {60, 8176}:
            self._raise_transaction_error(e, error_code)
        elif error_code in {1722, 1858, 1840}:
            self._raise_data_error(e, error_code)
        elif error_code and ORA_PARSING_RANGE_START <= error_code < ORA_PARSING_RANGE_END:
            self._raise_parsing_error(e, error_code)
        elif error_code == ORA_TABLESPACE_FULL:
            self._raise_operational_error(e, error_code)
        else:
            self._raise_generic_error(e, error_code)

    def _raise_unique_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle unique constraint violation [ORA-{code:05d}]: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle foreign key constraint violation [ORA-{code:05d}]: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_check_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle check constraint violation [ORA-{code:05d}]: {e}"
        raise CheckViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any, code: int) -> None:
        msg = f"Oracle not-null constraint violation [ORA-{code:05d}]: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_integrity_error(self, e: Any, code: int) -> None:
        msg = f"Oracle integrity constraint violation [ORA-{code:05d}]: {e}"
        raise IntegrityError(msg) from e

    def _raise_parsing_error(self, e: Any, code: int) -> None:
        msg = f"Oracle SQL syntax error [ORA-{code:05d}]: {e}"
        raise SQLParsingError(msg) from e

    def _raise_connection_error(self, e: Any, code: int) -> None:
        msg = f"Oracle connection error [ORA-{code:05d}]: {e}"
        raise DatabaseConnectionError(msg) from e

    def _raise_transaction_error(self, e: Any, code: int) -> None:
        msg = f"Oracle transaction error [ORA-{code:05d}]: {e}"
        raise TransactionError(msg) from e

    def _raise_data_error(self, e: Any, code: int) -> None:
        msg = f"Oracle data error [ORA-{code:05d}]: {e}"
        raise DataError(msg) from e

    def _raise_operational_error(self, e: Any, code: int) -> None:
        msg = f"Oracle operational error [ORA-{code:05d}]: {e}"
        raise OperationalError(msg) from e

    def _raise_generic_error(self, e: Any, code: "int | None") -> None:
        msg = f"Oracle database error [ORA-{code:05d}]: {e}" if code else f"Oracle database error: {e}"
        raise SQLSpecError(msg) from e


class OracleSyncDriver(SyncDriverAdapterBase):
    """Synchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "oracle"

    def __init__(
        self,
        connection: OracleSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
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
        self._data_dictionary: SyncDataDictionaryBase | None = None

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

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
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

        # Oracle-specific fix: Ensure parameters are in list format for executemany
        # Oracle expects a list of sequences, not a tuple of sequences
        if isinstance(prepared_parameters, tuple):
            prepared_parameters = list(prepared_parameters)

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

        # Oracle-specific: Use setinputsizes for large string parameters to avoid ORA-01704
        if prepared_parameters and isinstance(prepared_parameters, dict):
            for param_name, param_value in prepared_parameters.items():
                if isinstance(param_value, str) and len(param_value) > LARGE_STRING_THRESHOLD:
                    cursor.setinputsizes(**{param_name: len(param_value)})

        cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            # Oracle returns tuples - convert to consistent dict format
            data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]

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

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = OracleSyncDataDictionary()
        return self._data_dictionary


class OracleAsyncDriver(AsyncDriverAdapterBase):
    """Asynchronous Oracle Database driver.

    Provides Oracle Database connectivity with parameter style conversion,
    error handling, and transaction management for async operations.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "oracle"

    def __init__(
        self,
        connection: OracleAsyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
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
        self._data_dictionary: AsyncDataDictionaryBase | None = None

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

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "SQLResult | None":
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

        # Oracle-specific: Use setinputsizes for large string parameters to avoid ORA-01704
        if prepared_parameters and isinstance(prepared_parameters, dict):
            for param_name, param_value in prepared_parameters.items():
                if isinstance(param_value, str) and len(param_value) > LARGE_STRING_THRESHOLD:
                    cursor.setinputsizes(**{param_name: len(param_value)})

        await cursor.execute(sql, prepared_parameters or {})

        # SELECT result processing for Oracle
        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            # Oracle returns tuples - convert to consistent dict format
            data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]

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

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = OracleAsyncDataDictionary()
        return self._data_dictionary
