"""DuckDB driver implementation."""

import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final

import duckdb
from sqlglot import exp

from sqlspec.adapters.duckdb.data_dictionary import DuckDBSyncDataDictionary
from sqlspec.adapters.duckdb.type_converter import DuckDBTypeConverter
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    DataError,
    ForeignKeyViolationError,
    IntegrityError,
    NotFoundError,
    NotNullViolationError,
    OperationalError,
    SQLParsingError,
    SQLSpecError,
    UniqueViolationError,
)
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from sqlspec.adapters.duckdb._types import DuckDBConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.driver import ExecutionResult
    from sqlspec.driver._sync import SyncDataDictionaryBase

__all__ = ("DuckDBCursor", "DuckDBDriver", "DuckDBExceptionHandler", "duckdb_statement_config")

logger = get_logger("adapters.duckdb")

_type_converter = DuckDBTypeConverter()


duckdb_statement_config = StatementConfig(
    dialect="duckdb",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.QMARK,
        supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR},
        default_execution_parameter_style=ParameterStyle.QMARK,
        supported_execution_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NUMERIC},
        type_coercion_map={
            bool: int,
            datetime.datetime: lambda v: v.isoformat(),
            datetime.date: lambda v: v.isoformat(),
            Decimal: str,
            dict: to_json,
            list: to_json,
            str: _type_converter.convert_if_detected,
        },
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        preserve_parameter_format=True,
        allow_mixed_parameter_styles=False,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


MODIFYING_OPERATIONS: Final[tuple[str, ...]] = ("INSERT", "UPDATE", "DELETE")


class DuckDBCursor:
    """Context manager for DuckDB cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "DuckDBConnection") -> None:
        self.connection = connection
        self.cursor: Any | None = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class DuckDBExceptionHandler:
    """Context manager for handling DuckDB database exceptions.

    Uses exception type and message-based detection to map DuckDB errors
    to specific SQLSpec exceptions for better error handling.
    """

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = exc_tb
        if exc_type is None:
            return
        self._map_duckdb_exception(exc_type, exc_val)

    def _map_duckdb_exception(self, exc_type: Any, e: Any) -> None:
        """Map DuckDB exception to SQLSpec exception.

        Uses exception type and message-based detection.

        Args:
            exc_type: Exception type
            e: Exception instance
        """
        error_msg = str(e).lower()
        exc_name = exc_type.__name__ if hasattr(exc_type, "__name__") else str(exc_type)

        if "constraintexception" in exc_name.lower():
            self._handle_constraint_exception(e, error_msg)
        elif "catalogexception" in exc_name.lower():
            self._raise_not_found_error(e)
        elif "parserexception" in exc_name.lower() or "binderexception" in exc_name.lower():
            self._raise_parsing_error(e)
        elif "ioexception" in exc_name.lower():
            self._raise_operational_error(e)
        elif "conversionexception" in exc_name.lower() or "type mismatch" in error_msg:
            self._raise_data_error(e)
        else:
            self._raise_generic_error(e)

    def _handle_constraint_exception(self, e: Any, error_msg: str) -> None:
        """Handle constraint exceptions using message-based detection.

        Args:
            e: Exception instance
            error_msg: Lowercase error message
        """
        if "unique" in error_msg or "duplicate" in error_msg:
            self._raise_unique_violation(e)
        elif "foreign key" in error_msg or "violates foreign key" in error_msg:
            self._raise_foreign_key_violation(e)
        elif "not null" in error_msg or "null value" in error_msg:
            self._raise_not_null_violation(e)
        elif "check constraint" in error_msg or "check condition" in error_msg:
            self._raise_check_violation(e)
        else:
            self._raise_integrity_error(e)

    def _raise_unique_violation(self, e: Any) -> None:
        msg = f"DuckDB unique constraint violation: {e}"
        raise UniqueViolationError(msg) from e

    def _raise_foreign_key_violation(self, e: Any) -> None:
        msg = f"DuckDB foreign key constraint violation: {e}"
        raise ForeignKeyViolationError(msg) from e

    def _raise_not_null_violation(self, e: Any) -> None:
        msg = f"DuckDB not-null constraint violation: {e}"
        raise NotNullViolationError(msg) from e

    def _raise_check_violation(self, e: Any) -> None:
        msg = f"DuckDB check constraint violation: {e}"
        raise CheckViolationError(msg) from e

    def _raise_integrity_error(self, e: Any) -> None:
        msg = f"DuckDB integrity constraint violation: {e}"
        raise IntegrityError(msg) from e

    def _raise_not_found_error(self, e: Any) -> None:
        msg = f"DuckDB catalog error: {e}"
        raise NotFoundError(msg) from e

    def _raise_parsing_error(self, e: Any) -> None:
        msg = f"DuckDB SQL parsing error: {e}"
        raise SQLParsingError(msg) from e

    def _raise_operational_error(self, e: Any) -> None:
        msg = f"DuckDB operational error: {e}"
        raise OperationalError(msg) from e

    def _raise_data_error(self, e: Any) -> None:
        msg = f"DuckDB data error: {e}"
        raise DataError(msg) from e

    def _raise_generic_error(self, e: Any) -> None:
        msg = f"DuckDB database error: {e}"
        raise SQLSpecError(msg) from e


class DuckDBDriver(SyncDriverAdapterBase):
    """Synchronous DuckDB database driver.

    Provides SQL statement execution, transaction management, and result handling
    for DuckDB databases. Supports multiple parameter styles including QMARK,
    NUMERIC, and NAMED_DOLLAR formats.

    The driver handles script execution, batch operations, and integrates with
    the sqlspec.core modules for statement processing and caching.
    """

    __slots__ = ("_data_dictionary",)
    dialect = "duckdb"

    def __init__(
        self,
        connection: "DuckDBConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            updated_config = duckdb_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="duckdb",
            )
            statement_config = updated_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: SyncDataDictionaryBase | None = None

    def with_cursor(self, connection: "DuckDBConnection") -> "DuckDBCursor":
        """Create context manager for DuckDB cursor.

        Args:
            connection: DuckDB connection instance

        Returns:
            DuckDBCursor context manager instance
        """
        return DuckDBCursor(connection)

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately.

        Returns:
            Context manager that catches and converts DuckDB exceptions
        """
        return DuckDBExceptionHandler()

    def _try_special_handling(self, cursor: Any, statement: SQL) -> "SQLResult | None":
        """Handle DuckDB-specific special operations.

        DuckDB does not require special operation handling, so this method
        returns None to indicate standard execution should proceed.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement to analyze

        Returns:
            None to indicate no special handling required
        """
        _ = (cursor, statement)
        return None

    def _is_modifying_operation(self, statement: SQL) -> bool:
        """Check if the SQL statement modifies data.

        Determines if a statement is an INSERT, UPDATE, or DELETE operation
        using AST analysis when available, falling back to text parsing.

        Args:
            statement: SQL statement to analyze

        Returns:
            True if the operation modifies data (INSERT/UPDATE/DELETE)
        """

        expression = statement.expression
        if expression and isinstance(expression, (exp.Insert, exp.Update, exp.Delete)):
            return True

        sql_upper = statement.sql.strip().upper()
        return any(sql_upper.startswith(op) for op in MODIFYING_OPERATIONS)

    def _execute_script(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Parses multi-statement scripts and executes each statement sequentially
        with the provided parameters.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with script content

        Returns:
            ExecutionResult with script execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None

        for stmt in statements:
            last_result = cursor.execute(stmt, prepared_parameters or ())
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using batch processing.

        Uses DuckDB's executemany method for batch operations and calculates
        row counts for both data modification and query operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            cursor.executemany(sql, prepared_parameters)

            if self._is_modifying_operation(statement):
                row_count = len(prepared_parameters)
            else:
                try:
                    result = cursor.fetchone()
                    row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
                except Exception:
                    row_count = max(cursor.rowcount, 0) if hasattr(cursor, "rowcount") else 0
        else:
            row_count = 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: SQL) -> "ExecutionResult":
        """Execute single SQL statement with data handling.

        Executes a SQL statement with parameter binding and processes the results.
        Handles both data-returning queries and data modification operations.

        Args:
            cursor: DuckDB cursor object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with execution metadata
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        cursor.execute(sql, prepared_parameters or ())

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            if fetched_data and isinstance(fetched_data[0], tuple):
                dict_data = [dict(zip(column_names, row, strict=False)) for row in fetched_data]
            else:
                dict_data = fetched_data

            return self.create_execution_result(
                cursor,
                selected_data=dict_data,
                column_names=column_names,
                data_row_count=len(dict_data),
                is_select_result=True,
            )

        try:
            result = cursor.fetchone()
            row_count = int(result[0]) if result and isinstance(result, tuple) and len(result) == 1 else 0
        except Exception:
            row_count = max(cursor.rowcount, 0) if hasattr(cursor, "rowcount") else 0

        return self.create_execution_result(cursor, rowcount_override=row_count)

    def begin(self) -> None:
        """Begin a database transaction."""
        try:
            self.connection.execute("BEGIN TRANSACTION")
        except duckdb.Error as e:
            msg = f"Failed to begin DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            self.connection.rollback()
        except duckdb.Error as e:
            msg = f"Failed to rollback DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction."""
        try:
            self.connection.commit()
        except duckdb.Error as e:
            msg = f"Failed to commit DuckDB transaction: {e}"
            raise SQLSpecError(msg) from e

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = DuckDBSyncDataDictionary()
        return self._data_dictionary
