"""PostgreSQL psycopg driver implementation.

This driver provides PostgreSQL database connectivity using psycopg3:
- SQL statement execution with parameter binding
- Connection and transaction management
- Row result processing with dictionary-based access
- PostgreSQL-specific features (COPY, arrays, JSON types)

PostgreSQL Features:
- Parameter styles ($1, %s, %(name)s)
- PostgreSQL array support
- COPY operations for bulk data transfer
- JSON/JSONB type handling
- PostgreSQL-specific error handling
"""

import io
from typing import TYPE_CHECKING, Any, Optional

import psycopg

from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.result import SQLResult
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager, AbstractContextManager

    from sqlspec.driver._common import ExecutionResult

logger = get_logger("adapters.psycopg")


TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4


def _convert_list_to_postgres_array(value: Any) -> str:
    """Convert Python list to PostgreSQL array literal format.

    Args:
        value: Python list to convert

    Returns:
        PostgreSQL array literal string
    """
    if not isinstance(value, list):
        return str(value)

    elements = []
    for item in value:
        if isinstance(item, list):
            elements.append(_convert_list_to_postgres_array(item))
        elif isinstance(item, str):
            escaped = item.replace("'", "''")
            elements.append(f"'{escaped}'")
        elif item is None:
            elements.append("NULL")
        else:
            elements.append(str(item))

    return f"{{{','.join(elements)}}}"


psycopg_statement_config = StatementConfig(
    dialect="postgres",
    pre_process_steps=None,
    post_process_steps=None,
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_parameter_styles={
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NUMERIC,
            ParameterStyle.QMARK,
        },
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_execution_parameter_styles={
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NUMERIC,
        },
        type_coercion_map={dict: to_json},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        preserve_parameter_format=True,
    ),
)

__all__ = (
    "PsycopgAsyncCursor",
    "PsycopgAsyncDriver",
    "PsycopgAsyncExceptionHandler",
    "PsycopgSyncCursor",
    "PsycopgSyncDriver",
    "PsycopgSyncExceptionHandler",
    "psycopg_statement_config",
)


class PsycopgSyncCursor:
    """Context manager for PostgreSQL psycopg cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: PsycopgSyncConnection) -> None:
        self.connection = connection
        self.cursor: Optional[Any] = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            self.cursor.close()


class PsycopgSyncExceptionHandler:
    """Context manager for handling PostgreSQL psycopg database exceptions."""

    __slots__ = ()

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        if issubclass(exc_type, psycopg.IntegrityError):
            e = exc_val
            msg = f"PostgreSQL psycopg integrity constraint violation: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.ProgrammingError):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "parse" in error_msg:
                msg = f"PostgreSQL psycopg SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"PostgreSQL psycopg programming error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.OperationalError):
            e = exc_val
            msg = f"PostgreSQL psycopg operational error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.DatabaseError):
            e = exc_val
            msg = f"PostgreSQL psycopg database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.Error):
            e = exc_val
            msg = f"PostgreSQL psycopg error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected database operation error: {e}"
            raise SQLSpecError(msg) from e


class PsycopgSyncDriver(SyncDriverAdapterBase):
    """PostgreSQL psycopg synchronous driver.

    Provides synchronous database operations for PostgreSQL using psycopg3.
    Supports SQL statement execution with parameter binding, transaction
    management, result processing with column metadata, parameter style
    conversion, PostgreSQL arrays and JSON handling, COPY operations for
    bulk data transfer, and PostgreSQL-specific error handling.
    """

    __slots__ = ()
    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            default_config = psycopg_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )
            statement_config = default_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: PsycopgSyncConnection) -> PsycopgSyncCursor:
        """Create context manager for PostgreSQL cursor."""
        return PsycopgSyncCursor(connection)

    def begin(self) -> None:
        """Begin a database transaction on the current connection."""
        try:
            if hasattr(self.connection, "autocommit") and not self.connection.autocommit:
                pass
            else:
                self.connection.autocommit = False
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""
        try:
            self.connection.rollback()
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    def commit(self) -> None:
        """Commit the current transaction on the current connection."""
        try:
            self.connection.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def handle_database_exceptions(self) -> "AbstractContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return PsycopgSyncExceptionHandler()

    def _handle_transaction_error_cleanup(self) -> None:
        """Handle transaction cleanup after database errors."""
        try:
            if hasattr(self.connection, "info") and hasattr(self.connection.info, "transaction_status"):
                status = self.connection.info.transaction_status

                if status == TRANSACTION_STATUS_INERROR:
                    logger.debug("Connection in aborted transaction state, performing rollback")
                    self.connection.rollback()
        except Exception as cleanup_error:
            logger.warning("Failed to cleanup transaction state: %s", cleanup_error)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL-specific special operations.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling was applied, None otherwise
        """

        statement.compile()

        if statement.operation_type in {"COPY_FROM", "COPY_TO"}:
            return self._handle_copy_operation(cursor, statement)

        return None

    def _handle_copy_operation(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Handle PostgreSQL COPY operations using copy_expert.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement with COPY operation

        Returns:
            SQLResult with COPY operation results
        """

        sql = statement.sql

        copy_data = statement.parameters
        if isinstance(copy_data, list) and len(copy_data) == 1:
            copy_data = copy_data[0]

        if statement.operation_type == "COPY_FROM":
            if isinstance(copy_data, (str, bytes)):
                data_file = io.StringIO(copy_data) if isinstance(copy_data, str) else io.BytesIO(copy_data)
            elif hasattr(copy_data, "read"):
                data_file = copy_data
            else:
                data_file = io.StringIO(str(copy_data))

            with cursor.copy(sql) as copy_ctx:
                data_to_write = data_file.read() if hasattr(data_file, "read") else str(copy_data)  # pyright: ignore
                if isinstance(data_to_write, str):
                    data_to_write = data_to_write.encode()
                copy_ctx.write(data_to_write)

            rows_affected = max(cursor.rowcount, 0)

            return SQLResult(
                data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FROM_STDIN"}
            )

        if statement.operation_type == "COPY_TO":
            output_data: list[str] = []
            with cursor.copy(sql) as copy_ctx:
                output_data.extend(row.decode() if isinstance(row, bytes) else str(row) for row in copy_ctx)

            exported_data = "".join(output_data)

            return SQLResult(
                data=[{"copy_output": exported_data}],
                rows_affected=0,
                statement=statement,
                metadata={"copy_operation": "TO_STDOUT"},
            )

        cursor.execute(sql)
        rows_affected = max(cursor.rowcount, 0)

        return SQLResult(
            data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FILE"}
        )

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with multiple statements.

        Args:
            cursor: Database cursor
            statement: SQL statement containing multiple commands

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            if prepared_parameters:
                cursor.execute(stmt, prepared_parameters)
            else:
                cursor.execute(stmt)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets.

        Args:
            cursor: Database cursor
            statement: SQL statement with parameter list

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        cursor.executemany(sql, prepared_parameters)

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            cursor.execute(sql, prepared_parameters)
        else:
            cursor.execute(sql)

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]

            return self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)


class PsycopgAsyncCursor:
    """Async context manager for PostgreSQL psycopg cursor management."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: "PsycopgAsyncConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Any] = None

    async def __aenter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        if self.cursor is not None:
            await self.cursor.close()


class PsycopgAsyncExceptionHandler:
    """Async context manager for handling PostgreSQL psycopg database exceptions."""

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return

        if issubclass(exc_type, psycopg.IntegrityError):
            e = exc_val
            msg = f"PostgreSQL psycopg integrity constraint violation: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.ProgrammingError):
            e = exc_val
            error_msg = str(e).lower()
            if "syntax" in error_msg or "parse" in error_msg:
                msg = f"PostgreSQL psycopg SQL syntax error: {e}"
                raise SQLParsingError(msg) from e
            msg = f"PostgreSQL psycopg programming error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.OperationalError):
            e = exc_val
            msg = f"PostgreSQL psycopg operational error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.DatabaseError):
            e = exc_val
            msg = f"PostgreSQL psycopg database error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, psycopg.Error):
            e = exc_val
            msg = f"PostgreSQL psycopg error: {e}"
            raise SQLSpecError(msg) from e
        if issubclass(exc_type, Exception):
            e = exc_val
            error_msg = str(e).lower()
            if "parse" in error_msg or "syntax" in error_msg:
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected async database operation error: {e}"
            raise SQLSpecError(msg) from e


class PsycopgAsyncDriver(AsyncDriverAdapterBase):
    """PostgreSQL psycopg asynchronous driver.

    Provides asynchronous database operations for PostgreSQL using psycopg3.
    Supports async SQL statement execution with parameter binding, async
    transaction management, async result processing with column metadata,
    parameter style conversion, PostgreSQL arrays and JSON handling, COPY
    operations for bulk data transfer, PostgreSQL-specific error handling,
    and async pub/sub support.
    """

    __slots__ = ()
    dialect = "postgres"

    def __init__(
        self,
        connection: "PsycopgAsyncConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            default_config = psycopg_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )
            statement_config = default_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "PsycopgAsyncConnection") -> "PsycopgAsyncCursor":
        """Create async context manager for PostgreSQL cursor."""
        return PsycopgAsyncCursor(connection)

    async def begin(self) -> None:
        """Begin a database transaction on the current connection."""
        try:
            if hasattr(self.connection, "autocommit") and not self.connection.autocommit:
                pass
            else:
                self.connection.autocommit = False
        except Exception as e:
            msg = f"Failed to begin transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction on the current connection."""
        try:
            await self.connection.rollback()
        except Exception as e:
            msg = f"Failed to rollback transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction on the current connection."""
        try:
            await self.connection.commit()
        except Exception as e:
            msg = f"Failed to commit transaction: {e}"
            raise SQLSpecError(msg) from e

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database-specific exceptions and wrap them appropriately."""
        return PsycopgAsyncExceptionHandler()

    async def _handle_transaction_error_cleanup_async(self) -> None:
        """Handle async transaction cleanup after database errors."""
        try:
            if hasattr(self.connection, "info") and hasattr(self.connection.info, "transaction_status"):
                status = self.connection.info.transaction_status

                if status == TRANSACTION_STATUS_INERROR:
                    logger.debug("Connection in aborted transaction state, performing async rollback")
                    await self.connection.rollback()
        except Exception as cleanup_error:
            logger.warning("Failed to cleanup transaction state: %s", cleanup_error)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL-specific special operations.

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special handling was applied, None otherwise
        """

        sql_upper = statement.sql.strip().upper()
        if sql_upper.startswith("COPY ") and ("FROM STDIN" in sql_upper or "TO STDOUT" in sql_upper):
            return await self._handle_copy_operation_async(cursor, statement)

        return None

    async def _handle_copy_operation_async(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Handle PostgreSQL COPY operations (async).

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement with COPY operation

        Returns:
            SQLResult with COPY operation results
        """

        sql = statement.sql

        copy_data = statement.parameters
        if isinstance(copy_data, list) and len(copy_data) == 1:
            copy_data = copy_data[0]

        sql_upper = sql.upper()
        is_stdin = "FROM STDIN" in sql_upper
        is_stdout = "TO STDOUT" in sql_upper

        if is_stdin:
            if isinstance(copy_data, (str, bytes)):
                data_file = io.StringIO(copy_data) if isinstance(copy_data, str) else io.BytesIO(copy_data)
            elif hasattr(copy_data, "read"):
                data_file = copy_data
            else:
                data_file = io.StringIO(str(copy_data))

            async with cursor.copy(sql) as copy_ctx:
                data_to_write = data_file.read() if hasattr(data_file, "read") else str(copy_data)  # pyright: ignore
                if isinstance(data_to_write, str):
                    data_to_write = data_to_write.encode()
                await copy_ctx.write(data_to_write)

            rows_affected = max(cursor.rowcount, 0)

            return SQLResult(
                data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FROM_STDIN"}
            )

        if is_stdout:
            output_data: list[str] = []
            async with cursor.copy(sql) as copy_ctx:
                output_data.extend([row.decode() if isinstance(row, bytes) else str(row) async for row in copy_ctx])

            exported_data = "".join(output_data)

            return SQLResult(
                data=[{"copy_output": exported_data}],
                rows_affected=0,
                statement=statement,
                metadata={"copy_operation": "TO_STDOUT"},
            )

        await cursor.execute(sql)
        rows_affected = max(cursor.rowcount, 0)

        return SQLResult(
            data=None, rows_affected=rows_affected, statement=statement, metadata={"copy_operation": "FILE"}
        )

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with multiple statements (async).

        Args:
            cursor: Database cursor
            statement: SQL statement containing multiple commands

        Returns:
            ExecutionResult with script execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_cursor = cursor

        for stmt in statements:
            if prepared_parameters:
                await cursor.execute(stmt, prepared_parameters)
            else:
                await cursor.execute(stmt)
            successful_count += 1

        return self.create_execution_result(
            last_cursor, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets (async).

        Args:
            cursor: Database cursor
            statement: SQL statement with parameter list

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        await cursor.executemany(sql, prepared_parameters)

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement (async).

        Args:
            cursor: Database cursor
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            await cursor.execute(sql, prepared_parameters)
        else:
            await cursor.execute(sql)

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]

            return self.create_execution_result(
                cursor,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        affected_rows = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        return self.create_execution_result(cursor, rowcount_override=affected_rows)
