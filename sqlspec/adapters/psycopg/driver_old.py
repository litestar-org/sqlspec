import logging
from contextlib import AbstractAsyncContextManager, asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

import psycopg

from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import SQL, StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver._common import ExecutionResult

logger = logging.getLogger(__name__)


def _convert_list_to_postgres_array(value: Any) -> str:
    """Convert Python list to PostgreSQL array literal format.

    Args:
        value: Python list to convert

    Returns:
        PostgreSQL array literal string (e.g., '{1,2,3}' or '{"a","b","c"}')
    """
    if not isinstance(value, (list, tuple)) or not value:
        return "{}"

    elements = []
    for item in value:
        if item is None:
            elements.append("NULL")
        elif isinstance(item, str):
            escaped = item.replace('"', '\\"')
            elements.append(f'"{escaped}"')
        else:
            elements.append(str(item))

    return "{" + ",".join(elements) + "}"


psycopg_statement_config = StatementConfig(
    dialect="postgres",
    pre_process_steps=None,
    post_process_steps=None,
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_parameter_styles={
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NUMERIC,
            ParameterStyle.QMARK,
        },
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        type_coercion_map={
            str: lambda x: int(x) if x.isdigit() else x,
            dict: to_json,
            list: _convert_list_to_postgres_array,
            bool: lambda x: x,
        },
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
    ),
)


__all__ = (
    "PsycopgAsyncCursor",
    "PsycopgAsyncDriver",
    "PsycopgSyncCursor",
    "PsycopgSyncDriver",
    "psycopg_statement_config",
)


class PsycopgSyncCursor:
    """Context manager for Psycopg cursor management."""

    def __init__(self, connection: PsycopgSyncConnection) -> None:
        self.connection = connection
        self.cursor: Optional[Any] = None

    def __enter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor is not None:
            self.cursor.close()


class PsycopgAsyncCursor:
    def __init__(self, connection: "PsycopgAsyncConnection") -> None:
        self.connection = connection
        self.cursor: Optional[Any] = None

    async def __aenter__(self) -> Any:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.cursor:
            await self.cursor.close()


class PsycopgSyncDriver(SyncDriverAdapterBase):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or psycopg_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: PsycopgSyncConnection) -> PsycopgSyncCursor:
        return PsycopgSyncCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL-specific special operations.

        Checks for execution_mode configuration to handle special operations like COPY.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if the special operation was handled and completed,
            None if standard execution should proceed
        """
        # Check if this statement has a special execution mode configured
        execution_mode = statement.statement_config.execution_mode

        if execution_mode == "copy":
            return self._handle_copy_execution(cursor, statement)

        # Check for automatic COPY detection if no execution_mode is set
        if statement.operation_type == "COPY":
            return self._handle_copy_execution(cursor, statement)

        return None

    def _handle_copy_execution(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Handle COPY operations using psycopg's copy context manager.

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement with COPY operation

        Returns:
            SQLResult for the COPY operation
        """
        # Get compiled SQL (but COPY commands typically don't have parameters in the SQL itself)
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        execution_args = statement.statement_config.execution_args or {}
        data_param = execution_args.get("data_param", 0)  # Default to first parameter

        # Extract COPY data from parameters - use original parameters for data extraction
        copy_data = None
        if statement.parameters:
            if isinstance(data_param, str):
                # Named parameter
                if isinstance(statement.parameters, dict) and data_param in statement.parameters:
                    copy_data = statement.parameters[data_param]
            elif isinstance(data_param, int):
                # Positional parameter
                if isinstance(statement.parameters, (list, tuple)) and len(statement.parameters) > data_param:
                    copy_data = statement.parameters[data_param]
                elif not isinstance(statement.parameters, (dict, list, tuple)):
                    # Single parameter
                    copy_data = statement.parameters

        # Execute COPY operation using compiled SQL
        if copy_data is not None:
            result = self._execute_copy_with_data(cursor, sql, str(copy_data))
        else:
            result = self._execute_copy_without_data(cursor, sql)

        row_count = result.rowcount if result.rowcount is not None else -1
        execution_result = self.create_execution_result(result, rowcount_override=row_count)
        return self.build_statement_result(statement, execution_result)

    def _execute_copy_with_data(self, cursor: Any, sql_text: str, data_str: str) -> Any:
        """Execute COPY operation with data using Psycopg sync context manager."""
        with cursor.copy(sql_text) as copy:
            copy.write(data_str)
        return cursor

    def _execute_copy_without_data(self, cursor: Any, sql_text: str) -> Any:
        """Execute COPY operation without data using Psycopg sync context manager."""
        with cursor.copy(sql_text):
            pass
        return cursor

    def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        Psycopg doesn't have executescript but supports parameters in all statements.
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)

        last_result = None
        for stmt in statements:
            # Only pass parameters if they exist - psycopg treats empty containers as parameterized mode
            last_result = cursor.execute(stmt, prepared_parameters) if prepared_parameters else cursor.execute(stmt)

        return self.create_execution_result(
            last_result, statement_count=statement_count, successful_statements=statement_count, is_script_result=True
        )

    def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Psycopg executemany."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        result = cursor.executemany(sql, prepared_parameters)

        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(result, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement using Psycopg execute."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        # Only pass parameters if they exist - psycopg treats empty containers as parameterized mode
        result = cursor.execute(sql, prepared_parameters) if prepared_parameters else cursor.execute(sql)

        if statement.returns_rows():
            fetched_data = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            return self.create_execution_result(
                result,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(result, rowcount_override=row_count)

    def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        self.connection.commit()

    def handle_database_exceptions(self) -> "Generator[None, None, None]":
        """Handle Psycopg-specific exceptions and wrap them appropriately."""
        return cast("Generator[None, None, None]", self._handle_database_exceptions_impl())

    @contextmanager
    def _handle_database_exceptions_impl(self) -> "Generator[None, None, None]":
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except psycopg.Error as e:
            msg = f"Psycopg database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e


class PsycopgAsyncDriver(AsyncDriverAdapterBase):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgAsyncConnection,
        statement_config: Optional[StatementConfig] = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or psycopg_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: "PsycopgAsyncConnection") -> "PsycopgAsyncCursor":
        return PsycopgAsyncCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL-specific special operations.

        Checks for execution_mode configuration to handle special operations like COPY.

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if the special operation was handled and completed,
            None if standard execution should proceed
        """
        # Check if this statement has a special execution mode configured
        execution_mode = statement.statement_config.execution_mode

        if execution_mode == "copy":
            return await self._handle_copy_execution(cursor, statement)

        # Check for automatic COPY detection if no execution_mode is set
        if statement.operation_type == "COPY":
            return await self._handle_copy_execution(cursor, statement)

        return None

    async def _handle_copy_execution(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Handle COPY operations using psycopg's async copy context manager.

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement with COPY operation

        Returns:
            SQLResult for the COPY operation
        """
        # Get compiled SQL (but COPY commands typically don't have parameters in the SQL itself)
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        execution_args = statement.statement_config.execution_args or {}
        data_param = execution_args.get("data_param", 0)  # Default to first parameter

        # Extract COPY data from parameters - use original parameters for data extraction
        copy_data = None
        if statement.parameters:
            if isinstance(data_param, str):
                # Named parameter
                if isinstance(statement.parameters, dict) and data_param in statement.parameters:
                    copy_data = statement.parameters[data_param]
            elif isinstance(data_param, int):
                # Positional parameter
                if isinstance(statement.parameters, (list, tuple)) and len(statement.parameters) > data_param:
                    copy_data = statement.parameters[data_param]
                elif not isinstance(statement.parameters, (dict, list, tuple)):
                    # Single parameter
                    copy_data = statement.parameters

        # Execute COPY operation using compiled SQL
        if copy_data is not None:
            result = await self._execute_copy_with_data(cursor, sql, str(copy_data))
        else:
            result = await self._execute_copy_without_data(cursor, sql)

        row_count = result.rowcount if result.rowcount is not None else -1
        execution_result = self.create_execution_result(result, rowcount_override=row_count)
        return self.build_statement_result(statement, execution_result)

    async def _execute_copy_with_data(self, cursor: Any, sql_text: str, data_str: str) -> Any:
        """Execute COPY operation with data using Psycopg async context manager."""
        async with cursor.copy(sql_text) as copy:
            await copy.write(data_str)
        return cursor

    async def _execute_copy_without_data(self, cursor: Any, sql_text: str) -> Any:
        """Execute COPY operation without data using Psycopg async context manager."""
        async with cursor.copy(sql_text):
            pass
        return cursor

    async def _execute_script(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        Psycopg doesn't have executescript but supports parameters in all statements.
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        for stmt in statements:
            # Only pass parameters if they exist - psycopg treats empty containers as parameterized mode
            if prepared_parameters:
                await cursor.execute(stmt, prepared_parameters)
            else:
                await cursor.execute(stmt)

        return self.create_execution_result(
            cursor,
            statement_count=len(statements),
            successful_statements=len(statements),  # Assume all successful if no exception
            is_script_result=True,
        )

    async def _execute_many(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Psycopg executemany."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        result = await cursor.executemany(sql, prepared_parameters)

        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(result, rowcount_override=row_count, is_many_result=True)

    async def _execute_statement(self, cursor: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement using Psycopg execute."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        # Only pass parameters if they exist - psycopg treats empty containers as parameterized mode
        if prepared_parameters:
            result = await cursor.execute(sql, prepared_parameters)
        else:
            result = await cursor.execute(sql)

        if statement.returns_rows():
            fetched_data = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            return self.create_execution_result(
                result,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(result, rowcount_override=row_count)

    async def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        await self.connection.commit()

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle Psycopg-specific exceptions and wrap them appropriately."""
        return self._handle_database_exceptions_impl()

    @asynccontextmanager
    async def _handle_database_exceptions_impl(self) -> "AsyncGenerator[None, None]":
        """Handle Psycopg-specific exceptions and wrap them appropriately."""
        try:
            yield
        except psycopg.Error as e:
            msg = f"Psycopg database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
