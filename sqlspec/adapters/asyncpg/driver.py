"""AsyncPG PostgreSQL driver implementation for async PostgreSQL operations.

Provides async PostgreSQL connectivity with parameter processing, resource management,
PostgreSQL COPY operation support, and transaction management.
"""

import re
from typing import TYPE_CHECKING, Any, Final, Optional

import asyncpg

from sqlspec.core.cache import get_cache_config
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from contextlib import AbstractAsyncContextManager

    from sqlspec.adapters.asyncpg._types import AsyncpgConnection
    from sqlspec.core.result import SQLResult
    from sqlspec.core.statement import SQL
    from sqlspec.driver import ExecutionResult

__all__ = ("AsyncpgCursor", "AsyncpgDriver", "AsyncpgExceptionHandler", "asyncpg_statement_config")

logger = get_logger("adapters.asyncpg")


asyncpg_statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.POSITIONAL_PYFORMAT},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
        preserve_parameter_format=True,
    ),
    enable_parsing=True,
    enable_validation=True,
    enable_caching=True,
    enable_parameter_type_wrapping=True,
)


ASYNC_PG_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)
EXPECTED_REGEX_GROUPS: Final[int] = 3


class AsyncpgCursor:
    """Context manager for AsyncPG cursor management."""

    __slots__ = ("connection",)

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "AsyncpgConnection":
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)


class AsyncpgExceptionHandler:
    """Async context manager for handling AsyncPG database exceptions."""

    __slots__ = ()

    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is None:
            return
        if issubclass(exc_type, asyncpg.PostgresError):
            e = exc_val
            error_code = getattr(e, "sqlstate", None)
            if error_code:
                if error_code.startswith("23"):
                    msg = f"PostgreSQL integrity constraint violation [{error_code}]: {e}"
                elif error_code.startswith("42"):
                    msg = f"PostgreSQL SQL syntax error [{error_code}]: {e}"
                    raise SQLParsingError(msg) from e
                elif error_code.startswith("08"):
                    msg = f"PostgreSQL connection error [{error_code}]: {e}"
                else:
                    msg = f"PostgreSQL database error [{error_code}]: {e}"
            else:
                msg = f"PostgreSQL database error: {e}"
            raise SQLSpecError(msg) from e


class AsyncpgDriver(AsyncDriverAdapterBase):
    """AsyncPG PostgreSQL driver for async database operations.

    Supports COPY operations, numeric parameter style handling, PostgreSQL
    exception handling, transaction management, SQL statement compilation
    and caching, and parameter processing with type coercion.
    """

    __slots__ = ()
    dialect = "postgres"

    def __init__(
        self,
        connection: "AsyncpgConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            cache_config = get_cache_config()
            statement_config = asyncpg_statement_config.replace(
                enable_caching=cache_config.compiled_cache_enabled,
                enable_parsing=True,
                enable_validation=True,
                dialect="postgres",
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        """Create context manager for AsyncPG cursor."""
        return AsyncpgCursor(connection)

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle database exceptions with PostgreSQL error codes."""
        return AsyncpgExceptionHandler()

    async def _try_special_handling(self, cursor: "AsyncpgConnection", statement: "SQL") -> "Optional[SQLResult]":
        """Handle PostgreSQL COPY operations and other special cases.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        if statement.operation_type == "COPY":
            await self._handle_copy_operation(cursor, statement)
            return self.build_statement_result(statement, self.create_execution_result(cursor))

        return None

    async def _handle_copy_operation(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations.

        Supports both COPY FROM STDIN and COPY TO STDOUT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with COPY operation
        """

        metadata: dict[str, Any] = getattr(statement, "metadata", {})
        sql_text = statement.sql

        copy_data = metadata.get("postgres_copy_data")

        if copy_data:
            if isinstance(copy_data, dict):
                data_str = (
                    str(next(iter(copy_data.values())))
                    if len(copy_data) == 1
                    else "\n".join(str(value) for value in copy_data.values())
                )
            elif isinstance(copy_data, (list, tuple)):
                data_str = str(copy_data[0]) if len(copy_data) == 1 else "\n".join(str(value) for value in copy_data)
            else:
                data_str = str(copy_data)

            if "FROM STDIN" in sql_text.upper():
                from io import BytesIO

                data_io = BytesIO(data_str.encode("utf-8"))
                await cursor.copy_from_query(sql_text, output=data_io)
            else:
                await cursor.execute(sql_text)
        else:
            await cursor.execute(sql_text)

    async def _execute_script(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None

        for stmt in statements:
            result = await cursor.execute(stmt)
            last_result = result
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def _execute_many(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using AsyncPG's executemany.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            await cursor.executemany(sql, prepared_parameters)

            affected_rows = len(prepared_parameters)
        else:
            affected_rows = 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def _execute_statement(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Handles both SELECT queries and non-SELECT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if statement.returns_rows():
            records = await cursor.fetch(sql, *prepared_parameters) if prepared_parameters else await cursor.fetch(sql)

            data = [dict(record) for record in records]
            column_names = list(records[0].keys()) if records else []

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        result = await cursor.execute(sql, *prepared_parameters) if prepared_parameters else await cursor.execute(sql)

        affected_rows = self._parse_asyncpg_status(result) if isinstance(result, str) else 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    @staticmethod
    def _parse_asyncpg_status(status: str) -> int:
        """Parse AsyncPG status string to extract row count.

        AsyncPG returns status strings like "INSERT 0 1", "UPDATE 3", "DELETE 2"
        for non-SELECT operations. This method extracts the affected row count.

        Args:
            status: Status string from AsyncPG operation

        Returns:
            Number of affected rows, or 0 if cannot parse
        """
        if not status:
            return 0

        match = ASYNC_PG_STATUS_REGEX.match(status.strip())
        if match:
            groups = match.groups()
            if len(groups) >= EXPECTED_REGEX_GROUPS:
                try:
                    return int(groups[-1])
                except (ValueError, IndexError):
                    pass

        return 0

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            await self.connection.execute("BEGIN")
        except asyncpg.PostgresError as e:
            msg = f"Failed to begin async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.execute("ROLLBACK")
        except asyncpg.PostgresError as e:
            msg = f"Failed to rollback async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.execute("COMMIT")
        except asyncpg.PostgresError as e:
            msg = f"Failed to commit async transaction: {e}"
            raise SQLSpecError(msg) from e
