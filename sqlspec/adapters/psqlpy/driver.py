"""Psqlpy Driver Implementation."""

import logging
import re
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, Final, Optional

import psqlpy

from sqlspec.adapters.psqlpy._types import PsqlpyConnection
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

logger = logging.getLogger("sqlspec.adapters.psqlpy")

psqlpy_statement_config = StatementConfig(
    dialect="postgres",
    pre_process_steps=None,
    post_process_steps=None,
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC, ParameterStyle.NAMED_DOLLAR, ParameterStyle.QMARK},
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        type_coercion_map={
            list: lambda x: x,
            tuple: list,
            dict: to_json,
            bool: lambda x: x,
            int: lambda x: x,
            float: lambda x: x,
            str: lambda x: x,
            bytes: lambda x: x,
            type(None): lambda _: None,
        },
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
    ),
)

PSQLPY_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "psqlpy_statement_config")


class PsqlpyCursor:
    """Psqlpy cursor context manager for consistent interface.

    Provides proper lifecycle management and connection cleanup.
    """

    def __init__(self, connection: "PsqlpyConnection") -> None:
        self.connection = connection
        self._transaction = None
        self._in_use = False

    async def __aenter__(self) -> "PsqlpyConnection":
        """Enter cursor context with proper lifecycle tracking."""
        self._in_use = True
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit cursor context with proper cleanup."""
        self._in_use = False
        if hasattr(self.connection, "back_to_pool"):
            try:
                if not self._transaction:
                    self.connection.back_to_pool()
            except Exception as e:
                logger.debug("Failed to return psqlpy connection to pool: %s", e)

    def is_in_use(self) -> bool:
        """Check if cursor is currently in use."""
        return self._in_use


class PsqlpyDriver(AsyncDriverAdapterBase):
    """Psqlpy Driver Adapter."""

    dialect: "DialectType" = "postgres"
    connection: "PsqlpyConnection"

    def __init__(
        self,
        connection: PsqlpyConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or psqlpy_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: PsqlpyConnection) -> "PsqlpyCursor":
        return PsqlpyCursor(connection)

    async def _try_special_handling(self, cursor: PsqlpyConnection, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for psqlpy-specific special operations.

        Psqlpy has some specific optimizations we could leverage:
        - Native transaction management
        - Batch execution for scripts
        - Cursor-based iteration for large result sets

        For now, we proceed with standard execution but this could be
        expanded for psqlpy-specific optimizations.
        """
        return None

    async def _execute_many(self, cursor: PsqlpyConnection, statement: "SQL") -> "ExecutionResult":
        """Psqlpy execute_many implementation with proper parameter format.

        Psqlpy expects parameters as a list of lists/sequences for execute_many.
        """
        sql = statement.sql
        prepared_parameters = statement.parameters
        if not prepared_parameters:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        formatted_parameters = [
            list(param_set) if isinstance(param_set, (list, tuple)) else [param_set]
            for param_set in prepared_parameters
        ]

        await cursor.execute_many(sql, formatted_parameters)
        return self.create_execution_result(cursor, rowcount_override=len(formatted_parameters), is_many_result=True)

    async def _execute_script(self, cursor: PsqlpyConnection, statement: "SQL") -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements sequentially.

        Psqlpy supports execute_batch for multi-statement scripts.
        """
        sql = statement.sql
        prepared_parameters = statement.parameters
        statement_config = statement.statement_config
        if not prepared_parameters:
            try:
                await cursor.execute_batch(sql)
                statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
                statement_count = len(statements)
                return self.create_execution_result(
                    cursor,
                    statement_count=statement_count,
                    successful_statements=statement_count,
                    is_script_result=True,
                )
            except Exception as e:
                logger.debug("psqlpy execute_batch failed, falling back to individual statements: %s", e)

        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)

        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_parameters or [])

        return self.create_execution_result(
            last_result, statement_count=statement_count, successful_statements=statement_count, is_script_result=True
        )

    async def _execute_statement(self, cursor: PsqlpyConnection, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement using psqlpy-optimized approach."""
        sql = statement.sql
        prepared_parameters = statement.parameters
        if statement.returns_rows():
            query_result = await cursor.fetch(sql, prepared_parameters or [])
            dict_rows: list[dict[str, Any]] = query_result.result() if query_result else []

            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=list(dict_rows[0].keys()) if dict_rows else [],
                data_row_count=len(dict_rows),
                is_select_result=True,
            )

        result = await cursor.execute(sql, prepared_parameters or [])
        return self.create_execution_result(cursor, rowcount_override=self._extract_rows_affected(result))

    def _extract_rows_affected(self, result: Any) -> int:
        """Extract rows affected from psqlpy result.

        Psqlpy may return command tag information that we can parse.
        """
        try:
            if hasattr(result, "tag") and result.tag:
                return self._parse_command_tag(result.tag)
            if hasattr(result, "status") and result.status:
                return self._parse_command_tag(result.status)
            if isinstance(result, str):
                return self._parse_command_tag(result)
        except Exception as e:
            logger.debug("Failed to parse psqlpy command tag: %s", e)
        return -1

    def _parse_command_tag(self, tag: str) -> int:
        """Parse PostgreSQL command tag to extract rows affected.

        Examples: 'INSERT 0 1', 'UPDATE 5', 'DELETE 3'
        """
        if not tag:
            return -1

        match = PSQLPY_STATUS_REGEX.match(tag.strip())
        if match:
            command = match.group(1).upper()
            if command == "INSERT" and match.group(3):
                return int(match.group(3))
            if command in {"UPDATE", "DELETE"} and match.group(3):
                return int(match.group(3))
        return -1

    async def begin(self) -> None:
        """Begin transaction using psqlpy native transaction method."""
        if not hasattr(self, "_transaction"):
            self._transaction = self.connection.transaction()
            await self._transaction.begin()
        else:
            await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psqlpy native method."""
        if hasattr(self, "_transaction") and self._transaction:
            await self._transaction.rollback()
            delattr(self, "_transaction")
        else:
            await self.connection.execute("ROLLBACK")

    async def commit(self) -> None:
        """Commit transaction using psqlpy native method."""
        if hasattr(self, "_transaction") and self._transaction:
            await self._transaction.commit()
            delattr(self, "_transaction")
        else:
            await self.connection.execute("COMMIT")

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle Psqlpy-specific exceptions and wrap them appropriately."""
        return self._handle_database_exceptions_impl()

    @asynccontextmanager
    async def _handle_database_exceptions_impl(self) -> Any:
        """Implementation of database exception handling without decorator."""
        try:
            yield
        except psqlpy.exceptions.DatabaseError as e:
            msg = f"Psqlpy database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
