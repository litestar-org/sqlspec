# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import re
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING, Any, Final, Optional

import asyncpg

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.exceptions import SQLParsingError, SQLSpecError
from sqlspec.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._types import AsyncpgConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, StatementConfig

from sqlspec.statement.sql import StatementConfig

asyncpg_statement_config = StatementConfig(
    dialect="postgres",
    pre_process_steps=None,
    post_process_steps=None,
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC},
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
    ),
)

__all__ = ("AsyncpgCursor", "AsyncpgDriver", "asyncpg_statement_config")

logger = get_logger("adapters.asyncpg")


ASYNC_PG_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

EXPECTED_REGEX_GROUPS: Final[int] = 3

OPERATION_MAP: Final[dict[str, str]] = {
    "INSERT": "INSERT",
    "UPDATE": "UPDATE",
    "DELETE": "DELETE",
    "SELECT": "SELECT",
    "COPY": "COPY",
}


class AsyncpgCursor:
    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "AsyncpgConnection":
        return self.connection

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


class AsyncpgDriver(AsyncDriverAdapterBase):
    """AsyncPG PostgreSQL driver adapter with hook-based implementation."""

    dialect = "postgres"

    def __init__(
        self,
        connection: "AsyncpgConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config or asyncpg_statement_config,
            driver_features=driver_features,
        )

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        return AsyncpgCursor(connection)

    async def _try_special_handling(self, cursor: "AsyncpgConnection", statement: "SQL") -> "Optional[SQLResult]":
        """Handle PostgreSQL COPY operations."""
        if statement.operation_type == "COPY":
            await self._handle_copy_operation(cursor, statement)
            return self.build_statement_result(statement, self.create_execution_result(cursor))

        return None

    async def _execute_many(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute multiple statements with AsyncPG."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        await cursor.executemany(sql, prepared_parameters)

        return self.create_execution_result(
            cursor, rowcount_override=len(prepared_parameters) if prepared_parameters else 0, is_many_result=True
        )

    async def _execute_statement(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute single statement with AsyncPG."""
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        if statement.returns_rows():
            records = await cursor.fetch(sql, *prepared_parameters)
            data = [dict(record) for record in records]
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=list(records[0].keys()) if records else [],
                data_row_count=len(records),
                is_select_result=True,
            )

        result = await cursor.execute(sql, *prepared_parameters)
        return self.create_execution_result(
            result, rowcount_override=self._parse_asyncpg_status(result) if isinstance(result, str) else 0
        )

    async def _handle_copy_operation(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations.

        Args:
            cursor: Database connection
            statement: SQL statement with COPY operation
        """
        metadata: dict[str, Any] = {}
        # Use processed SQL with all transformations applied
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

    @staticmethod
    def _parse_asyncpg_status(status: str) -> int:
        """Parse AsyncPG status string to extract row count.

        Args:
            status: Status string like "INSERT 0 1", "UPDATE 3", "DELETE 2"

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
        """Begin transaction using asyncpg-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using asyncpg-specific method."""
        await self.connection.execute("ROLLBACK")

    async def commit(self) -> None:
        """Commit transaction using asyncpg-specific method."""
        await self.connection.execute("COMMIT")

    def handle_database_exceptions(self) -> "AbstractAsyncContextManager[None]":
        """Handle AsyncPG-specific exceptions and wrap them appropriately."""
        return self._handle_database_exceptions_async()

    @asynccontextmanager
    async def _handle_database_exceptions_async(self) -> Any:
        try:
            yield
        except asyncpg.PostgresError as e:
            msg = f"AsyncPG database error: {e}"
            raise SQLSpecError(msg) from e
        except Exception as e:
            # Handle any other unexpected errors
            if "parse" in str(e).lower() or "syntax" in str(e).lower():
                msg = f"SQL parsing failed: {e}"
                raise SQLParsingError(msg) from e
            msg = f"Unexpected error: {e}"
            raise SQLSpecError(msg) from e
