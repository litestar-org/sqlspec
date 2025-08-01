# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import re
from typing import TYPE_CHECKING, Any, Final, Optional

from sqlspec.adapters.asyncpg.pipeline_steps import postgres_copy_pipeline_step
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._types import AsyncpgConnection
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, StatementConfig

# Import StatementConfig at runtime for the shared config
from sqlspec.statement.sql import StatementConfig

# Shared AsyncPG statement configuration
asyncpg_statement_config = StatementConfig(
    dialect="postgres",
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={ParameterStyle.NUMERIC},
        type_coercion_map={},
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
    ),
    custom_pipeline_steps=[postgres_copy_pipeline_step],
)

__all__ = ("AsyncpgCursor", "AsyncpgDriver", "asyncpg_statement_config")

logger = get_logger("adapters.asyncpg")


# Compiled regex to parse asyncpg status messages like "INSERT 0 1" or "UPDATE 1"
# Group 1: Command Tag (e.g., INSERT, UPDATE)
# Group 2: (Optional) OID count for INSERT (we ignore this)
# Group 3: Rows affected
ASYNC_PG_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

# Expected number of groups in the regex match for row count extraction
EXPECTED_REGEX_GROUPS: Final[int] = 3

# Operation mapping for fast lookup
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
    """AsyncPG PostgreSQL Driver Adapter. Clean hook-based implementation with no state management."""

    dialect = "postgres"

    def __init__(
        self,
        connection: "AsyncpgConnection",
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            statement_config = asyncpg_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        return AsyncpgCursor(connection)

    async def _try_special_handling(self, cursor: "AsyncpgConnection", statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL COPY operations only."""
        # Handle COPY operations
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            await self._handle_copy_operation_from_pipeline(cursor, statement)

            # Create ExecutionResult and build SQLResult directly
            execution_result = self.create_execution_result(cursor)
            return self.build_statement_result(statement, execution_result)

        # Return None to let standard execution flow handle all other queries
        return None

    async def _execute_many(
        self, cursor: "AsyncpgConnection", sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """AsyncPG executemany with row count approximation."""
        await cursor.executemany(sql, prepared_params)

        # AsyncPG doesn't provide exact row count for executemany, approximate using parameter count
        row_count = len(prepared_params) if prepared_params else 0

        return self.create_execution_result(cursor, rowcount_override=row_count, is_many_result=True)

    async def _execute_statement(
        self, cursor: "AsyncpgConnection", sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """AsyncPG single execution using AsyncPG-optimized approach."""
        if statement.returns_rows():
            # Use fetch() for row-returning queries
            records = await cursor.fetch(sql, *prepared_params)
            data = [dict(record) for record in records]
            column_names = list(records[0].keys()) if records else []
            return self.create_execution_result(
                cursor,
                selected_data=data,
                column_names=column_names,
                data_row_count=len(records),
                is_select_result=True,
            )

        # Use execute() for non-row-returning queries
        result = await cursor.execute(sql, *prepared_params)
        row_count = self._parse_asyncpg_status(result) if isinstance(result, str) else 0
        return self.create_execution_result(result, rowcount_override=row_count)

    async def _handle_copy_operation_from_pipeline(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations using pipeline metadata.

        Args:
            cursor: Database connection (AsyncPG doesn't use cursors)
            statement: SQL statement with COPY metadata from pipeline
        """
        # Get the original SQL from pipeline metadata
        metadata = statement._processing_context.metadata if statement._processing_context else {}
        sql_text = metadata.get("postgres_copy_original_sql")
        if not sql_text:
            # Fallback to expression
            sql_text = str(statement.expression)

        # Get the raw COPY data from pipeline metadata
        copy_data = metadata.get("postgres_copy_data")

        # For COPY operations, parameters contain the data to be copied, not SQL parameters
        if copy_data:
            # Handle different parameter formats (positional or keyword)
            if isinstance(copy_data, dict):
                # For named parameters, assume single data value or concatenate all values
                if len(copy_data) == 1:
                    data_str = str(next(iter(copy_data.values())))
                else:
                    data_str = "\n".join(str(value) for value in copy_data.values())
            elif isinstance(copy_data, (list, tuple)):
                # For positional parameters, if single item, use as is, otherwise join
                data_str = str(copy_data[0]) if len(copy_data) == 1 else "\n".join(str(value) for value in copy_data)
            else:
                data_str = str(copy_data)

            # Use AsyncPG's COPY FROM STDIN for data input operations
            # Parse the COPY statement to determine if it's FROM STDIN
            if "FROM STDIN" in sql_text.upper():
                # Use copy_from_query for COPY FROM STDIN operations
                from io import BytesIO

                data_io = BytesIO(data_str.encode("utf-8"))
                await cursor.copy_from_query(sql_text, output=data_io)
            else:
                # For other COPY operations, use execute() with the raw SQL
                await cursor.execute(sql_text)
        else:
            # COPY without data (e.g., COPY TO STDOUT)
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
