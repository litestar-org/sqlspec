# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import re
from typing import TYPE_CHECKING, Any, Final, Optional

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlspec.adapters.asyncpg._types import AsyncpgConnection
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, StatementConfig


__all__ = ("AsyncpgCursor", "AsyncpgDriver")

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
        from sqlspec.statement.sql import StatementConfig

        # Set default AsyncPG-specific configuration
        if statement_config is None:
            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.NUMERIC,
                supported_parameter_styles={ParameterStyle.NUMERIC},
                type_coercion_map={},  # AsyncPG handles most types natively
                has_native_list_expansion=True,
                needs_static_script_compilation=False,  # AsyncPG supports parameters in scripts
            )
            statement_config = StatementConfig(dialect="postgres", parameter_config=parameter_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        return AsyncpgCursor(connection)

    async def _try_special_handling(
        self, cursor: "AsyncpgConnection", statement: "SQL"
    ) -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for PostgreSQL COPY operations and row-returning queries with pre-fetching."""
        # Handle COPY operations
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            await self._handle_copy_operation_from_pipeline(cursor, statement)
            from sqlspec.driver._common import create_execution_result

            return create_execution_result(cursor)

        # Handle row-returning queries with pre-fetching for AsyncPG
        if statement.returns_rows() and not statement.is_script and not statement.is_many:
            # For AsyncPG, fetch data immediately since we need separate fetch() call
            sql, params = statement.compile()
            prepared_params = self.prepare_driver_parameters(params, self.statement_config, is_many=False)

            records = await cursor.fetch(sql, *prepared_params)
            data = [dict(record) for record in records]
            column_names = list(records[0].keys()) if records else []

            # Package the pre-fetched data as special_data
            special_data = (data, column_names, len(records))
            from sqlspec.driver._common import create_execution_result

            return create_execution_result(cursor, None, special_data)

        return None

    async def _execute_many(self, cursor: "AsyncpgConnection", sql: str, prepared_params: Any) -> Any:
        """AsyncPG executemany with row count approximation."""
        await cursor.executemany(sql, prepared_params)
        # AsyncPG doesn't provide exact row count for executemany
        return len(prepared_params) if prepared_params else 0

    async def _execute_statement(self, cursor: "AsyncpgConnection", sql: str, prepared_params: Any) -> Any:
        """AsyncPG single execution for non-row-returning queries."""
        # This hook only handles non-row-returning queries since row-returning
        # queries are handled by _try_special_handling
        return await cursor.execute(sql, *prepared_params)

    async def _get_selected_data(self, cursor: "AsyncpgConnection") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Note: For AsyncPG, data is pre-fetched in _try_special_handling,
        so this method should not be called.
        """
        # This should not be called for AsyncPG since we pre-fetch in _try_special_handling
        return [], [], 0

    def _get_row_count(self, cursor: "AsyncpgConnection") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE.

        For AsyncPG, the row count is parsed from the execution status.
        """
        # This method is called by the base class after _execute_statement
        # The actual status is handled in the execution result
        return 0

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

    async def _build_result(
        self, cursor: "AsyncpgConnection", statement: "SQL", execution_result: "tuple[Any, Optional[int], Any]"
    ) -> "SQLResult":
        """Build result with AsyncPG-specific handling for pre-fetched data."""
        cursor_result, rowcount_override, special_data = execution_result

        # Handle regular operations
        if statement.returns_rows():
            if special_data and isinstance(special_data, tuple) and len(special_data) == 3:
                data, column_names, row_count = special_data
                return self._build_select_result_from_data(
                    statement=statement, data=data, column_names=column_names, row_count=row_count
                )

        # For non-row-returning queries, use status parsing for row count
        if rowcount_override is not None:
            row_count = rowcount_override
        elif isinstance(cursor_result, str):
            row_count = self._parse_asyncpg_status(cursor_result)
        elif isinstance(cursor_result, int):
            row_count = cursor_result
        else:
            row_count = 0

        return self._build_execute_result_from_data(statement=statement, row_count=row_count)

    @staticmethod
    def _parse_asyncpg_status(status: str) -> int:
        """Parse AsyncPG status string to extract row count.

        Args:
            status: Status string like "INSERT 0 1", "UPDATE 3", "DELETE 2", or "EXECUTE_MANY 5"

        Returns:
            Number of affected rows, or 0 if cannot parse
        """
        if not status:
            return 0

        # Handle our custom EXECUTE_MANY status format
        if status.startswith("EXECUTE_MANY "):
            try:
                return int(status.rsplit(maxsplit=1)[-1])
            except (ValueError, IndexError):
                return 0

        match = ASYNC_PG_STATUS_REGEX.match(status.strip())
        if match:
            # For INSERT: "INSERT 0 5" -> groups: (INSERT, 0, 5)
            # For UPDATE/DELETE: "UPDATE 3" -> groups: (UPDATE, None, 3)
            groups = match.groups()
            if len(groups) >= EXPECTED_REGEX_GROUPS:
                try:
                    # The last group is always the row count
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
