# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import re
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.adapters.asyncpg._types import AsyncpgConnection
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, SQLConfig


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
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "postgres"
    parameter_config: DriverParameterConfig
    _execution_state: dict[str, "Optional[Union[str, Any]]"]

    def __init__(self, connection: "AsyncpgConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=[ParameterStyle.NUMERIC],
            default_parameter_style=ParameterStyle.NUMERIC,
            type_coercion_map={},
            has_native_list_expansion=True,
            force_style_conversion=True,
        )
        self._execution_state = {"last_status": None, "compiled_sql": None, "prepared_params": None}

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        return AsyncpgCursor(connection)

    async def _perform_execute(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        sql, params = self._get_compiled_sql(statement, self.parameter_config.default_parameter_style)

        # Check if this is a COPY statement marked by the pipeline
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            await self._handle_copy_operation_from_pipeline(cursor, statement)
            return

        if statement.is_script:
            # AsyncPG doesn't have executescript - execute statements one by one
            # But we can still use parameters since we're using regular execute()
            prepared_params = self._prepare_driver_parameters(params)
            # Use the proper script splitter to handle complex cases
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            last_status = "UNKNOWN 0"
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    last_status = await cursor.execute(stmt, *(prepared_params or []))
            # Store the last statement's status for row count
            self._execution_state["last_status"] = last_status
        elif statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            # executemany doesn't return status in asyncpg, approximate row count
            await cursor.executemany(sql, prepared_params)
            # For executemany, we can't get exact row count from asyncpg
            # Store approximate count based on number of parameter sets
            row_count = len(prepared_params) if prepared_params else 0
            self._execution_state["last_status"] = f"EXECUTE_MANY {row_count}"
        else:
            prepared_params = self._prepare_driver_parameters(params)
            # For row-returning queries (including CTEs with INSERT...RETURNING),
            # we need to use fetch() directly instead of execute() to get the results
            if self.returns_rows(statement.expression):
                # Store params for _extract_select_data to use
                self._execution_state["prepared_params"] = prepared_params
                self._execution_state["compiled_sql"] = sql
                # For CTEs with RETURNING, we'll fetch the data in _extract_select_data
            else:
                # For non-row-returning queries, use execute() and store status
                status = await cursor.execute(sql, *(prepared_params or []))
                self._execution_state["last_status"] = status

    async def _extract_select_data(self, cursor: "AsyncpgConnection") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Note: asyncpg requires a separate fetch() call after execute().
        """
        # Use the compiled SQL and prepared params stored during _perform_execute
        # This avoids re-compilation and follows single-pass processing principle
        sql = self._execution_state.get("compiled_sql")
        prepared_params = self._execution_state.get("prepared_params")
        records: list[Any]
        if sql is not None:
            records = await cursor.fetch(sql, *(prepared_params or []))
        else:
            records = []

        data = [dict(record) for record in records]
        column_names = list(records[0].keys()) if records else []
        return data, column_names, len(records)

    def _extract_execute_rowcount(self, cursor: "AsyncpgConnection") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        # Use the status stored during _perform_execute
        status = self._execution_state.get("last_status", "UNKNOWN 0")
        return self._parse_asyncpg_status(status or "UNKNOWN 0")

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

    async def _build_result(self, cursor: "AsyncpgConnection", statement: "SQL") -> "SQLResult":
        """Build result - override to handle asyncpg's special cleanup needs."""
        try:
            return await super()._build_result(cursor, statement)
        finally:
            # Clean up stored state after result is built
            self._execution_state["compiled_sql"] = None
            self._execution_state["prepared_params"] = None
            self._execution_state["last_status"] = None

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
