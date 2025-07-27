# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import re
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Final, Optional, Union

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from asyncpg import Connection as AsyncpgNativeConnection
    from asyncpg import Record
    from asyncpg.pool import PoolConnectionProxy
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL, SQLConfig


__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = get_logger("adapters.asyncpg")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection = Any

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


class AsyncpgDriver(AsyncDriverAdapterBase):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "postgres"
    parameter_config = DriverParameterConfig(
        supported_parameter_styles=[ParameterStyle.NUMERIC],  # Only supports $1, $2, etc.
        default_parameter_style=ParameterStyle.NUMERIC,
        type_coercion_map={
            # AsyncPG has excellent native type support
            # Most types are handled natively
        },
        has_native_list_expansion=True,  # AsyncPG handles lists natively
    )

    def __init__(self, connection: "AsyncpgConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    @asynccontextmanager
    async def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncGenerator[Any, None]":
        yield connection

    async def _perform_execute(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)

        if statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            # asyncpg uses *args for parameters
            await cursor.execute(sql, *(prepared_params or []))

    async def _extract_select_data(self, cursor: "AsyncpgConnection") -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution.

        Note: asyncpg requires a separate fetch() call after execute().
        """
        # For asyncpg, we need to fetch after execute
        # The statement is already compiled and executed in _perform_execute
        # We need to get the last executed SQL and params to fetch
        # This is a limitation of asyncpg's API

        # Workaround: Store the SQL/params during _perform_execute
        # For now, we'll re-compile to get SQL and params
        if hasattr(self, "_last_statement"):
            statement = self._last_statement
            sql, params = statement.compile(placeholder_style=self.parameter_config.default_parameter_style)
            prepared_params = self._prepare_driver_parameters(params)
            records = await cursor.fetch(sql, *(prepared_params or []))
        else:
            # Fallback - this shouldn't happen in normal flow
            records = []

        data = [dict(record) for record in records]
        column_names = list(records[0].keys()) if records else []
        return data, column_names, len(records)

    def _extract_execute_rowcount(self, cursor: "AsyncpgConnection") -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        # Get status from the last executed command
        status = cursor._protocol.get_last_status() if hasattr(cursor, "_protocol") else "UNKNOWN 0"
        return self._parse_asyncpg_status(status)

    async def _build_result(self, cursor: "AsyncpgConnection", statement: "SQL") -> "SQLResult":
        """Build result - override to handle asyncpg's special needs."""
        # Store statement for _extract_select_data
        self._last_statement = statement
        try:
            return await super()._build_result(cursor, statement)
        finally:
            # Clean up
            if hasattr(self, "_last_statement"):
                delattr(self, "_last_statement")

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
