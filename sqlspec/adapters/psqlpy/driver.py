"""Psqlpy Driver Implementation."""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

import logging
import re
from typing import TYPE_CHECKING, Any, Final, Optional

from sqlspec.adapters.psqlpy._types import PsqlpyConnection
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

logger = logging.getLogger("sqlspec.adapters.psqlpy")

# PostgreSQL type coercion map optimized for psqlpy
psqlpy_type_coercion_map = {
    # Convert Python lists/tuples to PostgreSQL arrays when needed
    list: lambda x: x,  # psqlpy handles arrays natively
    tuple: list,  # Convert tuples to lists
    # Convert dicts to JSON strings for PostgreSQL JSON/JSONB types
    dict: to_json,
    # Keep other types as-is since psqlpy handles them efficiently
    bool: lambda x: x,
    int: lambda x: x,
    float: lambda x: x,
    str: lambda x: x,
    bytes: lambda x: x,
    type(None): lambda _: None,
}

# Shared PsqlPy statement configuration following asyncpg and psycopg patterns
psqlpy_statement_config = StatementConfig(
    dialect="postgres",
    # Enhanced pipeline steps (can be extended for COPY support like psycopg)
    pre_process_steps=None,  # Future: Could add postgres_copy_pipeline_step
    post_process_steps=None,  # No post-processing needed for psqlpy
    # Core pipeline configuration
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    # Parameter processing configuration optimized for psqlpy
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.NUMERIC,
        supported_parameter_styles={
            ParameterStyle.NUMERIC,
            ParameterStyle.NAMED_DOLLAR,
            ParameterStyle.QMARK,  # Add support for ? placeholders
        },
        # Psqlpy supports NUMERIC natively (like asyncpg)
        supported_execution_parameter_styles={ParameterStyle.NUMERIC},
        default_execution_parameter_style=ParameterStyle.NUMERIC,
        type_coercion_map=psqlpy_type_coercion_map,
        has_native_list_expansion=True,
        needs_static_script_compilation=False,
    ),
)

# Regex pattern to extract row counts from psqlpy command tags
# Similar to asyncpg pattern: "INSERT 0 1", "UPDATE 5", "DELETE 3"
PSQLPY_STATUS_REGEX: Final[re.Pattern[str]] = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

__all__ = ("PsqlpyCursor", "PsqlpyDriver", "psqlpy_statement_config", "psqlpy_type_coercion_map")


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
        # Psqlpy connections are managed by the pool
        # Optionally return connection to pool if needed
        if hasattr(self.connection, "back_to_pool"):
            try:
                # Only return to pool if no active transactions
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
        # Set default psqlpy-specific configuration
        if statement_config is None:
            statement_config = psqlpy_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

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

    async def _execute_many(
        self, cursor: PsqlpyConnection, sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """Psqlpy execute_many implementation with proper parameter format.

        Psqlpy expects parameters as a list of lists/sequences for execute_many.
        """
        if not prepared_params:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)

        # Ensure parameters are in the correct format for psqlpy
        # prepared_params should be a list of parameter sets
        formatted_params = []
        for param_set in prepared_params:
            if isinstance(param_set, (list, tuple)):
                formatted_params.append(list(param_set))
            else:
                # Single parameter, wrap in list
                formatted_params.append([param_set])

        await cursor.execute_many(sql, formatted_params)

        # Calculate estimated row count for execute_many
        estimated_rows = len(formatted_params)
        return self.create_execution_result(cursor, rowcount_override=estimated_rows, is_many_result=True)

    async def _execute_script(
        self,
        cursor: PsqlpyConnection,
        sql: str,
        prepared_params: Any,
        statement_config: "StatementConfig",
        statement: "SQL",
    ) -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements sequentially.

        Psqlpy supports execute_batch for multi-statement scripts.
        """
        if not prepared_params:
            # Use psqlpy's native execute_batch for parameter-free scripts
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
                # Fall back to individual statement execution
                logger.debug("psqlpy execute_batch failed, falling back to individual statements: %s", e)

        # Fall back to individual statement execution for parameterized scripts
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)

        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_params or [])

        return self.create_execution_result(
            last_result, statement_count=statement_count, successful_statements=statement_count, is_script_result=True
        )

    async def _execute_statement(
        self, cursor: PsqlpyConnection, sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """Execute single SQL statement using psqlpy-optimized approach."""
        if statement.returns_rows():
            # Use fetch() for row-returning queries (SELECT, RETURNING, etc.)
            query_result = await cursor.fetch(sql, prepared_params or [])
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                dict_rows = query_result.result()

            column_names = list(dict_rows[0].keys()) if dict_rows else []
            return self.create_execution_result(
                cursor,
                selected_data=dict_rows,
                column_names=column_names,
                data_row_count=len(dict_rows),
                is_select_result=True,
            )

        # Use execute() for non-row-returning queries (INSERT, UPDATE, DELETE, etc.)
        result = await cursor.execute(sql, prepared_params or [])

        # Try to extract row count from psqlpy result if available
        rows_affected = self._extract_rows_affected(result)
        return self.create_execution_result(cursor, rowcount_override=rows_affected)

    def _extract_rows_affected(self, result: Any) -> int:
        """Extract rows affected from psqlpy result.

        Psqlpy may return command tag information that we can parse.
        """
        try:
            # If result has a status or tag attribute, try to parse it
            if hasattr(result, "tag") and result.tag:
                return self._parse_command_tag(result.tag)
            if hasattr(result, "status") and result.status:
                return self._parse_command_tag(result.status)
            # If result is a string command tag itself
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
            # For INSERT: group(2) is OID, group(3) is rows
            # For UPDATE/DELETE: group(2) is None, group(3) is rows
            if command == "INSERT" and match.group(3):
                return int(match.group(3))
            if command in {"UPDATE", "DELETE"} and match.group(3):
                return int(match.group(3))
        return -1

    async def begin(self) -> None:
        """Begin transaction using psqlpy native transaction method."""
        # Psqlpy prefers using connection.transaction() as a context manager
        # For manual transaction control, create transaction object and begin
        if not hasattr(self, "_transaction"):
            self._transaction = self.connection.transaction()
            await self._transaction.begin()
        else:
            # Fall back to SQL if transaction already exists
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
