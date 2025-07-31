# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import Any, Optional

from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.adapters.psycopg.mixins import PsycopgCopyMixin
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.statement.sql import SQL, StatementConfig

__all__ = ("PsycopgAsyncCursor", "PsycopgAsyncDriver", "PsycopgSyncCursor", "PsycopgSyncDriver")


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


class PsycopgSyncDriver(PsycopgCopyMixin, SyncDriverAdapterBase):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        # Set default Psycopg-specific configuration
        if statement_config is None:
            from sqlspec.parameters.config import ParameterStyleConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
                supported_parameter_styles={
                    ParameterStyle.POSITIONAL_PYFORMAT,  # %s
                    ParameterStyle.NAMED_PYFORMAT,  # %(name)s
                    ParameterStyle.NUMERIC,  # $1 (also supported!)
                },
                type_coercion_map={
                    # Psycopg handles most types natively
                    # Add any specific type mappings as needed
                },
                has_native_list_expansion=True,  # Psycopg handles lists/tuples natively
                needs_static_script_compilation=False,  # Psycopg supports parameters in scripts
            )
            statement_config = StatementConfig(dialect="postgres", parameter_config=parameter_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: PsycopgSyncConnection) -> PsycopgSyncCursor:
        return PsycopgSyncCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for PostgreSQL-specific special operations (COPY commands).

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement to analyze

        Returns:
            Tuple of (cursor_result, rowcount_override, special_data) if handled,
            None if standard execution should proceed
        """
        # Check if this is a COPY statement marked by the pipeline
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            result = self._handle_copy_operation_from_pipeline(cursor, statement)
            # Return execution result tuple - COPY operations typically don't return data
            from sqlspec.driver._common import create_execution_result

            return create_execution_result(result)

        return None

    def _execute_copy_with_data(self, cursor: Any, sql_text: str, data_str: str) -> Any:
        """Execute COPY operation with data using Psycopg sync context manager."""
        with cursor.copy(sql_text) as copy:
            copy.write(data_str)

    def _execute_copy_without_data(self, cursor: Any, sql_text: str) -> Any:
        """Execute COPY operation without data using Psycopg sync context manager."""
        with cursor.copy(sql_text):
            pass  # Just execute the COPY command

    def _execute_script(self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig") -> Any:
        """Execute SQL script by splitting and executing statements individually.

        Psycopg doesn't have executescript but supports parameters in all statements.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        last_result = None
        for stmt in statements:
            if stmt.strip():  # Skip empty statements
                last_result = cursor.execute(stmt, prepared_params or ())
        return last_result

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute SQL with multiple parameter sets using Psycopg executemany."""
        return cursor.executemany(sql, prepared_params)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute single SQL statement using Psycopg execute."""
        return cursor.execute(sql, prepared_params or ())

    def _get_selected_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        # Data is already in dict format from DictRow
        return fetched_data, column_names, len(fetched_data)

    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        self.connection.commit()


class PsycopgAsyncDriver(PsycopgCopyMixin, AsyncDriverAdapterBase):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgAsyncConnection,
        statement_config: Optional[StatementConfig] = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        # Set default Psycopg-specific configuration
        if statement_config is None:
            from sqlspec.parameters.config import ParameterStyleConfig

            parameter_config = ParameterStyleConfig(
                default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
                supported_parameter_styles={
                    ParameterStyle.POSITIONAL_PYFORMAT,
                    ParameterStyle.NAMED_PYFORMAT,
                    ParameterStyle.NUMERIC,
                },
                type_coercion_map={},
                has_native_list_expansion=True,
                needs_static_script_compilation=False,  # Psycopg supports parameters in scripts
            )
            statement_config = StatementConfig(dialect="postgres", parameter_config=parameter_config)

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "PsycopgAsyncConnection") -> "PsycopgAsyncCursor":
        return PsycopgAsyncCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[tuple[Any, Optional[int], Any]]":
        """Hook for PostgreSQL-specific special operations (COPY commands).

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement to analyze

        Returns:
            Tuple of (cursor_result, rowcount_override, special_data) if handled,
            None if standard execution should proceed
        """
        # Check if this is a COPY statement marked by the pipeline
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            result = await self._handle_copy_operation_from_pipeline(cursor, statement)
            # Return execution result tuple - COPY operations typically don't return data
            from sqlspec.driver._common import create_execution_result

            return create_execution_result(result)

        return None

    async def _execute_copy_with_data(self, cursor: Any, sql_text: str, data_str: str) -> Any:
        """Execute COPY operation with data using Psycopg async context manager."""
        async with cursor.copy(sql_text) as copy:
            await copy.write(data_str)

    async def _execute_copy_without_data(self, cursor: Any, sql_text: str) -> Any:
        """Execute COPY operation without data using Psycopg async context manager."""
        async with cursor.copy(sql_text):
            pass  # Just execute the COPY command

    async def _handle_copy_operation_from_pipeline(self, cursor: Any, statement: "SQL") -> Any:
        """Async version of COPY handling using the mixin logic."""
        # Get the original SQL from pipeline metadata
        metadata = statement._processing_context.metadata if statement._processing_context else {}
        sql_text = metadata.get("postgres_copy_original_sql")
        if not sql_text:
            # Fallback to expression
            sql_text = str(statement.expression)

        # Get the raw COPY data from pipeline metadata
        copy_data = metadata.get("postgres_copy_data")

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

            return await self._execute_copy_with_data(cursor, sql_text, data_str)
        # COPY without data (e.g., COPY TO STDOUT)
        return await self._execute_copy_without_data(cursor, sql_text)

    async def _execute_script(
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig"
    ) -> Any:
        """Execute SQL script by splitting and executing statements individually.

        Psycopg doesn't have executescript but supports parameters in all statements.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_params or ())
        return last_result

    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute SQL with multiple parameter sets using Psycopg executemany."""
        return await cursor.executemany(sql, prepared_params)

    async def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any) -> Any:
        """Execute single SQL statement using Psycopg execute."""
        return await cursor.execute(sql, prepared_params or ())

    async def _get_selected_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        # Data is already in dict format from DictRow
        return fetched_data, column_names, len(fetched_data)

    def _get_row_count(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    async def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        await self.connection.commit()
