# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
import logging
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.adapters.psycopg.mixins import PsycopgCopyMixin
from sqlspec.adapters.psycopg.pipeline_steps import postgres_copy_pipeline_step
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import ParameterStyle
from sqlspec.parameters.config import ParameterStyleConfig
from sqlspec.statement.sql import SQL, StatementConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlspec.driver._common import ExecutionResult
    from sqlspec.statement.result import SQLResult
    from sqlspec.statement.sql import SQL

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
            # Escape quotes and wrap in quotes for strings
            escaped = item.replace('"', '\\"')
            elements.append(f'"{escaped}"')
        else:
            elements.append(str(item))

    return "{" + ",".join(elements) + "}"


psycopg_statement_config = StatementConfig(
    dialect="postgres",
    # Enhanced pipeline steps with precise execution control
    pre_process_steps=[postgres_copy_pipeline_step],  # COPY detection before parameterization
    post_process_steps=None,  # No post-processing needed for psycopg
    # Core pipeline configuration
    enable_parsing=True,
    enable_transformations=True,
    enable_validation=True,
    enable_caching=True,
    # Parameter processing configuration
    parameter_config=ParameterStyleConfig(
        default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        supported_parameter_styles={
            ParameterStyle.POSITIONAL_PYFORMAT,
            ParameterStyle.NAMED_PYFORMAT,
            ParameterStyle.NUMERIC,
            ParameterStyle.QMARK,  # Add support for ? placeholders
        },
        # Psycopg supports both named and positional pyformat natively
        supported_execution_parameter_styles={ParameterStyle.POSITIONAL_PYFORMAT, ParameterStyle.NAMED_PYFORMAT},
        # Fallback to positional when the original style is not supported
        default_execution_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
        type_coercion_map={
            str: lambda x: int(x) if x.isdigit() else x,  # Convert numeric strings to integers for integer columns
            dict: to_json,  # Convert dict to JSON string for JSONB columns
            list: _convert_list_to_postgres_array,  # Convert Python lists to PostgreSQL array format
            bool: lambda x: x,  # Keep booleans as-is for PostgreSQL
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


class PsycopgSyncDriver(PsycopgCopyMixin, SyncDriverAdapterBase):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        statement_config: "Optional[StatementConfig]" = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            statement_config = psycopg_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: PsycopgSyncConnection) -> PsycopgSyncCursor:
        return PsycopgSyncCursor(connection)

    def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL-specific special operations (COPY commands).

        Args:
            cursor: Psycopg cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if the special operation was handled and completed,
            None if standard execution should proceed
        """
        # Check if this is a COPY statement marked by the pipeline

        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            try:
                result = self._handle_copy_operation_from_pipeline(cursor, statement)

                # Create ExecutionResult and build SQLResult directly - capture rowcount from cursor
                row_count = result.rowcount if hasattr(result, "rowcount") and result.rowcount is not None else -1
                execution_result = self.create_execution_result(result, rowcount_override=row_count)
                return self.build_statement_result(statement, execution_result)
            except Exception:
                # Log the error but don't fail silently
                logger.exception("COPY operation failed in special handling")
                raise

        return None

    def _execute_copy_with_data(self, cursor: Any, sql_text: str, data_str: str) -> Any:
        """Execute COPY operation with data using Psycopg sync context manager."""
        with cursor.copy(sql_text) as copy:
            copy.write(data_str)
        # Return cursor after copy operation completes - rowcount should be available
        return cursor

    def _execute_copy_without_data(self, cursor: Any, sql_text: str) -> Any:
        """Execute COPY operation without data using Psycopg sync context manager."""
        with cursor.copy(sql_text):
            pass  # Just execute the COPY command
        return cursor

    def _handle_copy_operation_from_pipeline(self, cursor: Any, statement: "SQL") -> Any:
        """Sync version of COPY handling using the pipeline metadata."""
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

            return self._execute_copy_with_data(cursor, sql_text, data_str)
        # COPY without data (e.g., COPY TO STDOUT)
        return self._execute_copy_without_data(cursor, sql_text)

    def _execute_script(
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig", statement: "SQL"
    ) -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        Psycopg doesn't have executescript but supports parameters in all statements.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)  # Script splitter already filters empty statements

        last_result = None
        for stmt in statements:
            last_result = cursor.execute(stmt, prepared_params or ())

        return self.create_execution_result(
            last_result,
            statement_count=statement_count,
            successful_statements=statement_count,  # Assume all successful if no exception
            is_script_result=True,
        )

    def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Psycopg executemany."""
        result = cursor.executemany(sql, prepared_params)

        # For executemany, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(result, rowcount_override=row_count, is_many_result=True)

    def _execute_statement(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement using Psycopg execute."""
        result = cursor.execute(sql, prepared_params or ())

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
            fetched_data = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            # Data is already in dict format from DictRow
            return self.create_execution_result(
                result,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        # For non-SELECT operations, get row count
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


class PsycopgAsyncDriver(PsycopgCopyMixin, AsyncDriverAdapterBase):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect = "postgres"

    def __init__(
        self,
        connection: PsycopgAsyncConnection,
        statement_config: Optional[StatementConfig] = None,
        driver_features: "Optional[dict[str, Any]]" = None,
    ) -> None:
        if statement_config is None:
            statement_config = psycopg_statement_config

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)

    def with_cursor(self, connection: "PsycopgAsyncConnection") -> "PsycopgAsyncCursor":
        return PsycopgAsyncCursor(connection)

    async def _try_special_handling(self, cursor: Any, statement: "SQL") -> "Optional[SQLResult]":
        """Hook for PostgreSQL-specific special operations (COPY commands).

        Args:
            cursor: Psycopg async cursor object
            statement: SQL statement to analyze

        Returns:
            SQLResult if the special operation was handled and completed,
            None if standard execution should proceed
        """
        # Check if this is a COPY statement marked by the pipeline

        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            try:
                result = await self._handle_copy_operation_from_pipeline(cursor, statement)

                # Create ExecutionResult and build SQLResult directly - capture rowcount from cursor
                row_count = result.rowcount if hasattr(result, "rowcount") and result.rowcount is not None else -1
                execution_result = self.create_execution_result(result, rowcount_override=row_count)
                return self.build_statement_result(statement, execution_result)
            except Exception:
                # Log the error but don't fail silently
                logger.exception("Async COPY operation failed in special handling")
                raise

        return None

    async def _execute_copy_with_data(self, cursor: Any, sql_text: str, data_str: str) -> Any:
        """Execute COPY operation with data using Psycopg async context manager."""
        async with cursor.copy(sql_text) as copy:
            await copy.write(data_str)
        # Return cursor after copy operation completes - rowcount should be available
        return cursor

    async def _execute_copy_without_data(self, cursor: Any, sql_text: str) -> Any:
        """Execute COPY operation without data using Psycopg async context manager."""
        async with cursor.copy(sql_text):
            pass  # Just execute the COPY command
        return cursor

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
        self, cursor: Any, sql: str, prepared_params: Any, statement_config: "StatementConfig", statement: "SQL"
    ) -> "ExecutionResult":
        """Execute SQL script by splitting and executing statements individually.

        Psycopg doesn't have executescript but supports parameters in all statements.
        """
        statements = self.split_script_statements(sql, statement_config, strip_trailing_semicolon=True)
        statement_count = len(statements)  # Script splitter already filters empty statements

        last_result = None
        for stmt in statements:
            last_result = await cursor.execute(stmt, prepared_params or ())

        return self.create_execution_result(
            last_result,
            statement_count=statement_count,
            successful_statements=statement_count,  # Assume all successful if no exception
            is_script_result=True,
        )

    async def _execute_many(self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using Psycopg executemany."""
        result = await cursor.executemany(sql, prepared_params)

        # For executemany, get row count
        row_count = cursor.rowcount if cursor.rowcount is not None else 0
        return self.create_execution_result(result, rowcount_override=row_count, is_many_result=True)

    async def _execute_statement(
        self, cursor: Any, sql: str, prepared_params: Any, statement: "SQL"
    ) -> "ExecutionResult":
        """Execute single SQL statement using Psycopg execute."""
        result = await cursor.execute(sql, prepared_params or ())

        if statement.returns_rows():
            # Extract data immediately for SELECT operations
            fetched_data = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            # Data is already in dict format from DictRow
            return self.create_execution_result(
                result,
                selected_data=fetched_data,
                column_names=column_names,
                data_row_count=len(fetched_data),
                is_select_result=True,
            )

        # For non-SELECT operations, get row count
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
