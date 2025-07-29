# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false
from typing import TYPE_CHECKING, Any, Optional

from sqlspec.adapters.psycopg._types import PsycopgAsyncConnection, PsycopgSyncConnection
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.parameters import DriverParameterConfig, ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

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


class PsycopgSyncDriver(SyncDriverAdapterBase):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"
    parameter_config: DriverParameterConfig

    def __init__(self, connection: PsycopgSyncConnection, config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=[
                ParameterStyle.POSITIONAL_PYFORMAT,  # %s
                ParameterStyle.NAMED_PYFORMAT,  # %(name)s
                ParameterStyle.NUMERIC,  # $1 (also supported!)
            ],
            default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
            type_coercion_map={
                # Psycopg handles most types natively
                # Add any specific type mappings as needed
            },
            has_native_list_expansion=True,  # Psycopg handles lists/tuples natively
            force_style_conversion=False,  # Psycopg natively supports %s placeholders
        )

    def with_cursor(self, connection: PsycopgSyncConnection) -> PsycopgSyncCursor:
        return PsycopgSyncCursor(connection)

    def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        sql, params = self._get_compiled_sql(statement, self.parameter_config.default_parameter_style)

        # Check if this is a COPY statement marked by the pipeline
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            self._handle_copy_operation_from_pipeline(cursor, statement)
            return

        if statement.is_script:
            # Psycopg doesn't have executescript - execute statements one by one
            # But we can still use parameters since we're using regular execute()
            prepared_params = self._prepare_driver_parameters(params)
            # Use the proper script splitter to handle complex cases
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    cursor.execute(stmt, prepared_params or ())
        elif statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            cursor.execute(sql, prepared_params or ())

    def _handle_copy_operation_from_pipeline(self, cursor: Any, statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations using pipeline metadata.

        Args:
            cursor: Database cursor
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

            # Use Psycopg's COPY context manager for proper resource handling
            with cursor.copy(sql_text) as copy:
                copy.write(data_str)
        else:
            # COPY without data (e.g., COPY TO STDOUT)
            with cursor.copy(sql_text):
                pass  # Just execute the COPY command

    def _extract_select_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        # Data is already in dict format from DictRow
        return fetched_data, column_names, len(fetched_data)

    def _extract_execute_rowcount(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution.

        Override to handle psycopg-specific executemany + RETURNING behavior.
        Psycopg's executemany doesn't return result sets even for RETURNING clauses.
        """
        if statement.is_script:
            row_count = self._extract_execute_rowcount(cursor)
            # Count statements in the script
            sql, _ = statement.compile()
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            statement_count = len([stmt for stmt in statements if stmt.strip()])
            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=row_count,
                operation_type="SCRIPT",
                total_statements=statement_count,
                successful_statements=statement_count,  # Assume all successful if no exception
                metadata={"status_message": "OK"},
            )

        # Special handling for executemany with RETURNING clauses
        if statement.is_many and self.returns_rows(statement.expression):
            # Psycopg's executemany doesn't return result sets for RETURNING clauses
            # so we treat it as an execute operation (rowcount only)
            row_count = self._extract_execute_rowcount(cursor)
            return self._build_execute_result_from_data(statement=statement, row_count=row_count)

        # Handle regular operations (delegate to parent)
        if self.returns_rows(statement.expression):
            data, column_names, row_count = self._extract_select_data(cursor)
            return self._build_select_result_from_data(
                statement=statement, data=data, column_names=column_names, row_count=row_count
            )
        row_count = self._extract_execute_rowcount(cursor)
        return self._build_execute_result_from_data(statement=statement, row_count=row_count)

    def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        self.connection.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        self.connection.rollback()

    def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        self.connection.commit()


class PsycopgAsyncDriver(AsyncDriverAdapterBase):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"
    parameter_config: DriverParameterConfig

    def __init__(self, connection: PsycopgAsyncConnection, config: Optional[SQLConfig] = None) -> None:
        super().__init__(connection=connection, config=config)
        self.parameter_config = DriverParameterConfig(
            supported_parameter_styles=[
                ParameterStyle.POSITIONAL_PYFORMAT,
                ParameterStyle.NAMED_PYFORMAT,
                ParameterStyle.NUMERIC,
            ],
            default_parameter_style=ParameterStyle.POSITIONAL_PYFORMAT,
            type_coercion_map={},
            has_native_list_expansion=True,
            force_style_conversion=False,
        )

    def with_cursor(self, connection: "PsycopgAsyncConnection") -> "PsycopgAsyncCursor":
        return PsycopgAsyncCursor(connection)

    async def _perform_execute(self, cursor: Any, statement: "SQL") -> None:
        sql, params = self._get_compiled_sql(statement, self.parameter_config.default_parameter_style)

        # Check if this is a COPY statement marked by the pipeline
        if statement._processing_context and statement._processing_context.metadata.get("postgres_copy_operation"):
            await self._handle_copy_operation_from_pipeline(cursor, statement)
            return

        if statement.is_script:
            # Psycopg doesn't have executescript - execute statements one by one
            # But we can still use parameters since we're using regular execute()
            prepared_params = self._prepare_driver_parameters(params)
            # Use the proper script splitter to handle complex cases
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            for stmt in statements:
                if stmt.strip():  # Skip empty statements
                    await cursor.execute(stmt, prepared_params or ())
        elif statement.is_many:
            # For execute_many, params is already a list of parameter sets
            prepared_params = self._prepare_driver_parameters_many(params) if params else []
            await cursor.executemany(sql, prepared_params)
        else:
            prepared_params = self._prepare_driver_parameters(params)
            await cursor.execute(sql, prepared_params or ())

    async def _handle_copy_operation_from_pipeline(self, cursor: Any, statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations using pipeline metadata.

        Args:
            cursor: Database cursor
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

            # Use Psycopg's async COPY context manager for proper resource handling
            async with cursor.copy(sql_text) as copy:
                await copy.write(data_str)
        else:
            # COPY without data (e.g., COPY TO STDOUT)
            async with cursor.copy(sql_text):
                pass  # Just execute the COPY command

    async def _extract_select_data(self, cursor: Any) -> "tuple[list[dict[str, Any]], list[str], int]":
        """Extract data from cursor after SELECT execution."""
        fetched_data = await cursor.fetchall()
        column_names = [col.name for col in cursor.description or []]
        # Data is already in dict format from DictRow
        return fetched_data, column_names, len(fetched_data)

    def _extract_execute_rowcount(self, cursor: Any) -> int:
        """Extract row count from cursor after INSERT/UPDATE/DELETE."""
        return cursor.rowcount if cursor.rowcount is not None else 0

    async def _build_result(self, cursor: Any, statement: "SQL") -> "SQLResult":
        """Build and return the result of the SQL execution.

        Override to handle psycopg-specific executemany + RETURNING behavior.
        Psycopg's executemany doesn't return result sets even for RETURNING clauses.
        """
        if statement.is_script:
            row_count = self._extract_execute_rowcount(cursor)
            # Count statements in the script
            sql, _ = statement.compile()
            statements = self._split_script_statements(sql, strip_trailing_semicolon=True)
            statement_count = len([stmt for stmt in statements if stmt.strip()])
            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=row_count,
                operation_type="SCRIPT",
                total_statements=statement_count,
                successful_statements=statement_count,  # Assume all successful if no exception
                metadata={"status_message": "OK"},
            )

        # Special handling for executemany with RETURNING clauses
        if statement.is_many and self.returns_rows(statement.expression):
            # Psycopg's executemany doesn't return result sets for RETURNING clauses
            # so we treat it as an execute operation (rowcount only)
            row_count = self._extract_execute_rowcount(cursor)
            return self._build_execute_result_from_data(statement=statement, row_count=row_count)

        # Handle regular operations (delegate to parent)
        if self.returns_rows(statement.expression):
            data, column_names, row_count = await self._extract_select_data(cursor)
            return self._build_select_result_from_data(
                statement=statement, data=data, column_names=column_names, row_count=row_count
            )
        row_count = self._extract_execute_rowcount(cursor)
        return self._build_execute_result_from_data(statement=statement, row_count=row_count)

    async def begin(self) -> None:
        """Begin transaction using psycopg-specific method."""
        await self.connection.execute("BEGIN")

    async def rollback(self) -> None:
        """Rollback transaction using psycopg-specific method."""
        await self.connection.rollback()

    async def commit(self) -> None:
        """Commit transaction using psycopg-specific method."""
        await self.connection.commit()
