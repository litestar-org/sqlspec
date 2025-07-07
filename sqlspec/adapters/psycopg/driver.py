import io
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from psycopg.abc import Query

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow as PsycopgDictRow
from sqlglot.dialects.dialect import DialectType

from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.driver.connection import managed_transaction_async, managed_transaction_sync
from sqlspec.driver.mixins import (
    AsyncAdapterCacheMixin,
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    SyncAdapterCacheMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.driver.parameters import convert_parameter_sequence
from sqlspec.exceptions import PipelineExecutionError
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.splitter import split_sql_script
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, RowT
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

logger = get_logger("adapters.psycopg")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[PsycopgDictRow]
PsycopgAsyncConnection = AsyncConnection[PsycopgDictRow]


class PsycopgSyncDriver(
    SyncDriverAdapterProtocol[PsycopgSyncConnection, RowT],
    SyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"  # pyright: ignore[reportInvalidTypeForm]
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.POSITIONAL_PYFORMAT,
        ParameterStyle.NAMED_PYFORMAT,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = dict,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    @staticmethod
    @contextmanager
    def _get_cursor(connection: PsycopgSyncConnection) -> Generator[Any, None, None]:
        with connection.cursor() as cursor:
            yield cursor

    def _execute_statement(
        self, statement: SQL, connection: Optional[PsycopgSyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        detected_styles = set()
        sql_str = statement.to_sql(placeholder_style=None)  # Get raw SQL
        validator = self.config.parameter_validator if self.config else ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)
        if param_infos:
            detected_styles = {p.style for p in param_infos}

        target_style = self.default_parameter_style
        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            target_style = self.default_parameter_style
        elif detected_styles:
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            # Check if parameters were provided in kwargs first
            kwargs_params = kwargs.get("parameters")
            if kwargs_params is not None:
                # Use the SQL string directly if parameters come from kwargs
                sql = statement.to_sql(placeholder_style=target_style)
                params = kwargs_params
            else:
                sql, params = self._get_compiled_sql(statement, target_style)
            if params is not None:
                processed_params = [self._process_parameters(param_set) for param_set in params]
                params = processed_params
            # Remove 'parameters' from kwargs to avoid conflicts in _execute_many method signature
            exec_kwargs = {k: v for k, v in kwargs.items() if k != "parameters"}
            return self._execute_many(sql, params, connection=connection, **exec_kwargs)

        # Check if parameters were provided in kwargs (user-provided parameters)
        kwargs_params = kwargs.get("parameters")
        if kwargs_params is not None:
            # Use the SQL string directly if parameters come from kwargs
            sql = statement.to_sql(placeholder_style=target_style)
            params = kwargs_params
        else:
            sql, params = self._get_compiled_sql(statement, target_style)
        params = self._process_parameters(params)

        # Fix over-nested parameters for Psycopg
        # If params is a tuple containing a single tuple or dict, flatten it
        if isinstance(params, tuple) and len(params) == 1 and isinstance(params[0], (tuple, dict, list)):
            params = params[0]

        # Remove 'parameters' from kwargs to avoid conflicts in _execute method signature
        exec_kwargs = {k: v for k, v in kwargs.items() if k != "parameters"}
        return self._execute(sql, params, statement, connection=connection, **exec_kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        # Handle COPY commands separately (they don't use transactions)
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("COPY") and ("FROM STDIN" in sql_upper or "TO STDOUT" in sql_upper):
            return self._handle_copy_command(sql, parameters, conn)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # For Psycopg, pass parameters directly to the driver
            final_params = parameters

            # Debug logging
            logger.debug("Executing SQL: %r with parameters: %r", sql, final_params)

            with txn_conn.cursor() as cursor:
                cursor.execute(cast("Query", sql), final_params)
                if cursor.description is not None:
                    fetched_data = cursor.fetchall()
                    column_names = [col.name for col in cursor.description]
                    return SQLResult(
                        statement=statement,
                        data=cast("list[RowT]", fetched_data),
                        column_names=column_names,
                        rows_affected=len(fetched_data),
                        operation_type="SELECT",
                    )
                operation_type = self._determine_operation_type(statement)
                return SQLResult(
                    statement=statement,
                    data=[],
                    rows_affected=cursor.rowcount or 0,
                    operation_type=operation_type,
                    metadata={"status_message": cursor.statusmessage or "OK"},
                )

    def _handle_copy_command(self, sql: str, data: Any, connection: PsycopgSyncConnection) -> SQLResult[RowT]:
        """Handle PostgreSQL COPY commands using cursor.copy() method."""
        sql_upper = sql.strip().upper()

        # Handle case where data is wrapped in a single-element tuple (from positional args)
        if isinstance(data, tuple) and len(data) == 1:
            data = data[0]

        with connection.cursor() as cursor:
            if "TO STDOUT" in sql_upper:
                # COPY TO STDOUT - read data from the database
                output_data: list[Any] = []
                with cursor.copy(cast("Query", sql)) as copy:
                    output_data.extend(row for row in copy)

                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=cast("list[RowT]", output_data),
                    column_names=["copy_data"],
                    rows_affected=len(output_data),
                    operation_type="SELECT",
                )
            # COPY FROM STDIN - write data to the database
            with cursor.copy(cast("Query", sql)) as copy:
                if data:
                    # If data is provided, write it to the copy stream
                    if isinstance(data, str):
                        copy.write(data.encode("utf-8"))
                    elif isinstance(data, bytes):
                        copy.write(data)
                    elif isinstance(data, (list, tuple)):
                        # If data is a list/tuple of rows, write each row
                        for row in data:
                            copy.write_row(row)
                    else:
                        # Single row
                        copy.write_row(data)

            # For COPY operations, cursor.rowcount contains the number of rows affected
            return SQLResult(
                statement=SQL(sql, _dialect=self.dialect),
                data=[],
                rows_affected=cursor.rowcount or -1,
                operation_type="EXECUTE",
                metadata={"status_message": cursor.statusmessage or "COPY COMPLETE"},
            )

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsycopgSyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)
            final_param_list = converted_param_list or []

            with self._get_cursor(txn_conn) as cursor:
                cursor.executemany(sql, final_param_list)
                # psycopg's executemany might return -1 or 0 for rowcount
                # In that case, use the length of param_list for DML operations
                rows_affected = cursor.rowcount
                if rows_affected <= 0 and final_param_list:
                    rows_affected = len(final_param_list)
                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=rows_affected,
                    operation_type="EXECUTE",
                    metadata={"status_message": cursor.statusmessage or "OK"},
                )

    def _execute_script(
        self, script: str, connection: Optional[PsycopgSyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn, self._get_cursor(txn_conn) as cursor:
            # Split script into individual statements for validation
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0
            last_status = None

            # Execute each statement individually for better control and validation
            for statement in statements:
                if statement.strip():
                    # Validate each statement unless warnings suppressed
                    if not suppress_warnings:
                        # Run validation through pipeline
                        temp_sql = SQL(statement, config=self.config)
                        temp_sql._ensure_processed()
                        # Validation errors are logged as warnings by default

                    cursor.execute(statement)
                    executed_count += 1
                    total_rows += cursor.rowcount or 0
                    last_status = cursor.statusmessage

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": last_status or "SCRIPT EXECUTED"},
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        import pyarrow.csv as pacsv

        conn = self._connection(None)
        with self._get_cursor(conn) as cursor:
            if mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for psycopg ingestion."
                raise NotImplementedError(msg)

            buffer = io.StringIO()
            pacsv.write_csv(table, buffer)
            buffer.seek(0)

            with cursor.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                copy.write(buffer.read())

            return cursor.rowcount if cursor.rowcount is not None else -1

    def _connection(self, connection: Optional[PsycopgSyncConnection] = None) -> PsycopgSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    def _execute_pipeline_native(self, operations: "list[Any]", **options: Any) -> "list[SQLResult[RowT]]":
        """Native pipeline execution using Psycopg's pipeline support.

        Psycopg has built-in pipeline support through the connection.pipeline() context manager.
        This provides significant performance benefits for batch operations.

        Args:
            operations: List of PipelineOperation objects
            **options: Pipeline configuration options

        Returns:
            List of SQLResult objects from all operations
        """

        results = []
        connection = self._connection()

        try:
            with connection.pipeline():
                for i, op in enumerate(operations):
                    result = self._execute_pipeline_operation(i, op, connection, options)
                    results.append(result)

        except Exception as e:
            if not isinstance(e, PipelineExecutionError):
                msg = f"Psycopg pipeline execution failed: {e}"
                raise PipelineExecutionError(msg) from e
            raise

        return results

    def _execute_pipeline_operation(
        self, index: int, operation: Any, connection: Any, options: dict
    ) -> "SQLResult[RowT]":
        """Execute a single pipeline operation with error handling."""
        from sqlspec.exceptions import PipelineExecutionError

        try:
            filtered_sql = self._apply_operation_filters(operation.sql, operation.filters)
            sql_str = filtered_sql.to_sql(placeholder_style=self.default_parameter_style)
            params = self._convert_psycopg_params(filtered_sql.parameters)

            # Execute based on operation type
            result = self._dispatch_pipeline_operation(operation, sql_str, params, connection)

        except Exception as e:
            if options.get("continue_on_error"):
                return SQLResult[RowT](
                    statement=operation.sql,
                    data=cast("list[RowT]", []),
                    error=e,
                    operation_index=index,
                    parameters=operation.original_params,
                )
            msg = f"Psycopg pipeline failed at operation {index}: {e}"
            raise PipelineExecutionError(
                msg, operation_index=index, partial_results=[], failed_operation=operation
            ) from e
        else:
            result.operation_index = index
            result.pipeline_sql = operation.sql
            return result

    def _dispatch_pipeline_operation(
        self, operation: Any, sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Dispatch to appropriate handler based on operation type."""
        handlers = {
            "execute_many": self._handle_pipeline_execute_many,
            "select": self._handle_pipeline_select,
            "execute_script": self._handle_pipeline_execute_script,
        }

        handler = handlers.get(operation.operation_type, self._handle_pipeline_execute)
        return handler(operation.sql, sql_str, params, connection)

    def _handle_pipeline_execute_many(
        self, sql: "SQL", sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Handle execute_many operation in pipeline."""
        with connection.cursor() as cursor:
            cursor.executemany(sql_str, params)
            return SQLResult[RowT](
                statement=sql,
                data=cast("list[RowT]", []),
                rows_affected=cursor.rowcount,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    def _handle_pipeline_select(self, sql: "SQL", sql_str: str, params: Any, connection: Any) -> "SQLResult[RowT]":
        """Handle select operation in pipeline."""
        with connection.cursor() as cursor:
            cursor.execute(sql_str, params)
            fetched_data = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            data = [dict(record) for record in fetched_data] if fetched_data else []
            return SQLResult[RowT](
                statement=sql,
                data=cast("list[RowT]", data),
                rows_affected=len(data),
                operation_type="SELECT",
                metadata={"column_names": column_names},
            )

    def _handle_pipeline_execute_script(
        self, sql: "SQL", sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Handle execute_script operation in pipeline."""
        script_statements = self._split_script_statements(sql_str)
        total_affected = 0

        with connection.cursor() as cursor:
            for stmt in script_statements:
                if stmt.strip():
                    cursor.execute(stmt)
                    total_affected += cursor.rowcount or 0

        return SQLResult[RowT](
            statement=sql,
            data=cast("list[RowT]", []),
            rows_affected=total_affected,
            operation_type="SCRIPT",
            metadata={"status_message": "SCRIPT EXECUTED", "statements_executed": len(script_statements)},
        )

    def _handle_pipeline_execute(self, sql: "SQL", sql_str: str, params: Any, connection: Any) -> "SQLResult[RowT]":
        """Handle regular execute operation in pipeline."""
        with connection.cursor() as cursor:
            cursor.execute(sql_str, params)
            return SQLResult[RowT](
                statement=sql,
                data=cast("list[RowT]", []),
                rows_affected=cursor.rowcount or 0,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    def _convert_psycopg_params(self, params: Any) -> Any:
        """Convert parameters to Psycopg-compatible format.

        Psycopg supports both named (%s, %(name)s) and positional (%s) parameters.

        Args:
            params: Parameters in various formats

        Returns:
            Parameters in Psycopg-compatible format
        """
        if params is None:
            return None
        if isinstance(params, dict):
            # Psycopg handles dict parameters directly for named placeholders
            return params
        if isinstance(params, (list, tuple)):
            return tuple(params)
        # Single parameter
        return (params,)

    def _apply_operation_filters(self, sql: "SQL", filters: "list[Any]") -> "SQL":
        """Apply filters to a SQL object for pipeline operations."""
        if not filters:
            return sql

        result_sql = sql
        for filter_obj in filters:
            if hasattr(filter_obj, "apply"):
                result_sql = filter_obj.apply(result_sql)

        return result_sql

    def _split_script_statements(self, script: str, strip_trailing_semicolon: bool = False) -> "list[str]":
        """Split a SQL script into individual statements."""

        return split_sql_script(script=script, dialect="postgresql", strip_trailing_semicolon=strip_trailing_semicolon)


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, RowT],
    AsyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"  # pyright: ignore[reportInvalidTypeForm]
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.POSITIONAL_PYFORMAT,
        ParameterStyle.NAMED_PYFORMAT,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT

    def __init__(
        self,
        connection: PsycopgAsyncConnection,
        config: Optional[SQLConfig] = None,
        default_row_type: "type[DictRow]" = dict,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    @staticmethod
    @asynccontextmanager
    async def _get_cursor(connection: PsycopgAsyncConnection) -> AsyncGenerator[Any, None]:
        async with connection.cursor() as cursor:
            yield cursor

    async def _execute_statement(
        self, statement: SQL, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        detected_styles = set()
        sql_str = statement.to_sql(placeholder_style=None)  # Get raw SQL
        validator = self.config.parameter_validator if self.config else ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)
        if param_infos:
            detected_styles = {p.style for p in param_infos}

        target_style = self.default_parameter_style

        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            target_style = self.default_parameter_style
        elif detected_styles:
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            # Check if parameters were provided in kwargs first
            kwargs_params = kwargs.get("parameters")
            if kwargs_params is not None:
                # Use the SQL string directly if parameters come from kwargs
                sql = statement.to_sql(placeholder_style=target_style)
                params = kwargs_params
            else:
                sql, params = self._get_compiled_sql(statement, target_style)
            if params is not None:
                processed_params = [self._process_parameters(param_set) for param_set in params]
                params = processed_params

                # Fix over-nested parameters for each param set
                fixed_params = []
                for param_set in params:
                    if isinstance(param_set, tuple) and len(param_set) == 1:
                        fixed_params.append(param_set[0])
                    else:
                        fixed_params.append(param_set)
                params = fixed_params
            # Remove 'parameters' from kwargs to avoid conflicts in _execute_many method signature
            exec_kwargs = {k: v for k, v in kwargs.items() if k != "parameters"}
            return await self._execute_many(sql, params, connection=connection, **exec_kwargs)

        # Check if parameters were provided in kwargs (user-provided parameters)
        kwargs_params = kwargs.get("parameters")
        if kwargs_params is not None:
            # Use the SQL string directly if parameters come from kwargs
            sql = statement.to_sql(placeholder_style=target_style)
            params = kwargs_params
        else:
            sql, params = self._get_compiled_sql(statement, target_style)
        params = self._process_parameters(params)

        # Fix over-nested parameters for Psycopg
        # If params is a tuple containing a single tuple or dict, flatten it
        if isinstance(params, tuple) and len(params) == 1 and isinstance(params[0], (tuple, dict, list)):
            params = params[0]

        # Remove 'parameters' from kwargs to avoid conflicts in _execute method signature
        exec_kwargs = {k: v for k, v in kwargs.items() if k != "parameters"}
        return await self._execute(sql, params, statement, connection=connection, **exec_kwargs)

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        # Handle COPY commands separately (they don't use transactions)
        sql_upper = sql.strip().upper()
        if sql_upper.startswith("COPY") and ("FROM STDIN" in sql_upper or "TO STDOUT" in sql_upper):
            return await self._handle_copy_command(sql, parameters, conn)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # For Psycopg, pass parameters directly to the driver
            final_params = parameters

            async with txn_conn.cursor() as cursor:
                await cursor.execute(cast("Query", sql), final_params)

                # When parsing is disabled, expression will be None, so check SQL directly
                if statement.expression and self.returns_rows(statement.expression):
                    # For SELECT statements, extract data while cursor is open
                    fetched_data = await cursor.fetchall()
                    column_names = [col.name for col in cursor.description or []]
                    return SQLResult(
                        statement=statement,
                        data=cast("list[RowT]", fetched_data),
                        column_names=column_names,
                        rows_affected=len(fetched_data),
                        operation_type="SELECT",
                    )
                if not statement.expression and sql.strip().upper().startswith("SELECT"):
                    # For SELECT statements when parsing is disabled
                    fetched_data = await cursor.fetchall()
                    column_names = [col.name for col in cursor.description or []]
                    return SQLResult(
                        statement=statement,
                        data=cast("list[RowT]", fetched_data),
                        column_names=column_names,
                        rows_affected=len(fetched_data),
                        operation_type="SELECT",
                    )
                # For DML statements
                operation_type = self._determine_operation_type(statement)
                return SQLResult(
                    statement=statement,
                    data=[],
                    rows_affected=cursor.rowcount or 0,
                    operation_type=operation_type,
                    metadata={"status_message": cursor.statusmessage or "OK"},
                )

    async def _handle_copy_command(self, sql: str, data: Any, connection: PsycopgAsyncConnection) -> SQLResult[RowT]:
        """Handle PostgreSQL COPY commands using cursor.copy() method."""
        sql_upper = sql.strip().upper()

        # Handle case where data is wrapped in a single-element tuple (from positional args)
        if isinstance(data, tuple) and len(data) == 1:
            data = data[0]

        async with connection.cursor() as cursor:
            if "TO STDOUT" in sql_upper:
                # COPY TO STDOUT - read data from the database
                output_data = []
                async with cursor.copy(cast("Query", sql)) as copy:
                    output_data.extend([row async for row in copy])

                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=cast("list[RowT]", output_data),
                    column_names=["copy_data"],
                    rows_affected=len(output_data),
                    operation_type="SELECT",
                )
            # COPY FROM STDIN - write data to the database
            async with cursor.copy(cast("Query", sql)) as copy:
                if data:
                    # If data is provided, write it to the copy stream
                    if isinstance(data, str):
                        await copy.write(data.encode("utf-8"))
                    elif isinstance(data, bytes):
                        await copy.write(data)
                    elif isinstance(data, (list, tuple)):
                        # If data is a list/tuple of rows, write each row
                        for row in data:
                            await copy.write_row(row)
                    else:
                        # Single row
                        await copy.write_row(data)

            # For COPY operations, cursor.rowcount contains the number of rows affected
            return SQLResult(
                statement=SQL(sql, _dialect=self.dialect),
                data=[],
                rows_affected=cursor.rowcount or -1,
                operation_type="EXECUTE",
                metadata={"status_message": cursor.statusmessage or "COPY COMPLETE"},
            )

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)
            final_param_list = converted_param_list or []

            async with txn_conn.cursor() as cursor:
                await cursor.executemany(cast("Query", sql), final_param_list)
                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=cursor.rowcount,
                    operation_type="EXECUTE",
                    metadata={"status_message": cursor.statusmessage or "OK"},
                )

    async def _execute_script(
        self, script: str, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn, txn_conn.cursor() as cursor:
            # Split script into individual statements for validation
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0
            last_status = None

            # Execute each statement individually for better control and validation
            for statement in statements:
                if statement.strip():
                    # Validate each statement unless warnings suppressed
                    if not suppress_warnings:
                        # Run validation through pipeline
                        temp_sql = SQL(statement, config=self.config)
                        temp_sql._ensure_processed()
                        # Validation errors are logged as warnings by default

                    await cursor.execute(cast("Query", statement))
                    executed_count += 1
                    total_rows += cursor.rowcount or 0
                    last_status = cursor.statusmessage

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": last_status or "SCRIPT EXECUTED"},
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    async def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        async with conn.cursor() as cursor:
            await cursor.execute(
                cast("Query", sql.to_sql(placeholder_style=self.default_parameter_style)),
                sql.get_parameters(style=self.default_parameter_style) or [],
            )
            arrow_table = await cursor.fetch_arrow_table()  # type: ignore[attr-defined]
            return ArrowResult(statement=sql, data=arrow_table)

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        import pyarrow.csv as pacsv

        conn = self._connection(None)
        async with conn.cursor() as cursor:
            if mode == "replace":
                await cursor.execute(cast("Query", f"TRUNCATE TABLE {table_name}"))
            elif mode == "create":
                msg = "'create' mode is not supported for psycopg ingestion."
                raise NotImplementedError(msg)

            buffer = io.StringIO()
            pacsv.write_csv(table, buffer)
            buffer.seek(0)

            async with cursor.copy(cast("Query", f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)")) as copy:
                await copy.write(buffer.read())

            return cursor.rowcount if cursor.rowcount is not None else -1

    def _connection(self, connection: Optional[PsycopgAsyncConnection] = None) -> PsycopgAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    async def _execute_pipeline_native(self, operations: "list[Any]", **options: Any) -> "list[SQLResult[RowT]]":
        """Native async pipeline execution using Psycopg's pipeline support."""
        from sqlspec.exceptions import PipelineExecutionError

        results = []
        connection = self._connection()

        try:
            async with connection.pipeline():
                for i, op in enumerate(operations):
                    result = await self._execute_pipeline_operation_async(i, op, connection, options)
                    results.append(result)

        except Exception as e:
            if not isinstance(e, PipelineExecutionError):
                msg = f"Psycopg async pipeline execution failed: {e}"
                raise PipelineExecutionError(msg) from e
            raise

        return results

    async def _execute_pipeline_operation_async(
        self, index: int, operation: Any, connection: Any, options: dict
    ) -> "SQLResult[RowT]":
        """Execute a single async pipeline operation with error handling."""
        from sqlspec.exceptions import PipelineExecutionError

        try:
            filtered_sql = self._apply_operation_filters(operation.sql, operation.filters)
            sql_str = filtered_sql.to_sql(placeholder_style=self.default_parameter_style)
            params = self._convert_psycopg_params(filtered_sql.parameters)

            # Execute based on operation type
            result = await self._dispatch_pipeline_operation_async(operation, sql_str, params, connection)

        except Exception as e:
            if options.get("continue_on_error"):
                return SQLResult[RowT](
                    statement=operation.sql,
                    data=cast("list[RowT]", []),
                    error=e,
                    operation_index=index,
                    parameters=operation.original_params,
                )
            msg = f"Psycopg async pipeline failed at operation {index}: {e}"
            raise PipelineExecutionError(
                msg, operation_index=index, partial_results=[], failed_operation=operation
            ) from e
        else:
            result.operation_index = index
            result.pipeline_sql = operation.sql
            return result

    async def _dispatch_pipeline_operation_async(
        self, operation: Any, sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Dispatch to appropriate async handler based on operation type."""
        handlers = {
            "execute_many": self._handle_pipeline_execute_many_async,
            "select": self._handle_pipeline_select_async,
            "execute_script": self._handle_pipeline_execute_script_async,
        }

        handler = handlers.get(operation.operation_type, self._handle_pipeline_execute_async)
        return await handler(operation.sql, sql_str, params, connection)

    async def _handle_pipeline_execute_many_async(
        self, sql: "SQL", sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Handle async execute_many operation in pipeline."""
        async with connection.cursor() as cursor:
            await cursor.executemany(sql_str, params)
            return SQLResult[RowT](
                statement=sql,
                data=cast("list[RowT]", []),
                rows_affected=cursor.rowcount,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    async def _handle_pipeline_select_async(
        self, sql: "SQL", sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Handle async select operation in pipeline."""
        async with connection.cursor() as cursor:
            await cursor.execute(sql_str, params)
            fetched_data = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
            data = [dict(record) for record in fetched_data] if fetched_data else []
            return SQLResult[RowT](
                statement=sql,
                data=cast("list[RowT]", data),
                rows_affected=len(data),
                operation_type="SELECT",
                metadata={"column_names": column_names},
            )

    async def _handle_pipeline_execute_script_async(
        self, sql: "SQL", sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Handle async execute_script operation in pipeline."""
        script_statements = self._split_script_statements(sql_str)
        total_affected = 0

        async with connection.cursor() as cursor:
            for stmt in script_statements:
                if stmt.strip():
                    await cursor.execute(stmt)
                    total_affected += cursor.rowcount or 0

        return SQLResult[RowT](
            statement=sql,
            data=cast("list[RowT]", []),
            rows_affected=total_affected,
            operation_type="SCRIPT",
            metadata={"status_message": "SCRIPT EXECUTED", "statements_executed": len(script_statements)},
        )

    async def _handle_pipeline_execute_async(
        self, sql: "SQL", sql_str: str, params: Any, connection: Any
    ) -> "SQLResult[RowT]":
        """Handle async regular execute operation in pipeline."""
        async with connection.cursor() as cursor:
            await cursor.execute(sql_str, params)
            return SQLResult[RowT](
                statement=sql,
                data=cast("list[RowT]", []),
                rows_affected=cursor.rowcount or 0,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    def _convert_psycopg_params(self, params: Any) -> Any:
        """Convert parameters to Psycopg-compatible format.

        Psycopg supports both named (%s, %(name)s) and positional (%s) parameters.

        Args:
            params: Parameters in various formats

        Returns:
            Parameters in Psycopg-compatible format
        """
        if params is None:
            return None
        if isinstance(params, dict):
            # Psycopg handles dict parameters directly for named placeholders
            return params
        if isinstance(params, (list, tuple)):
            return tuple(params)
        # Single parameter
        return (params,)

    def _apply_operation_filters(self, sql: "SQL", filters: "list[Any]") -> "SQL":
        """Apply filters to a SQL object for pipeline operations."""
        if not filters:
            return sql

        result_sql = sql
        for filter_obj in filters:
            if hasattr(filter_obj, "apply"):
                result_sql = filter_obj.apply(result_sql)

        return result_sql
