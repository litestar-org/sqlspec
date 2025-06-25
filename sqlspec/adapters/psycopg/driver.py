import io
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union, cast

if TYPE_CHECKING:
    from psycopg.abc import Query

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow as PsycopgDictRow
from sqlglot.dialects.dialect import DialectType

from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.splitter import split_sql_script
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT, is_dict_with_field
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

logger = get_logger("adapters.psycopg")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[PsycopgDictRow]
PsycopgAsyncConnection = AsyncConnection[PsycopgDictRow]


class PsycopgSyncDriver(
    SyncDriverAdapterProtocol[PsycopgSyncConnection, RowT],
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
    __slots__ = ()

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
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        detected_styles = {p.style for p in statement.parameter_info}
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
            sql, params = statement.compile(placeholder_style=target_style)
            # For execute_many, check if parameters were passed via kwargs (legacy support)
            # Otherwise use the parameters from the SQL object
            kwargs_params = kwargs.get("parameters")
            if kwargs_params is not None:
                params = kwargs_params
            if params is not None:
                processed_params = [self._process_parameters(param_set) for param_set in params]
                params = processed_params
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        params = self._process_parameters(params)
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        with conn.cursor() as cursor:
            cursor.execute(cast("Query", sql), parameters)
            # Check if the statement returns rows by checking cursor.description
            # This is more reliable than parsing when parsing is disabled
            if cursor.description is not None:
                fetched_data = cursor.fetchall()
                column_names = [col.name for col in cursor.description]
                return {"data": fetched_data, "column_names": column_names, "rows_affected": len(fetched_data)}
            return {"rows_affected": cursor.rowcount, "status_message": cursor.statusmessage or "OK"}

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsycopgSyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.executemany(sql, param_list or [])
            # psycopg's executemany might return -1 or 0 for rowcount
            # In that case, use the length of param_list for DML operations
            rows_affected = cursor.rowcount
            if rows_affected <= 0 and param_list:
                rows_affected = len(param_list)
            result: DMLResultDict = {"rows_affected": rows_affected, "status_message": cursor.statusmessage or "OK"}
            return result

    def _execute_script(
        self, script: str, connection: Optional[PsycopgSyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.execute(script)
            result: ScriptResultDict = {
                "statements_executed": -1,
                "status_message": cursor.statusmessage or "SCRIPT EXECUTED",
            }
            return result

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

    def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in result["data"]]

        if schema_type:
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(self.to_schema(data=result["data"], schema_type=schema_type)),
                column_names=result["column_names"],
                rows_affected=result["rows_affected"],
                operation_type="SELECT",
            )
        return SQLResult[RowT](
            statement=statement,
            data=rows_as_dicts,
            column_names=result["column_names"],
            rows_affected=result["rows_affected"],
            operation_type="SELECT",
        )

    def _wrap_execute_result(
        self, statement: SQL, result: Union[DMLResultDict, ScriptResultDict], **kwargs: Any
    ) -> SQLResult[RowT]:
        operation_type = "UNKNOWN"
        if statement.expression:
            operation_type = str(statement.expression.key).upper()

        # Handle case where we got a SelectResultDict but it was routed here due to parsing being disabled
        if is_dict_with_field(result, "data") and is_dict_with_field(result, "column_names"):
            # This is actually a SELECT result, wrap it properly
            return self._wrap_select_result(statement, cast("SelectResultDict", result), **kwargs)

        if is_dict_with_field(result, "statements_executed"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type="SCRIPT",
                metadata={"status_message": result.get("status_message", "")},
            )

        if is_dict_with_field(result, "rows_affected"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=cast("int", result.get("rows_affected", -1)),
                operation_type=operation_type,
                metadata={"status_message": result.get("status_message", "")},
            )

        # This shouldn't happen with TypedDict approach
        msg = f"Unexpected result type: {type(result)}"
        raise ValueError(msg)

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
        from sqlspec.exceptions import PipelineExecutionError

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
            # Prepare SQL and parameters
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
                operation_type="execute_many",
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
                operation_type="select",
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
            operation_type="execute_script",
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
                operation_type="execute",
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
            # Convert to tuple for positional parameters
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

        # Use the sophisticated splitter with PostgreSQL dialect
        return split_sql_script(script=script, dialect="postgresql", strip_trailing_semicolon=strip_trailing_semicolon)


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, RowT],
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
    __slots__ = ()

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
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Determine if we need to convert parameter style
        detected_styles = {p.style for p in statement.parameter_info}
        target_style = self.default_parameter_style

        # Check if any detected style is not supported
        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            # Convert to default style if we have unsupported styles
            target_style = self.default_parameter_style
        elif detected_styles:
            # Use the first detected style if all are supported
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            sql, _ = statement.compile(placeholder_style=target_style)
            # For execute_many, use the parameters passed via kwargs
            params = kwargs.get("parameters")
            if params is not None:
                # Process each parameter set individually
                processed_params = [self._process_parameters(param_set) for param_set in params]
                params = processed_params
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        params = self._process_parameters(params)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        async with conn.cursor() as cursor:
            await cursor.execute(cast("Query", sql), parameters)

            # When parsing is disabled, expression will be None, so check SQL directly
            if statement.expression and self.returns_rows(statement.expression):
                # For SELECT statements, extract data while cursor is open
                fetched_data = await cursor.fetchall()
                column_names = [col.name for col in cursor.description or []]
                return {"data": fetched_data, "column_names": column_names, "rows_affected": len(fetched_data)}
            if not statement.expression and sql.strip().upper().startswith("SELECT"):
                # For SELECT statements when parsing is disabled
                fetched_data = await cursor.fetchall()
                column_names = [col.name for col in cursor.description or []]
                return {"data": fetched_data, "column_names": column_names, "rows_affected": len(fetched_data)}
            # For DML statements
            dml_result: DMLResultDict = {
                "rows_affected": cursor.rowcount,
                "status_message": cursor.statusmessage or "OK",
            }
            return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        async with conn.cursor() as cursor:
            await cursor.executemany(cast("Query", sql), param_list or [])
            return {"rows_affected": cursor.rowcount, "status_message": cursor.statusmessage or "OK"}

    async def _execute_script(
        self, script: str, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        async with conn.cursor() as cursor:
            await cursor.execute(cast("Query", script))
            # For scripts, return script result format
            return {
                "statements_executed": -1,  # Psycopg doesn't provide this info
                "status_message": cursor.statusmessage or "SCRIPT EXECUTED",
            }

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

    async def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        # result must be a dict with keys: data, column_names, rows_affected
        fetched_data = result["data"]
        column_names = result["column_names"]
        rows_affected = result["rows_affected"]
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

        if schema_type:
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(self.to_schema(data=fetched_data, schema_type=schema_type)),
                column_names=column_names,
                rows_affected=rows_affected,
                operation_type="SELECT",
            )
        return SQLResult[RowT](
            statement=statement,
            data=rows_as_dicts,
            column_names=column_names,
            rows_affected=rows_affected,
            operation_type="SELECT",
        )

    async def _wrap_execute_result(
        self, statement: SQL, result: Union[DMLResultDict, ScriptResultDict], **kwargs: Any
    ) -> SQLResult[RowT]:
        operation_type = "UNKNOWN"
        if statement.expression:
            operation_type = str(statement.expression.key).upper()

        if is_dict_with_field(result, "statements_executed"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type="SCRIPT",
                metadata={"status_message": result.get("status_message", "")},
            )

        if is_dict_with_field(result, "rows_affected"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=cast("int", result.get("rows_affected", -1)),
                operation_type=operation_type,
                metadata={"status_message": result.get("status_message", "")},
            )
        # This shouldn't happen with TypedDict approach
        msg = f"Unexpected result type: {type(result)}"
        raise ValueError(msg)

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
            # Prepare SQL and parameters
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
            # Add pipeline context
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
                operation_type="execute_many",
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
                operation_type="select",
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
            operation_type="execute_script",
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
                operation_type="execute",
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
            # Convert to tuple for positional parameters
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
