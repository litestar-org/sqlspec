from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
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
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import ensure_async_

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = get_logger("adapters.oracledb")


class OracleSyncDriver(
    SyncDriverAdapterProtocol[OracleSyncConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Oracle Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "oracle"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.NAMED_COLON,
        ParameterStyle.POSITIONAL_COLON,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.NAMED_COLON
    support_native_arrow_export = True
    __slots__ = ("config", "connection", "default_row_type")

    def __init__(
        self,
        connection: OracleSyncConnection,
        config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    @contextmanager
    def _get_cursor(self, connection: Optional[OracleSyncConnection] = None) -> Generator[Cursor, None, None]:
        conn_to_use = connection or self.connection
        cursor: Cursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _execute_statement(
        self, statement: SQL, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

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
            sql, params = statement.compile(placeholder_style=target_style)
            params = self._process_parameters(params)
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        params = self._process_parameters(params)
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[OracleSyncConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.execute(sql, parameters or [])  # type: ignore[no-untyped-call]

            if self.returns_rows(statement.expression):
                fetched_data = cursor.fetchall()  # type: ignore[no-untyped-call]
                column_names = [col[0] for col in cursor.description or []]  # type: ignore[attr-defined]
                return {"data": fetched_data, "column_names": column_names, "rows_affected": cursor.rowcount}  # type: ignore[attr-defined]

            return {"rows_affected": cursor.rowcount, "status_message": "OK"}  # type: ignore[attr-defined]

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.executemany(sql, param_list or [])  # type: ignore[no-untyped-call]
            return {"rows_affected": cursor.rowcount, "status_message": "OK"}  # type: ignore[attr-defined]

    def _execute_script(
        self, script: str, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        statements = self._split_script_statements(script)
        with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement and statement.strip():
                    cursor.execute(statement)

        return {"statements_executed": len(statements), "status_message": "SCRIPT EXECUTED"}

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)
        arrow_table = conn.fetch_df_all(
            sql.to_sql(placeholder_style=self.default_parameter_style),
            sql.get_parameters(style=self.default_parameter_style) or [],
        )
        return ArrowResult(statement=sql, data=arrow_table)

    def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        conn = self._connection(None)

        with self._get_cursor(conn) as cursor:
            if mode == "replace":
                cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for oracledb ingestion."
                raise NotImplementedError(msg)

            data_for_ingest = table.to_pylist()
            if not data_for_ingest:
                return 0

            # Generate column placeholders: :1, :2, etc.
            num_columns = len(data_for_ingest[0])
            placeholders = ", ".join(f":{i + 1}" for i in range(num_columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.executemany(sql, data_for_ingest)
            return cursor.rowcount

    def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        fetched_tuples = result.get("data", [])
        column_names = result.get("column_names", [])

        if not fetched_tuples:
            return SQLResult[RowT](statement=statement, data=[], column_names=column_names, operation_type="SELECT")

        rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row_tuple)) for row_tuple in fetched_tuples]

        if schema_type:
            converted_data = self.to_schema(rows_as_dicts, schema_type=schema_type)
            return SQLResult[ModelDTOT](
                statement=statement, data=list(converted_data), column_names=column_names, operation_type="SELECT"
            )

        return SQLResult[RowT](
            statement=statement, data=rows_as_dicts, column_names=column_names, operation_type="SELECT"
        )

    def _wrap_execute_result(
        self, statement: SQL, result: Union[DMLResultDict, ScriptResultDict], **kwargs: Any
    ) -> SQLResult[RowT]:
        operation_type = "UNKNOWN"
        if statement.expression:
            operation_type = str(statement.expression.key).upper()

        if "statements_executed" in result:
            script_result = cast("ScriptResultDict", result)
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type="SCRIPT",
                metadata={
                    "status_message": script_result.get("status_message", ""),
                    "statements_executed": script_result.get("statements_executed", -1),
                },
            )

        dml_result = cast("DMLResultDict", result)
        rows_affected = dml_result.get("rows_affected", -1)
        status_message = dml_result.get("status_message", "")
        return SQLResult[RowT](
            statement=statement,
            data=[],
            rows_affected=rows_affected,
            operation_type=operation_type,
            metadata={"status_message": status_message},
        )

    def _execute_pipeline_native(self, operations: "list[Any]", **options: Any) -> "list[SQLResult[RowT]]":
        """Native pipeline execution using Oracle's pipeline support.

        Oracle has built-in pipeline support through the create_pipeline() and run_pipeline() API.
        This provides significant performance benefits for batch operations.

        Args:
            operations: List of PipelineOperation objects
            **options: Pipeline configuration options

        Returns:
            List of SQLResult objects from all operations
        """
        import oracledb

        from sqlspec.exceptions import PipelineExecutionError

        results = []
        connection = self._connection()

        try:
            # Create Oracle's native pipeline
            pipeline = oracledb.create_pipeline()

            # Add operations to Oracle pipeline
            for i, op in enumerate(operations):
                if not self._add_pipeline_operation(pipeline, i, op, options, results):
                    continue  # Error was handled with continue_on_error

            # Execute the entire pipeline in one network round-trip
            oracle_results = connection.run_pipeline(pipeline)

            # Convert Oracle results to SQLResult objects
            for i, (op, oracle_result) in enumerate(zip(operations, oracle_results)):
                self._process_pipeline_result(i, op, oracle_result, options, results)

        except Exception as e:
            if not isinstance(e, PipelineExecutionError):
                msg = f"Oracle pipeline execution failed: {e}"
                raise PipelineExecutionError(msg) from e
            raise

        return results

    def _add_pipeline_operation(
        self, pipeline: Any, i: int, op: Any, options: dict[str, Any], results: list[Any]
    ) -> bool:
        """Add a single operation to the Oracle pipeline with error handling.

        Returns:
            True if operation was added successfully, False if error was handled with continue_on_error
        """
        from sqlspec.exceptions import PipelineExecutionError

        try:
            # Apply operation-specific filters
            filtered_sql = self._apply_operation_filters(op.sql, op.filters)
            sql_str = filtered_sql.to_sql(placeholder_style=self.default_parameter_style)
            params = self._convert_oracle_params(filtered_sql.parameters)

            # Add to pipeline based on operation type
            if op.operation_type == "execute_many":
                # Oracle's add_executemany for batch operations
                pipeline.add_executemany(sql_str, params)
            elif op.operation_type == "select":
                # Use fetchall for SELECT statements
                pipeline.add_fetchall(sql_str, params)
            elif op.operation_type == "execute_script":
                # For scripts, split and add each statement
                script_statements = self._split_script_statements(sql_str)
                for stmt in script_statements:
                    if stmt.strip():
                        pipeline.add_execute(stmt)
            else:
                # Regular execute for DML/DDL
                pipeline.add_execute(sql_str, params)

        except Exception as e:
            if options.get("continue_on_error"):
                # Create error result and continue
                error_result = SQLResult[RowT](
                    statement=op.sql, error=e, operation_index=i, parameters=op.original_params
                )
                results.append(error_result)
                return False
            msg = f"Oracle pipeline failed to add operation {i}: {e}"
            raise PipelineExecutionError(
                msg, operation_index=i, partial_results=results, failed_operation=op
            ) from e
        else:
            return True

    def _process_pipeline_result(
        self, i: int, op: Any, oracle_result: Any, options: dict[str, Any], results: list[Any]
    ) -> None:
        """Process a single Oracle pipeline result with error handling."""
        from sqlspec.exceptions import PipelineExecutionError

        try:
            if hasattr(oracle_result, "rows") and oracle_result.rows is not None:
                # SELECT operation - has row data
                if oracle_result.rows:
                    # Extract column names from first row if available
                    column_names = list(oracle_result.rows[0].keys()) if oracle_result.rows else []
                else:
                    column_names = []

                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", oracle_result.rows or []),
                    rows_affected=len(oracle_result.rows) if oracle_result.rows else 0,
                    operation_type="select",
                    metadata={"column_names": column_names},
                )
            else:
                # DML operation - check for row count
                rows_affected = getattr(oracle_result, "rowcount", 0)
                operation_type = op.operation_type

                if op.operation_type == "execute_script":
                    script_statements = self._split_script_statements(op.sql.to_sql())
                    result = SQLResult[RowT](
                        statement=op.sql,
                        data=cast("list[RowT]", []),
                        rows_affected=rows_affected,
                        operation_type="execute_script",
                        metadata={
                            "status_message": "SCRIPT EXECUTED",
                            "statements_executed": len(script_statements),
                        },
                    )
                else:
                    result = SQLResult[RowT](
                        statement=op.sql,
                        data=cast("list[RowT]", []),
                        rows_affected=rows_affected,
                        operation_type=operation_type,
                        metadata={"status_message": "OK"},
                    )

            # Add operation context
            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if options.get("continue_on_error"):
                # Create error result
                error_result = SQLResult[RowT](
                    statement=op.sql, error=e, operation_index=i, parameters=op.original_params
                )
                results.append(error_result)
            else:
                msg = f"Oracle pipeline failed to process result {i}: {e}"
                raise PipelineExecutionError(
                    msg, operation_index=i, partial_results=results, failed_operation=op
                ) from e

    @staticmethod
    def _convert_oracle_params(params: Any) -> Any:
        """Convert parameters to Oracle-compatible format.

        Oracle supports both named (:name) and positional (:1, :2) parameters.
        We prefer named parameters for better readability.

        Args:
            params: Parameters in various formats

        Returns:
            Parameters in Oracle-compatible format
        """
        if params is None:
            return None
        if isinstance(params, dict):
            # Oracle handles dict parameters directly for named placeholders
            return params
        if isinstance(params, (list, tuple)):
            # Convert to list for positional parameters
            return list(params)
        # Single parameter
        return [params]

    @staticmethod
    def _apply_operation_filters(sql: "SQL", filters: "list[Any]") -> "SQL":
        """Apply filters to a SQL object for pipeline operations."""
        if not filters:
            return sql

        result_sql = sql
        for filter_obj in filters:
            if hasattr(filter_obj, "apply"):
                result_sql = filter_obj.apply(result_sql)

        return result_sql


class OracleAsyncDriver(
    AsyncDriverAdapterProtocol[OracleAsyncConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Oracle Async Driver Adapter. Refactored for new protocol."""

    dialect: DialectType = "oracle"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.NAMED_COLON,
        ParameterStyle.POSITIONAL_COLON,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.NAMED_COLON
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False
    __slots__ = ("config", "connection", "default_row_type")

    def __init__(
        self,
        connection: OracleAsyncConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[OracleAsyncConnection] = None
    ) -> AsyncGenerator[AsyncCursor, None]:
        conn_to_use = connection or self.connection
        cursor: AsyncCursor = conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await ensure_async_(cursor.close)()

    async def _execute_statement(
        self, statement: SQL, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
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
            sql, params = statement.compile(placeholder_style=target_style)
            params = self._process_parameters(params)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        params = self._process_parameters(params)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[OracleAsyncConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            if parameters is None:
                await cursor.execute(sql)  # type: ignore[no-untyped-call]
            else:
                await cursor.execute(sql, parameters)  # type: ignore[no-untyped-call]

            # For SELECT statements, extract data while cursor is open
            if self.returns_rows(statement.expression):
                fetched_data = await cursor.fetchall()  # type: ignore[no-untyped-call]
                column_names = [col[0] for col in cursor.description or []]  # type: ignore[attr-defined]
                result: SelectResultDict = {
                    "data": fetched_data,
                    "column_names": column_names,
                    "rows_affected": cursor.rowcount,
                }
                return result
            dml_result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            await cursor.executemany(sql, param_list or [])  # type: ignore[no-untyped-call]
            result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return result

    async def _execute_script(
        self, script: str, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        # Oracle doesn't support multi-statement scripts in a single execute
        # Split the script into individual statements
        statements = self._split_script_statements(script)

        async with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement:
                    statement = statement.strip()
                    if statement:
                        # No need to manually strip semicolons - the splitter handles it
                        await cursor.execute(statement)

        result: ScriptResultDict = {"statements_executed": len(statements), "status_message": "SCRIPT EXECUTED"}
        return result

    async def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)
        arrow_table = await conn.fetch_df_all(
            sql.to_sql(placeholder_style=self.default_parameter_style),
            sql.get_parameters(style=self.default_parameter_style) or [],
        )
        return ArrowResult(statement=sql, data=arrow_table)

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        conn = self._connection(None)

        async with self._get_cursor(conn) as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for oracledb ingestion."
                raise NotImplementedError(msg)

            data_for_ingest = table.to_pylist()
            if not data_for_ingest:
                return 0

            # Generate column placeholders: :1, :2, etc.
            num_columns = len(data_for_ingest[0])
            placeholders = ", ".join(f":{i + 1}" for i in range(num_columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            await cursor.executemany(sql, data_for_ingest)
            return cursor.rowcount

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: SelectResultDict,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,  # pyright: ignore[reportUnusedParameter]
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        fetched_tuples = result["data"]
        column_names = result["column_names"]

        if not fetched_tuples:
            return SQLResult[RowT](statement=statement, data=[], column_names=column_names, operation_type="SELECT")

        rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row_tuple)) for row_tuple in fetched_tuples]

        if schema_type:
            converted_data = self.to_schema(rows_as_dicts, schema_type=schema_type)
            return SQLResult[ModelDTOT](
                statement=statement, data=list(converted_data), column_names=column_names, operation_type="SELECT"
            )
        return SQLResult[RowT](
            statement=statement, data=rows_as_dicts, column_names=column_names, operation_type="SELECT"
        )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        result: Union[DMLResultDict, ScriptResultDict],
        **kwargs: Any,  # pyright: ignore[reportUnusedParameter]
    ) -> SQLResult[RowT]:
        operation_type = "UNKNOWN"
        if statement.expression:
            operation_type = str(statement.expression.key).upper()

        if "statements_executed" in result:
            script_result = cast("ScriptResultDict", result)
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type="SCRIPT",
                metadata={
                    "status_message": script_result.get("status_message", ""),
                    "statements_executed": script_result.get("statements_executed", -1),
                },
            )

        dml_result = cast("DMLResultDict", result)
        rows_affected = dml_result.get("rows_affected", -1)
        status_message = dml_result.get("status_message", "")
        return SQLResult[RowT](
            statement=statement,
            data=[],
            rows_affected=rows_affected,
            operation_type=operation_type,
            metadata={"status_message": status_message},
        )

    async def _execute_pipeline_native(self, operations: "list[Any]", **options: Any) -> "list[SQLResult[RowT]]":
        """Native async pipeline execution using Oracle's pipeline support.

        Oracle has built-in async pipeline support through the create_pipeline() and run_pipeline() API.
        This provides significant performance benefits for batch operations.

        Args:
            operations: List of PipelineOperation objects
            **options: Pipeline configuration options

        Returns:
            List of SQLResult objects from all operations
        """
        import oracledb

        from sqlspec.exceptions import PipelineExecutionError

        results = []
        connection = self._connection()

        try:
            # Create Oracle's native pipeline
            pipeline = oracledb.create_pipeline()

            # Add operations to Oracle pipeline
            for i, op in enumerate(operations):
                if not self._add_async_pipeline_operation(pipeline, i, op, options, results):
                    continue  # Error was handled with continue_on_error

            # Execute the entire pipeline in one network round-trip
            oracle_results = await connection.run_pipeline(pipeline)

            # Convert Oracle results to SQLResult objects
            for i, (op, oracle_result) in enumerate(zip(operations, oracle_results)):
                self._process_async_pipeline_result(i, op, oracle_result, options, results)

        except Exception as e:
            if not isinstance(e, PipelineExecutionError):
                msg = f"Oracle async pipeline execution failed: {e}"
                raise PipelineExecutionError(msg) from e
            raise

        return results

    def _convert_oracle_params(self, params: Any) -> Any:
        """Convert parameters to Oracle-compatible format.

        Oracle supports both named (:name) and positional (:1, :2) parameters.
        We prefer named parameters for better readability.

        Args:
            params: Parameters in various formats

        Returns:
            Parameters in Oracle-compatible format
        """
        if params is None:
            return None
        if isinstance(params, dict):
            # Oracle handles dict parameters directly for named placeholders
            return params
        if isinstance(params, (list, tuple)):
            # Convert to list for positional parameters
            return list(params)
        # Single parameter
        return [params]

    def _apply_operation_filters(self, sql: "SQL", filters: "list[Any]") -> "SQL":
        """Apply filters to a SQL object for pipeline operations."""
        if not filters:
            return sql

        result_sql = sql
        for filter_obj in filters:
            if hasattr(filter_obj, "apply"):
                result_sql = filter_obj.apply(result_sql)

        return result_sql

    def _add_async_pipeline_operation(
        self, pipeline: Any, i: int, op: Any, options: dict[str, Any], results: list[Any]
    ) -> bool:
        """Add a single operation to the Oracle async pipeline with error handling.

        Returns:
            True if operation was added successfully, False if error was handled with continue_on_error
        """
        from sqlspec.exceptions import PipelineExecutionError

        try:
            # Apply operation-specific filters
            filtered_sql = self._apply_operation_filters(op.sql, op.filters)
            sql_str = filtered_sql.to_sql(placeholder_style=self.default_parameter_style)
            params = self._convert_oracle_params(filtered_sql.parameters)

            # Add to pipeline based on operation type
            if op.operation_type == "execute_many":
                # Oracle's add_executemany for batch operations
                pipeline.add_executemany(sql_str, params)
            elif op.operation_type == "select":
                # Use fetchall for SELECT statements
                pipeline.add_fetchall(sql_str, params)
            elif op.operation_type == "execute_script":
                # For scripts, split and add each statement
                script_statements = self._split_script_statements(sql_str)
                for stmt in script_statements:
                    if stmt.strip():
                        pipeline.add_execute(stmt)
            else:
                # Regular execute for DML/DDL
                pipeline.add_execute(sql_str, params)

        except Exception as e:
            if options.get("continue_on_error"):
                # Create error result and continue
                error_result = SQLResult[RowT](
                    statement=op.sql, error=e, operation_index=i, parameters=op.original_params
                )
                results.append(error_result)
                return False
            msg = f"Oracle async pipeline failed to add operation {i}: {e}"
            raise PipelineExecutionError(
                msg, operation_index=i, partial_results=results, failed_operation=op
            ) from e
        else:
            return True

    def _process_async_pipeline_result(
        self, i: int, op: Any, oracle_result: Any, options: dict[str, Any], results: list[Any]
    ) -> None:
        """Process a single Oracle async pipeline result with error handling."""
        from sqlspec.exceptions import PipelineExecutionError

        try:
            if hasattr(oracle_result, "rows") and oracle_result.rows is not None:
                # SELECT operation - has row data
                if oracle_result.rows:
                    # Extract column names from first row if available
                    column_names = list(oracle_result.rows[0].keys()) if oracle_result.rows else []
                else:
                    column_names = []

                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", oracle_result.rows or []),
                    rows_affected=len(oracle_result.rows) if oracle_result.rows else 0,
                    operation_type="select",
                    metadata={"column_names": column_names},
                )
            else:
                # DML operation - check for row count
                rows_affected = getattr(oracle_result, "rowcount", 0)
                operation_type = op.operation_type

                if op.operation_type == "execute_script":
                    script_statements = self._split_script_statements(op.sql.to_sql())
                    result = SQLResult[RowT](
                        statement=op.sql,
                        data=cast("list[RowT]", []),
                        rows_affected=rows_affected,
                        operation_type="execute_script",
                        metadata={
                            "status_message": "SCRIPT EXECUTED",
                            "statements_executed": len(script_statements),
                        },
                    )
                else:
                    result = SQLResult[RowT](
                        statement=op.sql,
                        data=cast("list[RowT]", []),
                        rows_affected=rows_affected,
                        operation_type=operation_type,
                        metadata={"status_message": "OK"},
                    )

            # Add operation context
            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if options.get("continue_on_error"):
                # Create error result
                error_result = SQLResult[RowT](
                    statement=op.sql, error=e, operation_index=i, parameters=op.original_params
                )
                results.append(error_result)
            else:
                msg = f"Oracle async pipeline failed to process result {i}: {e}"
                raise PipelineExecutionError(
                    msg, operation_index=i, partial_results=results, failed_operation=op
                ) from e
