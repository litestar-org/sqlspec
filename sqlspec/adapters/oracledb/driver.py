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
from sqlspec.typing import DictRow, ModelDTOT, RowT, SQLParameterType
from sqlspec.utils.logging import get_logger
from sqlspec.utils.sync_tools import ensure_async_

__all__ = ("OracleAsyncConnection", "OracleAsyncDriver", "OracleSyncConnection", "OracleSyncDriver")

OracleSyncConnection = Connection
OracleAsyncConnection = AsyncConnection

logger = get_logger("adapters.oracledb")


def _process_oracle_parameters(params: Any) -> Any:
    """Process parameters to handle Oracle-specific requirements.

    - Extract values from TypedParameter objects
    - Convert tuples to lists (Oracle doesn't support tuples)
    """
    from sqlspec.statement.parameters import TypedParameter

    if params is None:
        return None

    # Handle TypedParameter objects
    if isinstance(params, TypedParameter):
        return _process_oracle_parameters(params.value)

    if isinstance(params, tuple):
        # Convert single tuple to list and process each element
        return [_process_oracle_parameters(item) for item in params]
    if isinstance(params, list):
        # Process list of parameter sets
        processed = []
        for param_set in params:
            if isinstance(param_set, tuple):
                # Convert tuple to list and process each element
                processed.append([_process_oracle_parameters(item) for item in param_set])
            elif isinstance(param_set, list):
                # Process each element in the list
                processed.append([_process_oracle_parameters(item) for item in param_set])
            else:
                processed.append(_process_oracle_parameters(param_set))
        return processed
    if isinstance(params, dict):
        # Process dict values
        return {key: _process_oracle_parameters(value) for key, value in params.items()}
    # Return as-is for other types
    return params


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
    __slots__ = ()

    def __init__(
        self,
        connection: OracleSyncConnection,
        config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    def _process_parameters(self, parameters: "SQLParameterType") -> "SQLParameterType":
        """Process parameters to handle Oracle-specific requirements.

        - Extract values from TypedParameter objects
        - Convert tuples to lists (Oracle doesn't support tuples)
        """
        return _process_oracle_parameters(parameters)

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
            # Process parameters to convert tuples to lists for Oracle
            params = self._process_parameters(params)
            # Oracle doesn't like underscores in bind parameter names
            if isinstance(params, list) and params and isinstance(params[0], dict):
                # Fix the SQL and parameters
                for key in list(params[0].keys()):
                    if key.startswith("_arg_"):
                        # Remove leading underscore: _arg_0 -> arg0
                        new_key = key[1:].replace("_", "")
                        sql = sql.replace(f":{key}", f":{new_key}")
                        # Update all parameter sets
                        for param_set in params:
                            if isinstance(param_set, dict) and key in param_set:
                                param_set[new_key] = param_set.pop(key)
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        # Oracle doesn't like underscores in bind parameter names
        if isinstance(params, dict):
            # Fix the SQL and parameters
            for key in list(params.keys()):
                if key.startswith("_arg_"):
                    # Remove leading underscore: _arg_0 -> arg0
                    new_key = key[1:].replace("_", "")
                    sql = sql.replace(f":{key}", f":{new_key}")
                    params[new_key] = params.pop(key)
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
            # Process parameters to extract values from TypedParameter objects
            processed_params = self._process_parameters(parameters) if parameters else []
            cursor.execute(sql, processed_params)

            if self.returns_rows(statement.expression):
                fetched_data = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                return {"data": fetched_data, "column_names": column_names, "rows_affected": cursor.rowcount}

            return {"rows_affected": cursor.rowcount, "status_message": "OK"}

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            # Handle None or empty param_list
            if param_list is None:
                param_list = []
            # Ensure param_list is a list of parameter sets
            elif param_list and not isinstance(param_list, list):
                # Single parameter set, wrap it
                param_list = [param_list]
            elif param_list and not isinstance(param_list[0], (list, tuple, dict)):
                # Already a flat list, likely from incorrect usage
                param_list = [param_list]
            # Parameters have already been processed in _execute_statement
            cursor.executemany(sql, param_list)
            return {"rows_affected": cursor.rowcount, "status_message": "OK"}

    def _execute_script(
        self, script: str, connection: Optional[OracleSyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        statements = self._split_script_statements(script, strip_trailing_semicolon=True)
        with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement and statement.strip():
                    cursor.execute(statement.strip())

        return {"statements_executed": len(statements), "status_message": "SCRIPT EXECUTED"}

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        # Get SQL and parameters using compile to ensure they match
        # For fetch_arrow_table, we need to use POSITIONAL_COLON style since the SQL has :1 placeholders
        sql_str, params = sql.compile(placeholder_style=ParameterStyle.POSITIONAL_COLON)
        if params is None:
            params = []

        # Process parameters to extract values from TypedParameter objects
        processed_params = self._process_parameters(params) if params else []

        oracle_df = conn.fetch_df_all(sql_str, processed_params)
        from pyarrow.interchange.from_dataframe import from_dataframe

        arrow_table = from_dataframe(oracle_df)

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
    __slots__ = ()

    def __init__(
        self,
        connection: OracleAsyncConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    def _process_parameters(self, parameters: "SQLParameterType") -> "SQLParameterType":
        """Process parameters to handle Oracle-specific requirements.

        - Extract values from TypedParameter objects
        - Convert tuples to lists (Oracle doesn't support tuples)
        """
        return _process_oracle_parameters(parameters)

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
            # Process parameters to convert tuples to lists for Oracle
            params = self._process_parameters(params)
            # Oracle doesn't like underscores in bind parameter names
            if isinstance(params, list) and params and isinstance(params[0], dict):
                # Fix the SQL and parameters
                for key in list(params[0].keys()):
                    if key.startswith("_arg_"):
                        # Remove leading underscore: _arg_0 -> arg0
                        new_key = key[1:].replace("_", "")
                        sql = sql.replace(f":{key}", f":{new_key}")
                        # Update all parameter sets
                        for param_set in params:
                            if isinstance(param_set, dict) and key in param_set:
                                param_set[new_key] = param_set.pop(key)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        # Oracle doesn't like underscores in bind parameter names
        if isinstance(params, dict):
            # Fix the SQL and parameters
            for key in list(params.keys()):
                if key.startswith("_arg_"):
                    # Remove leading underscore: _arg_0 -> arg0
                    new_key = key[1:].replace("_", "")
                    sql = sql.replace(f":{key}", f":{new_key}")
                    params[new_key] = params.pop(key)
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
                await cursor.execute(sql)
            else:
                # Process parameters to extract values from TypedParameter objects
                processed_params = self._process_parameters(parameters)
                await cursor.execute(sql, processed_params)

            # For SELECT statements, extract data while cursor is open
            if self.returns_rows(statement.expression):
                fetched_data = await cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
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
            # Handle None or empty param_list
            if param_list is None:
                param_list = []
            # Ensure param_list is a list of parameter sets
            elif param_list and not isinstance(param_list, list):
                # Single parameter set, wrap it
                param_list = [param_list]
            elif param_list and not isinstance(param_list[0], (list, tuple, dict)):
                # Already a flat list, likely from incorrect usage
                param_list = [param_list]
            # Parameters have already been processed in _execute_statement
            await cursor.executemany(sql, param_list)
            result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return result

    async def _execute_script(
        self, script: str, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        # Oracle doesn't support multi-statement scripts in a single execute
        # The splitter now handles PL/SQL blocks correctly when strip_trailing_semicolon=True
        statements = self._split_script_statements(script, strip_trailing_semicolon=True)

        async with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement and statement.strip():
                    await cursor.execute(statement.strip())

        result: ScriptResultDict = {"statements_executed": len(statements), "status_message": "SCRIPT EXECUTED"}
        return result

    async def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        # Get SQL and parameters using compile to ensure they match
        # For fetch_arrow_table, we need to use POSITIONAL_COLON style since the SQL has :1 placeholders
        sql_str, params = sql.compile(placeholder_style=ParameterStyle.POSITIONAL_COLON)
        if params is None:
            params = []

        # Process parameters to extract values from TypedParameter objects
        processed_params = self._process_parameters(params) if params else []

        oracle_df = await conn.fetch_df_all(sql_str, processed_params)
        from pyarrow.interchange.from_dataframe import from_dataframe

        arrow_table = from_dataframe(oracle_df)

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
