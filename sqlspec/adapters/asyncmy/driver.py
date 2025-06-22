import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncmy import Connection
from typing_extensions import TypeAlias

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AsyncmyConnection", "AsyncmyDriver")

logger = logging.getLogger("sqlspec")

AsyncmyConnection: TypeAlias = Connection


class AsyncmyDriver(
    AsyncDriverAdapterProtocol[AsyncmyConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "mysql"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.POSITIONAL_PYFORMAT,)
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False
    __slots__ = ()

    def __init__(
        self,
        connection: AsyncmyConnection,
        config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    @asynccontextmanager
    async def _get_cursor(
        self, connection: "Optional[AsyncmyConnection]" = None
    ) -> "AsyncGenerator[Union[Cursor, DictCursor], None]":
        conn = self._connection(connection)
        cursor = await conn.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _execute_statement(
        self, statement: SQL, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Let the SQL object handle parameter style conversion based on dialect support
        sql, params = statement.compile(placeholder_style=self.default_parameter_style)

        if statement.is_many:
            # Process parameter list through type coercion
            params = self._process_parameters(params)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        # Process parameters through type coercion
        params = self._process_parameters(params)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        # AsyncMy doesn't like empty lists/tuples, convert to None
        if not parameters:
            parameters = None
        async with self._get_cursor(conn) as cursor:
            # AsyncMy expects list/tuple parameters or dict for named params
            await cursor.execute(sql, parameters)

            if self.returns_rows(statement.expression):
                # For SELECT queries, fetch data and return SelectResultDict
                data = await cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description or []]
                result: SelectResultDict = {"data": data, "column_names": column_names, "rows_affected": len(data)}
                return result

            # For DML/DDL queries, return DMLResultDict
            dml_result: DMLResultDict = {
                "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                "status_message": "OK",
            }
            return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)

        # Convert parameter list to proper format for executemany
        params_list: list[Union[list[Any], tuple[Any, ...]]] = []
        if param_list and isinstance(param_list, Sequence):
            for param_set in param_list:
                if isinstance(param_set, (list, tuple)):
                    params_list.append(param_set)
                elif param_set is None:
                    params_list.append([])
                else:
                    params_list.append([param_set])

        async with self._get_cursor(conn) as cursor:
            await cursor.executemany(sql, params_list)
            result: DMLResultDict = {
                "rows_affected": cursor.rowcount if cursor.rowcount != -1 else len(params_list),
                "status_message": "OK",
            }
            return result

    async def _execute_script(
        self, script: str, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        # AsyncMy may not support multi-statement scripts without CLIENT_MULTI_STATEMENTS flag
        # Use the shared implementation to split and execute statements individually
        statements = self._split_script_statements(script)
        statements_executed = 0

        async with self._get_cursor(conn) as cursor:
            for statement_str in statements:
                if statement_str:
                    await cursor.execute(statement_str)
                    statements_executed += 1

        result: ScriptResultDict = {"statements_executed": statements_executed, "status_message": "SCRIPT EXECUTED"}
        return result

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        conn = self._connection(None)

        async with self._get_cursor(conn) as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for asyncmy ingestion."
                raise NotImplementedError(msg)

            data_for_ingest = table.to_pylist()
            if not data_for_ingest:
                return 0

            # Generate column placeholders: %s, %s, etc.
            num_columns = len(data_for_ingest[0])
            placeholders = ", ".join("%s" for _ in range(num_columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            await cursor.executemany(sql, data_for_ingest)
            return cursor.rowcount if cursor.rowcount is not None else -1

    async def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: "Optional[type[ModelDTOT]]" = None, **kwargs: Any
    ) -> "Union[SQLResult[ModelDTOT], SQLResult[RowT]]":
        data = result["data"]
        column_names = result["column_names"]
        rows_affected = result["rows_affected"]

        if not data:
            return SQLResult[RowT](
                statement=statement, data=[], column_names=column_names, rows_affected=0, operation_type="SELECT"
            )

        rows_as_dicts = [dict(zip(column_names, row)) for row in data]

        if schema_type:
            converted_data = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(converted_data),
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

        # Handle script results
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

        # Handle DML results
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

    def _connection(self, connection: Optional["AsyncmyConnection"] = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
