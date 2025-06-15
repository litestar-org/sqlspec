from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from oracledb import AsyncConnection, AsyncCursor, Connection, Cursor
from sqlglot.dialects.dialect import DialectType

from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    AsyncStorageMixin,
    SQLTranslatorMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json
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

    def _coerce_boolean(self, value: Any) -> Any:
        """Oracle doesn't have native boolean, convert to number."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """Oracle handles decimal as NUMBER type."""
        if isinstance(value, str):
            from decimal import Decimal

            return Decimal(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """Oracle JSON stored as CLOB, serialize to string."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """Oracle doesn't have native arrays, serialize to JSON string."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

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
            cursor.execute(sql, parameters or [])

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
            cursor.executemany(sql, param_list or [])
            return {"rows_affected": cursor.rowcount, "status_message": "OK"}

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

    def _coerce_boolean(self, value: Any) -> Any:
        """Oracle doesn't have native boolean, convert to number."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """Oracle handles decimal as NUMBER type."""
        if isinstance(value, str):
            from decimal import Decimal

            return Decimal(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """Oracle JSON stored as CLOB, serialize to string."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """Oracle doesn't have native arrays, serialize to JSON string."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

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
                await cursor.execute(sql)
            else:
                await cursor.execute(sql, parameters)

            # For SELECT statements, extract data while cursor is open
            if self.returns_rows(statement.expression):
                fetched_data = await cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                result: SelectResultDict = {
                    "data": fetched_data,
                    "column_names": column_names,
                    "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                }
                return result
            dml_result: DMLResultDict = {
                "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                "status_message": "OK",
            }
            return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[OracleAsyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            await cursor.executemany(sql, param_list or [])
            result: DMLResultDict = {
                "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                "status_message": "OK",
            }
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
