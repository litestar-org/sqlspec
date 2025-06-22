"""Psqlpy Driver Implementation."""

import io
import logging
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from psqlpy import Connection

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
    from sqlglot.dialects.dialect import DialectType

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(
    AsyncDriverAdapterProtocol[PsqlpyConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Psqlpy Driver Adapter.

    Modern, high-performance driver for PostgreSQL.
    """

    dialect: "DialectType" = "postgres"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.NUMERIC,)
    default_parameter_style: ParameterStyle = ParameterStyle.NUMERIC
    __slots__ = ("config", "connection", "default_row_type")

    def __init__(
        self,
        connection: PsqlpyConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    def _coerce_boolean(self, value: Any) -> Any:
        """PostgreSQL has native boolean support, return as-is."""
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """PostgreSQL has native decimal support."""
        if isinstance(value, str):
            from decimal import Decimal

            return Decimal(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """PostgreSQL has native JSON/JSONB support, return as-is."""
        return value

    def _coerce_array(self, value: Any) -> Any:
        """PostgreSQL has native array support, return as-is."""
        return value

    async def _execute_statement(
        self, statement: SQL, connection: Optional[PsqlpyConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Let the SQL object handle parameter style conversion based on dialect support
        sql, params = statement.compile(placeholder_style=self.default_parameter_style)
        params = self._process_parameters(params)

        if statement.is_many:
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[PsqlpyConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        if self.returns_rows(statement.expression):
            query_result = await conn.fetch(sql, parameters=parameters)
            # Convert query_result to list of dicts
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                dict_rows = [dict(row) for row in query_result if hasattr(row, "__iter__")]  # pyright: ignore
            column_names = list(dict_rows[0].keys()) if dict_rows else []
            return {"data": dict_rows, "column_names": column_names, "rows_affected": len(dict_rows)}
        query_result = await conn.execute(sql, parameters=parameters)
        affected_count = getattr(query_result, "rows_affected", 0) if query_result is not None else -1
        return {"rows_affected": affected_count, "status_message": "OK"}

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsqlpyConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        query_result = await conn.execute_many(sql, param_list or [])
        affected_count = getattr(query_result, "rows_affected", 0) if query_result is not None else -1
        return {"rows_affected": affected_count, "status_message": "OK"}

    async def _execute_script(
        self, script: str, connection: Optional[PsqlpyConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        # psqlpy can execute multi-statement scripts directly
        await conn.execute(script)
        return {
            "statements_executed": -1,  # Not directly supported, but script is executed
            "status_message": "SCRIPT EXECUTED",
        }

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        import pyarrow.csv as pacsv

        conn = self._connection(None)
        if mode == "replace":
            await conn.execute(f"TRUNCATE TABLE {table_name}")
        elif mode == "create":
            msg = "'create' mode is not supported for psqlpy ingestion."
            raise NotImplementedError(msg)

        buffer = io.BytesIO()
        pacsv.write_csv(table, buffer)
        buffer.seek(0)

        await conn.copy_from_query(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)", data=buffer.read())  # pyright: ignore
        return table.num_rows

    async def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        dict_rows = result["data"]
        column_names = result["column_names"]
        rows_affected = result["rows_affected"]

        if schema_type:
            converted_data = self.to_schema(data=dict_rows, schema_type=schema_type)
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(converted_data),
                column_names=column_names,
                rows_affected=rows_affected,
                operation_type="SELECT",
            )
        return SQLResult[RowT](
            statement=statement,
            data=cast("list[RowT]", dict_rows),
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

    def _connection(self, connection: Optional[PsqlpyConnection] = None) -> PsqlpyConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
