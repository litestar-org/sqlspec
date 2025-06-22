import csv
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast

import aiosqlite

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
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    AsyncDriverAdapterProtocol[AiosqliteConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Aiosqlite SQLite Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK
    __slots__ = ()

    def __init__(
        self,
        connection: AiosqliteConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    # AIOSQLite-specific type coercion overrides (same as SQLite)
    def _coerce_boolean(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores booleans as integers (0/1)."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores decimals as strings to preserve precision."""
        if isinstance(value, str):
            return value  # Already a string
        from decimal import Decimal

        if isinstance(value, Decimal):
            return str(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores JSON as strings (requires JSON1 extension)."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """AIOSQLite/SQLite doesn't have native arrays - store as JSON strings."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[AiosqliteConnection] = None
    ) -> AsyncGenerator[aiosqlite.Cursor, None]:
        conn_to_use = connection or self.connection
        conn_to_use.row_factory = aiosqlite.Row
        cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _execute_statement(
        self, statement: SQL, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
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

            # Process parameter list through type coercion
            params = self._process_parameters(params)

            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)

        # Process parameters through type coercion
        params = self._process_parameters(params)

        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        # Convert parameters to the format expected by the SQL
        # Note: SQL was already rendered with appropriate placeholder style in _execute_statement
        if ":param_" in sql or (parameters and isinstance(parameters, dict)):
            # SQL has named placeholders, ensure params are dict
            converted_params = self._convert_parameters_to_driver_format(
                sql, parameters, target_style=ParameterStyle.NAMED_COLON
            )
        else:
            # SQL has positional placeholders, ensure params are list/tuple
            converted_params = self._convert_parameters_to_driver_format(
                sql, parameters, target_style=ParameterStyle.QMARK
            )
        async with self._get_cursor(conn) as cursor:
            # Aiosqlite handles both dict and tuple parameters
            await cursor.execute(sql, converted_params or ())
            if self.returns_rows(statement.expression):
                fetched_data = await cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description or []]
                # Convert to list of dicts or tuples as expected by TypedDict
                data_list: list[Any] = list(fetched_data) if fetched_data else []
                result: SelectResultDict = {
                    "data": data_list,
                    "column_names": column_names,
                    "rows_affected": len(data_list),
                }
                return result
            dml_result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        logger.debug("Executing SQL (executemany): %s", sql)
        if param_list:
            logger.debug("Query parameters (batch): %s", param_list)

        # Convert parameter list to proper format for executemany
        params_list: list[tuple[Any, ...]] = []
        if param_list and isinstance(param_list, Sequence):
            for param_set in param_list:
                param_set = cast("Any", param_set)
                if isinstance(param_set, (list, tuple)):
                    params_list.append(tuple(param_set))
                elif param_set is None:
                    params_list.append(())

        async with self._get_cursor(conn) as cursor:
            await cursor.executemany(sql, params_list)
            result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return result

    async def _execute_script(
        self, script: str, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            await cursor.executescript(script)
        result: ScriptResultDict = {
            "statements_executed": -1,  # AIOSQLite doesn't provide this info
            "status_message": "SCRIPT EXECUTED",
        }
        return result

    async def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Database-specific bulk load implementation."""
        # TODO: convert this to use the storage backend.  it has async support
        if format != "csv":
            msg = f"aiosqlite driver only supports CSV for bulk loading, not {format}."
            raise NotImplementedError(msg)

        conn = await self._create_connection()  # type: ignore[attr-defined]
        try:
            async with self._get_cursor(conn) as cursor:
                if mode == "replace":
                    await cursor.execute(f"DELETE FROM {table_name}")

                # Using sync file IO here as it's a fallback path and aiofiles is not a dependency
                with Path(file_path).open(encoding="utf-8") as f:  # noqa: ASYNC230
                    reader = csv.reader(f, **options)
                    header = next(reader)  # Skip header
                    placeholders = ", ".join("?" for _ in header)
                    sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                    data_iter = list(reader)
                    await cursor.executemany(sql, data_iter)
                    rowcount = cursor.rowcount
                await conn.commit()
                return rowcount
        finally:
            await conn.close()

    async def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: "Optional[type[ModelDTOT]]" = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        fetched_data = result["data"]
        column_names = result["column_names"]
        rows_affected = result["rows_affected"]

        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

        if self.returns_rows(statement.expression):
            converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(converted_data_seq),
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

        if "statements_executed" in result:
            script_result = cast("ScriptResultDict", result)
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type="SCRIPT",
                total_statements=script_result.get("statements_executed", -1),
                metadata={"status_message": script_result.get("status_message", "")},
            )

        if "rows_affected" in result:
            dml_result = cast("DMLResultDict", result)
            rows_affected = dml_result["rows_affected"]
            status_message = dml_result["status_message"]
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )

        # This shouldn't happen with TypedDict approach
        msg = f"Unexpected result type: {type(result)}"
        raise ValueError(msg)

    def _connection(self, connection: Optional[AiosqliteConnection] = None) -> AiosqliteConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
