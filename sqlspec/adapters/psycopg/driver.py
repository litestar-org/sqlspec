import io
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, Optional, Union

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow as PsycopgDictRow
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
from sqlspec.statement.result import ArrowResult, DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
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
    ToSchemaMixin,
):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"  # pyright: ignore[reportInvalidTypeForm]
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.POSITIONAL_PYFORMAT,
        ParameterStyle.NAMED_PYFORMAT,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT
    __slots__ = ("config", "connection", "default_row_type")

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
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
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.execute(sql, parameters)
            if cursor.description is not None:
                fetched_data = cursor.fetchall()
                column_names = [col.name for col in cursor.description]
                result: SelectResultDict = {
                    "data": fetched_data,
                    "column_names": column_names,
                    "rows_affected": cursor.rowcount,
                }
                return result
            dml_result: DMLResultDict = {
                "rows_affected": cursor.rowcount,
                "status_message": cursor.statusmessage or "OK",
            }
            return dml_result

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsycopgSyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.executemany(sql, param_list or [])
            result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": cursor.statusmessage or "OK"}
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

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        with self._get_cursor(conn) as cursor:
            cursor.execute(
                sql.to_sql(placeholder_style=self.default_parameter_style),
                sql.get_parameters(style=self.default_parameter_style) or [],
            )
            arrow_table = cursor.fetch_arrow_table()
            return ArrowResult(statement=sql, data=arrow_table)

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

        if is_dict_with_field(result, "statements_executed"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type=operation_type or "SCRIPT",
                metadata={"status_message": result["status_message"]},
            )

        if is_dict_with_field(result, "rows_affected"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=result["rows_affected"],
                operation_type=operation_type,
                metadata={"status_message": result["status_message"]},
            )

        # This shouldn't happen with TypedDict approach
        msg = f"Unexpected result type: {type(result)}"
        raise ValueError(msg)

    def _connection(self, connection: Optional[PsycopgSyncConnection] = None) -> PsycopgSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    ToSchemaMixin,
):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"  # pyright: ignore[reportInvalidTypeForm]
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.POSITIONAL_PYFORMAT,
        ParameterStyle.NAMED_PYFORMAT,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT
    __slots__ = ("config", "connection", "default_row_type")

    def __init__(self, connection: PsycopgAsyncConnection, config: Optional[SQLConfig] = None) -> None:
        super().__init__(connection=connection, config=config, default_row_type=DictRow)

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
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        psycopg_params = parameters
        async with self._get_cursor(conn) as cursor:
            # Psycopg accepts tuple, list, dict or None for parameters
            await cursor.execute(sql, psycopg_params)

            # Check if the query returned data by examining cursor.description
            # This handles SELECT, INSERT...RETURNING, UPDATE...RETURNING, etc.
            if cursor.description is not None:
                # Query returned data - fetch it
                fetched_data = await cursor.fetchall()
                column_names = [col.name for col in cursor.description]
                result: SelectResultDict = {
                    "data": fetched_data,
                    "column_names": column_names,
                    "rows_affected": cursor.rowcount,
                }
                return result

            # For DML/DDL queries that don't return data
            dml_result: DMLResultDict = {
                "rows_affected": cursor.rowcount,
                "status_message": cursor.statusmessage or "OK",
            }
            return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            # Psycopg expects a list of parameter dicts for executemany
            await cursor.executemany(sql, param_list or [])
            result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": cursor.statusmessage or "OK"}
            return result

    async def _execute_script(
        self, script: str, connection: Optional[PsycopgAsyncConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        async with self._get_cursor(conn) as cursor:
            await cursor.execute(script)
            # For scripts, return script result format
            result: ScriptResultDict = {
                "statements_executed": -1,  # Psycopg doesn't provide this info
                "status_message": cursor.statusmessage or "SCRIPT EXECUTED",
            }
            return result

    async def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        async with self._get_cursor(conn) as cursor:
            await cursor.execute(
                sql.to_sql(placeholder_style=self.default_parameter_style),
                sql.get_parameters(style=self.default_parameter_style) or [],
            )
            arrow_table = await cursor.fetch_arrow_table()
            return ArrowResult(statement=sql, data=arrow_table)

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        import pyarrow.csv as pacsv

        conn = self._connection(None)
        async with self._get_cursor(conn) as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for psycopg ingestion."
                raise NotImplementedError(msg)

            buffer = io.StringIO()
            pacsv.write_csv(table, buffer)
            buffer.seek(0)

            async with cursor.copy(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
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
                operation_type=operation_type or "SCRIPT",
                metadata={"status_message": result["status_message"]},
            )

        if is_dict_with_field(result, "rows_affected"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=result["rows_affected"],
                operation_type=operation_type,
                metadata={"status_message": result["status_message"]},
            )

        # This shouldn't happen with TypedDict approach
        msg = f"Unexpected result type: {type(result)}"
        raise ValueError(msg)

    def _connection(self, connection: Optional[PsycopgAsyncConnection] = None) -> PsycopgAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
