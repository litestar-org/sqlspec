# ruff: noqa: PLR6301
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow as PsycopgDictRow
from sqlglot.dialects.dialect import DialectType

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    AsyncStorageMixin,
    SQLTranslatorMixin,
    SyncStorageMixin,
    ToSchemaMixin,
)
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

logger = get_logger("adapters.psycopg")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[PsycopgDictRow]
PsycopgAsyncConnection = AsyncConnection[PsycopgDictRow]


class PsycopgSyncDriver(
    SyncDriverAdapterProtocol[PsycopgSyncConnection, RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"  # pyright: ignore[reportInvalidTypeForm]
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

    def __init__(
        self,
        connection: PsycopgSyncConnection,
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.PYFORMAT_POSITIONAL

    @staticmethod
    @contextmanager
    def _get_cursor(connection: PsycopgSyncConnection) -> Generator[Any, None, None]:
        with connection.cursor() as cursor:
            yield cursor

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )

        return self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.parameters,  # Use raw merged parameters
            statement,
            connection=connection,
            **kwargs,
        )

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            # TODO: why do we need this? we should use the normal parameter handling
            # Debug logging for parameters
            logger.debug("Raw parameters received: %s (type: %s)", parameters, type(parameters))

            # Convert parameters to the format Psycopg expects
            psycopg_params: Any = None
            if parameters is not None:
                if isinstance(parameters, dict):
                    if not parameters:  # Empty dict
                        psycopg_params = None
                    elif all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                        # Convert to positional list based on param indices
                        param_list = []
                        for i in range(len(parameters)):
                            param_key = f"param_{i}"
                            if param_key in parameters:
                                param_list.append(parameters[param_key])
                        psycopg_params = param_list
                    else:
                        # Use dict as-is for named parameters
                        psycopg_params = parameters
                elif isinstance(parameters, (list, tuple)):
                    psycopg_params = parameters or None
                else:
                    psycopg_params = [parameters]

            if self.instrumentation_config.log_parameters and psycopg_params:
                logger.debug("Query parameters: %s", psycopg_params)

            # Always log final params for debugging
            logger.debug("Final psycopg_params: %s (type: %s)", psycopg_params, type(psycopg_params))

            with self._get_cursor(conn) as cursor:
                # Psycopg accepts tuple, list, dict or None for parameters
                cursor.execute(sql, psycopg_params)

                # For SELECT queries, fetch data while cursor is still open
                is_select = self.returns_rows(statement.expression)
                # If expression is None (parsing disabled or failed), check SQL string
                if not is_select and statement.expression is None:
                    sql_upper = sql.strip().upper()
                    is_select = any(sql_upper.startswith(prefix) for prefix in ["SELECT", "WITH", "VALUES", "TABLE"])

                if is_select:
                    fetched_data = cursor.fetchall()
                    column_names = [col.name for col in cursor.description or []]
                    return {"data": fetched_data, "column_names": column_names}
                # For DML/DDL queries, extract cursor info while it's still valid
                return {
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "description": cursor.description,
                }

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Query parameters (batch): %s", param_list)
            with self._get_cursor(conn) as cursor:
                # Psycopg expects a list of parameter dicts for executemany
                cursor.executemany(sql, param_list or [])
                return {
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "description": cursor.description,
                }

    def _execute_script(
        self,
        script: str,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            with self._get_cursor(conn) as cursor:
                cursor.execute(script)
                # For scripts, return the status message string so base driver can wrap it
                return cursor.statusmessage or "SCRIPT EXECUTED"

    def _wrap_select_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "psycopg_wrap_select", "database"):
            # Result is now a dict with 'data' and 'column_names'
            fetched_data: list[PsycopgDictRow] = result["data"]
            column_names = result["column_names"]
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=fetched_data, schema_type=schema_type)
                # Ensure data is a list for SQLResult
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )
            return SQLResult[RowT](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            # Handle dict result from _execute (DML/DDL operations)
            if isinstance(result, dict) and "rowcount" in result:
                rows_affected = result["rowcount"]
                statusmessage = result.get("statusmessage", "")

                # Handle RETURNING clause data
                returned_data: list[dict[str, Any]] = []
                if result.get("description"):
                    # This means there was a RETURNING clause, but we can't fetch the data
                    # because the cursor is already closed. This is a limitation.
                    logger.debug("RETURNING clause detected but data not available")

                if self.instrumentation_config.log_results_count:
                    logger.debug("Execute operation affected %d rows", rows_affected)

                return SQLResult[RowT](
                    statement=statement,
                    data=returned_data,
                    rows_affected=rows_affected,
                    operation_type=operation_type,
                    metadata={"status_message": statusmessage},
                )

            # Handle cursor object (legacy path, should not be reached with new code)
            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)

            returned_data = []
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if cursor.description:
                    try:
                        fetched_returning_data = cursor.fetchall()
                        if fetched_returning_data:
                            returned_data = [dict(row) for row in fetched_returning_data]
                        if not rows_affected or rows_affected == -1:
                            rows_affected = len(returned_data)
                    except Exception as e:  # pragma: no cover
                        logger.debug("Could not fetch RETURNING data in _wrap_execute_result: %s", e)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if returned_data:
                    logger.debug("RETURNING clause returned %d rows", len(returned_data))

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=[col.name for col in cursor.description or []] if returned_data else [],
            )

    def _connection(self, connection: Optional[PsycopgSyncConnection] = None) -> PsycopgSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, RowT], SQLTranslatorMixin, AsyncStorageMixin, ToSchemaMixin
):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: "DialectType" = "postgres"  # pyright: ignore[reportInvalidTypeForm]
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

    def __init__(
        self,
        connection: PsycopgAsyncConnection,
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=DictRow,
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.PYFORMAT_POSITIONAL

    @staticmethod
    @asynccontextmanager
    async def _get_cursor(connection: PsycopgAsyncConnection) -> AsyncGenerator[Any, None]:
        async with connection.cursor() as cursor:
            yield cursor

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return await self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return await self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )

        return await self._execute(
            statement.to_sql(placeholder_style=self._get_placeholder_style()),
            statement.parameters,  # Use raw merged parameters
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)

            # Convert parameters to the format Psycopg expects
            # TODO: improve this.  why can't use just use parameter parsing?
            psycopg_params: Any = None
            if parameters is not None:
                if isinstance(parameters, dict):
                    if not parameters:  # Empty dict
                        psycopg_params = None
                    elif all(key.startswith("param_") and key[6:].isdigit() for key in parameters):
                        # Convert to positional list based on param indices
                        param_list = []
                        for i in range(len(parameters)):
                            param_key = f"param_{i}"
                            if param_key in parameters:
                                param_list.append(parameters[param_key])
                        psycopg_params = param_list
                    else:
                        # Use dict as-is for named parameters
                        psycopg_params = parameters
                elif isinstance(parameters, (list, tuple)):
                    psycopg_params = parameters or None
                else:
                    psycopg_params = [parameters]

            if self.instrumentation_config.log_parameters and psycopg_params:
                logger.debug("Query parameters: %s", psycopg_params)

            async with self._get_cursor(conn) as cursor:
                # Psycopg accepts tuple, list, dict or None for parameters
                await cursor.execute(sql, psycopg_params)

                # For SELECT queries, fetch data while cursor is still open
                is_select = self.returns_rows(statement.expression)
                # If expression is None (parsing disabled or failed), check SQL string
                if not is_select and statement.expression is None:
                    sql_upper = sql.strip().upper()
                    is_select = any(sql_upper.startswith(prefix) for prefix in ["SELECT", "WITH", "VALUES", "TABLE"])

                if is_select:
                    fetched_data = await cursor.fetchall()
                    column_names = [col.name for col in cursor.description or []]
                    return {"data": fetched_data, "column_names": column_names}
                # For DML/DDL queries, extract cursor info while it's still valid
                return {
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "description": cursor.description,
                }

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Query parameters (batch): %s", param_list)
            async with self._get_cursor(conn) as cursor:
                # Psycopg expects a list of parameter dicts for executemany
                await cursor.executemany(sql, param_list or [])
                return {
                    "rowcount": cursor.rowcount,
                    "statusmessage": cursor.statusmessage,
                    "description": cursor.description,
                }

    async def _execute_script(
        self,
        script: str,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            async with self._get_cursor(conn) as cursor:
                await cursor.execute(script)
                # For scripts, return the status message string so base driver can wrap it
                return cursor.statusmessage or "SCRIPT EXECUTED"

    async def _wrap_select_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        async with instrument_operation_async(self, "psycopg_wrap_select", "database"):
            # Result is now a dict with 'data' and 'column_names'
            fetched_data: list[PsycopgDictRow] = result["data"]
            column_names = result["column_names"]
            rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(data=fetched_data, schema_type=schema_type)
                # Ensure data is a list for SQLResult
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )
            return SQLResult[RowT](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(  # pyright: ignore
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        async with instrument_operation_async(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            # Handle dict result from _execute (DML/DDL operations)
            if isinstance(result, dict) and "rowcount" in result:
                rows_affected = result["rowcount"]
                statusmessage = result.get("statusmessage", "")

                # Handle RETURNING clause data
                returned_data: list[dict[str, Any]] = []
                if result.get("description"):
                    # This means there was a RETURNING clause, but we can't fetch the data
                    # because the cursor is already closed. This is a limitation.
                    logger.debug("RETURNING clause detected but data not available")

                if self.instrumentation_config.log_results_count:
                    logger.debug("Execute operation affected %d rows", rows_affected)

                return SQLResult[RowT](
                    statement=statement,
                    data=returned_data,
                    rows_affected=rows_affected,
                    operation_type=operation_type,
                    metadata={"status_message": statusmessage},
                )

            # Handle cursor object (legacy path, should not be reached with new code)
            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)

            returned_data = []
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if cursor.description:
                    try:
                        fetched_returning_data = await cursor.fetchall()
                        if fetched_returning_data:
                            returned_data = [dict(row) for row in fetched_returning_data]
                            if not rows_affected or rows_affected == -1:
                                rows_affected = len(returned_data)
                    except Exception as e:  # pragma: no cover
                        logger.debug("Could not fetch RETURNING data in async _wrap_execute_result: %s", e)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if returned_data:
                    logger.debug("RETURNING clause returned %d rows", len(returned_data))

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=[col.name for col in cursor.description or []] if returned_data else [],
            )

    def _connection(self, connection: Optional[PsycopgAsyncConnection] = None) -> PsycopgAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
