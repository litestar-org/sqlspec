# ruff: noqa: PLR6301
import logging
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from psycopg import AsyncConnection, Connection
from psycopg.rows import DictRow as PsycopgDictRow

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, SyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin, SyncArrowMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT
from sqlspec.utils.telemetry import instrument_operation, instrument_operation_async

logger = logging.getLogger("sqlspec")

__all__ = ("PsycopgAsyncConnection", "PsycopgAsyncDriver", "PsycopgSyncConnection", "PsycopgSyncDriver")

PsycopgSyncConnection = Connection[PsycopgDictRow]
PsycopgAsyncConnection = AsyncConnection[PsycopgDictRow]


class PsycopgSyncDriver(
    SyncDriverAdapterProtocol[PsycopgSyncConnection, DictRow],
    SQLTranslatorMixin[PsycopgSyncConnection],
    SyncArrowMixin[PsycopgSyncConnection],
    ResultConverter,
):
    """Psycopg Sync Driver Adapter. Refactored for new protocol."""

    dialect: str = "postgres"
    __supports_arrow__: ClassVar[bool] = False

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
            default_row_type=DictRow,
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.PYFORMAT_NAMED

    @staticmethod
    @contextmanager
    def _get_cursor(connection: PsycopgSyncConnection) -> Generator[Any, None, None]:
        with connection.cursor() as cursor:
            yield cursor

    def _execute_impl(
        self,
        statement: SQL,
        connection: Optional[PsycopgSyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "psycopg_execute", "database"):
            conn = self._connection(connection)
            # config parameter removed, statement.config is source of truth

            final_sql: str
            # Psycopg uses pyformat_named (e.g., %(name)s). It expects a dict for single execute
            # and a list of dicts for executemany.
            # statement.parameters should provide this directly.
            final_driver_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                # Parameters are not passed separately for scripts with psycopg
            else:
                final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
                params_to_execute = statement.parameters

                if statement.is_many:
                    if params_to_execute is not None and isinstance(params_to_execute, list):
                        # Expecting a list of dicts
                        final_driver_params = params_to_execute
                    else:
                        # Should be a list of dicts, or an error/empty list if invalid
                        final_driver_params = []
                elif params_to_execute is not None and isinstance(params_to_execute, dict):
                    final_driver_params = params_to_execute
                else:
                    # For single execute, if params are not a dict (or None), it might be an issue
                    # or imply no parameters. Psycopg execute with pyformat expects a mapping.
                    final_driver_params = {}  # Default to empty dict if not a dict or None

            with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if (
                    self.instrumentation_config.log_parameters
                    and final_driver_params
                    and not (not statement.is_many and final_driver_params == {} and statement.parameters is None)
                ):
                    logger.debug("Query parameters: %s", final_driver_params)

                if statement.is_script:
                    cursor.execute(script_sql=final_sql)  # Pass as script_sql to avoid param interpolation by mistake
                    return cursor.statusmessage or "SCRIPT EXECUTED"

                if statement.is_many:
                    # Ensure final_driver_params is a list of dicts
                    cursor.executemany(final_sql, cast("list[dict[str, Any]]", final_driver_params or []))
                else:
                    # Ensure final_driver_params is a dict or None (psycopg handles None as no params)
                    cursor.execute(final_sql, cast("Optional[dict[str, Any]]", final_driver_params))
                return cursor

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[dict[str, Any]]]:
        with instrument_operation(self, "psycopg_wrap_select", "database"):
            cursor = result
            fetched_data: list[PsycopgDictRow] = cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
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
            return SQLResult[dict[str, Any]](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[dict[str, Any]]:
        with instrument_operation(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)

            returned_data: list[dict[str, Any]] = []
            if hasattr(cursor, "description") and cursor.description:
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

            return SQLResult[dict[str, Any]](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=[col.name for col in cursor.description or []] if returned_data else [],
            )

    def _connection(self, connection: Optional[PsycopgSyncConnection] = None) -> PsycopgSyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    def _select_to_arrow_impl(
        self, stmt_obj: "SQL", connection: "PsycopgSyncConnection", **kwargs: "Any"
    ) -> "ArrowResult":
        msg = "Arrow export is not implemented for Psycopg sync driver."
        raise NotImplementedError(msg)


class PsycopgAsyncDriver(
    AsyncDriverAdapterProtocol[PsycopgAsyncConnection, DictRow],
    SQLTranslatorMixin[PsycopgAsyncConnection],
    AsyncArrowMixin[PsycopgAsyncConnection],
    ResultConverter,
):
    """Psycopg Async Driver Adapter. Refactored for new protocol."""

    dialect: str = "postgres"
    __supports_arrow__: ClassVar[bool] = False

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
        return ParameterStyle.PYFORMAT_NAMED

    @staticmethod
    @asynccontextmanager
    async def _get_cursor(connection: PsycopgAsyncConnection) -> AsyncGenerator[Any, None]:
        async with connection.cursor() as cursor:
            yield cursor

    async def _execute_impl(
        self,
        statement: SQL,
        connection: Optional[PsycopgAsyncConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "psycopg_async_execute", "database"):
            conn = self._connection(connection)
            # config parameter removed, statement.config is source of truth

            final_sql: str
            final_driver_params: Union[dict[str, Any], list[dict[str, Any]], None] = None

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            else:
                final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
                params_to_execute = statement.parameters

                if statement.is_many:
                    if params_to_execute is not None and isinstance(params_to_execute, list):
                        final_driver_params = params_to_execute
                    else:
                        final_driver_params = []
                elif params_to_execute is not None and isinstance(params_to_execute, dict):
                    final_driver_params = params_to_execute
                else:
                    final_driver_params = {}  # Default to empty dict

            async with self._get_cursor(conn) as cursor:
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL: %s", final_sql)

                if (
                    self.instrumentation_config.log_parameters
                    and final_driver_params
                    and not (not statement.is_many and final_driver_params == {} and statement.parameters is None)
                ):
                    logger.debug("Query parameters: %s", final_driver_params)

                if statement.is_script:
                    await cursor.execute(script_sql=final_sql)  # Pass as script_sql
                    return cursor.statusmessage or "SCRIPT EXECUTED"

                if statement.is_many:
                    await cursor.executemany(final_sql, cast("list[dict[str, Any]]", final_driver_params or []))
                else:
                    await cursor.execute(final_sql, cast("Optional[dict[str, Any]]", final_driver_params))
                return cursor

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[dict[str, Any]]]:
        with instrument_operation(self, "psycopg_wrap_select", "database"):
            cursor = result
            fetched_data: list[PsycopgDictRow] = await cursor.fetchall()
            column_names = [col.name for col in cursor.description or []]
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
            return SQLResult[dict[str, Any]](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[dict[str, Any]]:
        with instrument_operation(self, "psycopg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(result, str):
                return SQLResult[dict[str, Any]](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)

            returned_data: list[dict[str, Any]] = []
            if hasattr(cursor, "description") and cursor.description:
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

            return SQLResult[dict[str, Any]](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=[col.name for col in cursor.description or []] if returned_data else [],
            )

    def _connection(self, connection: Optional[PsycopgAsyncConnection] = None) -> PsycopgAsyncConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    async def _select_to_arrow_impl(
        self, stmt_obj: "SQL", connection: "PsycopgAsyncConnection", **kwargs: "Any"
    ) -> "ArrowResult":
        msg = "Arrow export is not implemented for Psycopg async driver."
        raise NotImplementedError(msg)
