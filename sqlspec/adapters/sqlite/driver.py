# ruff: noqa: PLR6301
import contextlib
import logging
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any, ClassVar, Optional, Union, cast

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_operation

__all__ = ("SqliteConnection", "SqliteDriver")

logger = logging.getLogger("sqlspec")

SqliteConnection = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterProtocol[SqliteConnection, dict[str, Any]],
    SQLTranslatorMixin[SqliteConnection],
    ResultConverter,
):
    """SQLite Sync Driver Adapter.

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
    """

    __supports_arrow__: ClassVar[bool] = False
    dialect: str = "sqlite"

    def __init__(
        self,
        connection: SqliteConnection,
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=dict[str, Any],
        )

        # Ensure connection.row_factory is set for dictionary-like row access
        if not isinstance(connection.row_factory, type(sqlite3.Row)):
            connection.row_factory = sqlite3.Row

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK

    @staticmethod
    @contextmanager
    def _get_cursor(connection: SqliteConnection) -> Iterator[sqlite3.Cursor]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[SqliteConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "sqlite_execute", "database"):
            conn = self._connection(connection)
            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

            final_exec_params: Union[tuple[Any, ...], list[tuple[Any, ...]], None] = None

            if is_script:
                # For scripts, parameters should be static (baked into SQL)
                script_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)

                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQLite script: %s", script_sql)

                with self._get_cursor(conn) as cursor:
                    cursor.executescript(script_sql)
                    return "SCRIPT EXECUTED"

            elif is_many:
                # Handle batch operations
                batch_params: list[tuple[Any, ...]] = []
                if parameters and isinstance(parameters, Sequence):
                    for param_set in parameters:
                        if isinstance(param_set, (list, tuple)):
                            batch_params.append(tuple(param_set))
                        elif isinstance(param_set, dict):
                            # Convert dict to tuple based on statement parameters
                            stmt_params = statement.get_parameters(style=self._get_placeholder_style())
                            if isinstance(stmt_params, dict):
                                # Match parameter order
                                batch_params.append(tuple(param_set.get(k, None) for k in stmt_params))
                            else:
                                batch_params.append((param_set,))
                        else:
                            batch_params.append((param_set,))
                final_exec_params = batch_params
            else:
                # Single execution - get parameters from statement
                retrieved_params = statement.get_parameters(style=self._get_placeholder_style())
                if isinstance(retrieved_params, (list, tuple)):
                    final_exec_params = tuple(retrieved_params)
                elif retrieved_params is None:
                    final_exec_params = ()
                else:
                    final_exec_params = (retrieved_params,)

            # Logging
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite SQL: %s", final_sql)

            if self.instrumentation_config.log_parameters and final_exec_params:
                logger.debug("SQLite query parameters: %s", final_exec_params)

            with self._get_cursor(conn) as cursor:
                if is_many:
                    cursor.executemany(
                        final_sql,
                        cast("list[tuple[Any, ...]]", final_exec_params if final_exec_params is not None else []),
                    )
                else:
                    cursor.execute(
                        final_sql, cast("tuple[Any, ...]", final_exec_params if final_exec_params is not None else ())
                    )

                return cursor

    def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[dict[str, Any]]]:
        with instrument_operation(self, "sqlite_wrap_select", "database"):
            cursor = raw_driver_result
            fetched_data = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(fetched_data))

            if schema_type:
                converted_data = self.to_schema(data=fetched_data, schema_type=schema_type)
                return SelectResult[ModelDTOT](
                    statement=statement,
                    data=converted_data,
                    column_names=column_names,
                )
            return SelectResult[dict[str, Any]](
                statement=statement,
                data=fetched_data,
                column_names=column_names,
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,
        **kwargs: Any,
    ) -> ExecuteResult:
        with instrument_operation(self, "sqlite_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            if isinstance(raw_driver_result, str):
                execute_data = {
                    "rows_affected": 0,
                    "last_inserted_id": None,
                    "inserted_ids": [],
                    "returning_data": None,
                    "operation_type": operation_type or "SCRIPT",
                }
                return ExecuteResult(
                    statement=statement,
                    data=execute_data,
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                )

            cursor = raw_driver_result
            rows_affected = getattr(cursor, "rowcount", -1)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            execute_data = {
                "rows_affected": rows_affected,
                "last_inserted_id": None,
                "inserted_ids": [],
                "returning_data": None,
                "operation_type": operation_type,
            }
            return ExecuteResult(
                statement=statement,
                data=execute_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
            )
