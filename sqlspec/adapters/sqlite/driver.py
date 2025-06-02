# ruff: noqa: PLR6301
import contextlib
import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from typing_extensions import TypeAlias

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.statement.mixins import ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig

__all__ = ("SqliteConnection", "SqliteDriver")

logger = logging.getLogger("sqlspec")

SqliteConnection: TypeAlias = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterProtocol[SqliteConnection, RowT],
    SQLTranslatorMixin[SqliteConnection],
    ResultConverter,
):
    """SQLite Sync Driver Adapter.

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
    """

    __supports_arrow__: "ClassVar[bool]" = False
    dialect: str = "sqlite"

    def __init__(
        self,
        connection: "SqliteConnection",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "Optional[type[RowT]]" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type or cast("type[RowT]", dict[str, Any]),
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
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> Any:
        with instrument_operation(self, "sqlite_execute", "database"):
            conn = self._connection(connection)
            # config parameter removed, statement.config is the source of truth

            final_sql: str
            # SQLite uses qmark (?) placeholders. It expects a tuple for single execute
            # and a list of tuples for executemany.
            # statement.parameters should provide this directly.
            final_driver_params: Union[tuple[Any, ...], list[tuple[Any, ...]], None] = None

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                # Parameters are not passed separately for scripts with sqlite (executescript)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQLite script: %s", final_sql)
                with self._get_cursor(conn) as cursor:
                    cursor.executescript(final_sql)
                return "SCRIPT EXECUTED"

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_to_execute = statement.parameters

            if statement.is_many:
                # executemany expects a list of tuples
                if params_to_execute is not None and isinstance(params_to_execute, list):
                    # Ensure each item in the list is a tuple
                    final_driver_params = [
                        tuple(p) if isinstance(p, (list, tuple)) else (p,) for p in params_to_execute
                    ]
                else:
                    final_driver_params = []  # Default to empty list if params are not a list for is_many
            # execute expects a tuple of parameters, or an empty tuple if no params
            elif params_to_execute is not None:
                if isinstance(params_to_execute, (list, tuple)):
                    final_driver_params = tuple(params_to_execute)
                else:  # Single parameter value, wrap in a tuple
                    final_driver_params = (params_to_execute,)
            else:
                final_driver_params = ()  # Empty tuple for no parameters

            # Logging
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite SQL: %s", final_sql)

            if (
                self.instrumentation_config.log_parameters
                and final_driver_params
                and not (not statement.is_many and final_driver_params == () and statement.parameters is None)
            ):
                logger.debug("SQLite query parameters: %s", final_driver_params)

            with self._get_cursor(conn) as cursor:
                if statement.is_many:
                    cursor.executemany(
                        final_sql,
                        cast("list[tuple[Any, ...]]", final_driver_params or []),
                    )
                else:
                    cursor.execute(final_sql, cast("tuple[Any, ...]", final_driver_params))
                return cursor  # Return cursor for _wrap_select_result or _wrap_execute_result

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "sqlite_wrap_select", "database"):
            cursor = result
            fetched_data: list[sqlite3.Row] = cursor.fetchall()
            column_names = [col[0] for col in cursor.description or []]

            # Convert list[sqlite3.Row] to list[dict[str, Any]] first for consistent processing
            rows_as_dicts = [dict(row) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                # Now rows_as_dicts is list[dict[str, Any]], which is compatible with to_schema
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", rows_as_dicts),  # Pass the already converted list of dicts
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "sqlite_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            # result is "SCRIPT EXECUTED" for successful scripts
            if isinstance(result, str) and result == "SCRIPT EXECUTED":
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": result},
                )

            cursor = result
            rows_affected = getattr(cursor, "rowcount", -1)
            last_inserted_id = getattr(cursor, "lastrowid", None)

            # SQLite DML operations (without RETURNING which isn't standardly fetched this way) don't populate cursor.fetchall()
            # So, data is typically empty.
            returned_data: list[dict[str, Any]] = []

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if last_inserted_id is not None:
                    logger.debug("Last inserted ID: %s", last_inserted_id)

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", returned_data),
                rows_affected=rows_affected,
                last_inserted_id=last_inserted_id,
                operation_type=operation_type,
            )
