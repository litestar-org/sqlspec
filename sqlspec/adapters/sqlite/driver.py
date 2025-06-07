# ruff: noqa: PLR6301
import contextlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from typing_extensions import TypeAlias

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.config import InstrumentationConfig

__all__ = ("SqliteConnection", "SqliteDriver")

logger = get_logger("adapters.sqlite")

SqliteConnection: TypeAlias = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterProtocol[SqliteConnection, RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """SQLite Sync Driver Adapter with Arrow/Parquet export support.

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
    """

    __supports_arrow__: "ClassVar[bool]" = True
    __supports_parquet__: "ClassVar[bool]" = False
    dialect: "DialectType" = "sqlite"
    parameter_style: ParameterStyle = ParameterStyle.QMARK

    def __init__(
        self,
        connection: "SqliteConnection",
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

        # Ensure connection.row_factory is set for dictionary-like row access
        if not isinstance(connection.row_factory, type(sqlite3.Row)):
            connection.row_factory = sqlite3.Row

    @staticmethod
    @contextmanager
    def _get_cursor(connection: SqliteConnection) -> Iterator[sqlite3.Cursor]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC), connection=connection, **kwargs
            )

        params_to_execute = statement.parameters
        has_named_params = (
            params_to_execute is not None and isinstance(params_to_execute, dict) and not statement.is_many
        )

        if statement.is_many:
            sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            param_list = (
                [tuple(p) if isinstance(p, (list, tuple)) else (p,) for p in params_to_execute]
                if params_to_execute is not None and isinstance(params_to_execute, list)
                else []
            )
            return self._execute_many(sql, param_list, connection=connection, **kwargs)

        if has_named_params:
            sql = statement.to_sql(placeholder_style=ParameterStyle.NAMED_COLON)
            params = params_to_execute
        elif params_to_execute is not None:
            sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params = tuple(params_to_execute) if isinstance(params_to_execute, (list, tuple)) else (params_to_execute,)
        else:
            sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params = ()
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a single statement with parameters."""
        with instrument_operation(self, "sqlite_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite SQL: %s", sql)
            if (
                self.instrumentation_config.log_parameters
                and parameters
                and not (not statement.is_many and parameters == () and statement.parameters is None)
            ):
                logger.debug("SQLite query parameters: %s", parameters)
            with self._get_cursor(conn) as cursor:
                if isinstance(parameters, dict):
                    cursor.execute(sql, parameters)
                else:
                    cursor.execute(sql, parameters or ())
                if self.returns_rows(statement.expression):
                    fetched_data: list[sqlite3.Row] = cursor.fetchall()
                    column_names = [col[0] for col in cursor.description or []]
                    return {"data": fetched_data, "column_names": column_names, "rowcount": cursor.rowcount}
                return cursor.rowcount

    def _execute_many(
        self,
        sql: str,
        param_list: list[tuple[Any, ...]],
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> int:
        """Execute a statement many times with a list of parameter tuples."""
        with instrument_operation(self, "sqlite_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters:
                logger.debug("SQLite query parameters (executemany): %s", param_list)
            with self._get_cursor(conn) as cursor:
                cursor.executemany(sql, param_list)
                return cursor.rowcount

    def _execute_script(
        self,
        script: str,
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a script on the SQLite connection."""
        with instrument_operation(self, "sqlite_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite script: %s", script)

            with self._get_cursor(conn) as cursor:
                cursor.executescript(script)
            # Explicitly commit the transaction after script execution
            conn.commit()
            return "SCRIPT EXECUTED"

    def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "sqlite_wrap_select", "database"):
            # result should be a dict with keys: data, column_names, rowcount
            if isinstance(result, dict):
                fetched_data = result.get("data", [])
                column_names = result.get("column_names", [])
            else:
                # Fallback for backward compatibility
                cursor = result
                fetched_data = cursor.fetchall()
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
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
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

            # result is an integer (rowcount) for DML operations
            if isinstance(result, int):
                rows_affected = result
                last_inserted_id = None
            else:
                # Fallback: assume cursor object
                cursor = result
                rows_affected = getattr(cursor, "rowcount", -1)
                last_inserted_id = getattr(cursor, "lastrowid", None)

            # SQLite DML operations (without RETURNING which isn't typically fetched this way) don't populate cursor.fetchall()
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
                operation_type=operation_type,
                last_inserted_id=last_inserted_id,
            )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.QMARK
