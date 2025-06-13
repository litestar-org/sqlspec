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
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
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

    __slots__ = (
        "_error_counter",
        "_latency_histogram",
        "_pool_connections_gauge",
        "_pool_latency_histogram",
        "_query_counter",
        "_tracer",
        "config",
        "connection",
        "default_row_type",
        "instrumentation_config",
    )

    __supports_arrow__: "ClassVar[bool]" = True
    __supports_parquet__: "ClassVar[bool]" = False
    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (
        ParameterStyle.QMARK,
        ParameterStyle.NAMED_COLON,
    )
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK

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
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        # SQLite expects tuples for positional parameters
        if isinstance(params, list):
            params = tuple(params)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> Union[SelectResultDict, DMLResultDict]:
        """Execute a single statement with parameters."""
        with instrument_operation(self, "sqlite_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite SQL: %s", sql)

            # Parameters are already in the correct format from compile()
            sqlite_params = parameters

            if self.instrumentation_config.log_parameters and sqlite_params:
                logger.debug("SQLite query parameters: %s", sqlite_params)
            with self._get_cursor(conn) as cursor:
                # SQLite expects tuple or dict parameters
                cursor.execute(sql, sqlite_params or ())
                if self.returns_rows(statement.expression):
                    fetched_data: list[sqlite3.Row] = cursor.fetchall()
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

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> DMLResultDict:
        """Execute a statement many times with a list of parameter tuples."""
        with instrument_operation(self, "sqlite_execute_many", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite SQL (executemany): %s", sql)

            # Convert parameter list to proper format for executemany
            formatted_params: list[tuple[Any, ...]] = []
            if param_list and isinstance(param_list, list):
                for param_set in param_list:
                    if isinstance(param_set, (list, tuple)):
                        formatted_params.append(tuple(param_set))
                    elif param_set is None:
                        formatted_params.append(())
                    else:
                        formatted_params.append((param_set,))

            if self.instrumentation_config.log_parameters and formatted_params:
                logger.debug("SQLite query parameters (executemany): %s", formatted_params)

            with self._get_cursor(conn) as cursor:
                cursor.executemany(sql, formatted_params)
                # Return DML result format for execute_many
                result: DMLResultDict = {
                    "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                    "status_message": "OK",
                }
                return result

    def _execute_script(
        self,
        script: str,
        connection: Optional[SqliteConnection] = None,
        **kwargs: Any,
    ) -> ScriptResultDict:
        """Execute a script on the SQLite connection."""
        with instrument_operation(self, "sqlite_execute_script", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQLite script: %s", script)

            with self._get_cursor(conn) as cursor:
                cursor.executescript(script)
            # Explicitly commit the transaction after script execution
            conn.commit()
            # Return script result format
            result: ScriptResultDict = {
                "statements_executed": -1,  # SQLite doesn't provide this info
                "status_message": "SCRIPT EXECUTED",
            }
            return result

    def _wrap_select_result(
        self,
        statement: SQL,
        result: SelectResultDict,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "sqlite_wrap_select", "database"):
            # result must be a dict with keys: data, column_names, rows_affected
            fetched_data = result["data"]
            column_names = result["column_names"]
            rows_affected = result["rows_affected"]

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
                    rows_affected=rows_affected,
                    operation_type="SELECT",
                )

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", rows_as_dicts),  # Pass the already converted list of dicts
                column_names=column_names,
                rows_affected=rows_affected,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self,
        statement: SQL,
        result: Union[DMLResultDict, ScriptResultDict],
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "sqlite_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            # Handle script results
            if "statements_executed" in result:
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    rows_affected=0,
                    operation_type="SCRIPT",
                    metadata={
                        "status_message": result.get("status_message", ""),
                        "statements_executed": result.get("statements_executed", -1),
                    },
                )

            # Handle DML results (dict with rows_affected, status_message)
            rows_affected = result.get("rows_affected", -1)
            status_message = result.get("status_message", "")

            # SQLite DML operations (without RETURNING) don't populate data
            returned_data: list[dict[str, Any]] = []

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", returned_data),
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )
