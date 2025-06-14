import contextlib
import datetime
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement.parameters import ParameterInfo
    from sqlspec.statement.result import ArrowResult

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection

# Date/time format constants
DATE_FORMAT_LENGTH = 10  # YYYY-MM-DD
TIME_FORMAT_LENGTH = 8  # HH:MM:SS
DATETIME_FORMAT_MIN_LENGTH = 19  # YYYY-MM-DD HH:MM:SS


class AdbcDriver(
    SyncDriverAdapterProtocol["AdbcConnection", RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """ADBC Sync Driver Adapter with modern architecture.

    ADBC (Arrow Database Connectivity) provides a universal interface for connecting
    to multiple database systems with high-performance Arrow-native data transfer.

    This driver provides:
    - Universal connectivity across database backends (PostgreSQL, SQLite, DuckDB, etc.)
    - High-performance Arrow data streaming and bulk operations
    - Intelligent dialect detection and parameter style handling
    - Seamless integration with cloud databases (BigQuery, Snowflake)
    - Driver manager abstraction for easy multi-database support
    """

    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = True
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK,)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK

    def __init__(
        self,
        connection: "AdbcConnection",
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
        self.dialect = self._get_dialect(connection)
        # Set parameter style based on detected dialect
        self.default_parameter_style = self._get_parameter_style_for_dialect(self.dialect)

    @staticmethod
    def _get_dialect(connection: "AdbcConnection") -> str:
        """Get the database dialect based on the driver name.

        Args:
            connection: The ADBC connection object.

        Returns:
            The database dialect.
        """
        try:
            driver_info = connection.adbc_get_info()
            vendor_name = driver_info.get("vendor_name", "").lower()
            driver_name = driver_info.get("driver_name", "").lower()

            if "postgres" in vendor_name or "postgresql" in driver_name:
                return "postgres"
            if "bigquery" in vendor_name or "bigquery" in driver_name:
                return "bigquery"
            if "sqlite" in vendor_name or "sqlite" in driver_name:
                return "sqlite"
            if "duckdb" in vendor_name or "duckdb" in driver_name:
                return "duckdb"
            if "mysql" in vendor_name or "mysql" in driver_name:
                return "mysql"
            if "snowflake" in vendor_name or "snowflake" in driver_name:
                return "snowflake"
            if "flight" in driver_name or "flightsql" in driver_name:
                return "sqlite"
        except Exception:
            logger.warning("Could not reliably determine ADBC dialect from driver info. Defaulting to 'postgres'.")
        return "postgres"

    @staticmethod
    def _get_parameter_style_for_dialect(dialect: str) -> ParameterStyle:
        """Get the parameter style for a given dialect."""
        dialect_style_map = {
            "postgres": ParameterStyle.NUMERIC,
            "postgresql": ParameterStyle.NUMERIC,
            "bigquery": ParameterStyle.NAMED_AT,
            "sqlite": ParameterStyle.QMARK,
            "duckdb": ParameterStyle.QMARK,
            "mysql": ParameterStyle.POSITIONAL_PYFORMAT,
            "snowflake": ParameterStyle.QMARK,
        }
        return dialect_style_map.get(dialect, ParameterStyle.QMARK)

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "AdbcConnection") -> Iterator["Cursor"]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _convert_sql_parameter_style(self, sql: str, target_style: ParameterStyle) -> str:
        """Convert SQL parameter placeholders to the target style.

        Args:
            sql: SQL string with placeholders
            target_style: Target parameter style for the database

        Returns:
            SQL string with converted placeholders
        """
        from sqlspec.statement.parameters import ParameterValidator

        # Extract parameters from the SQL
        validator = ParameterValidator()
        param_info_list = validator.extract_parameters(sql)

        if not param_info_list:
            # No parameters to convert
            return sql

        # Check if conversion is needed
        current_styles = {p.style for p in param_info_list if p.style}
        if len(current_styles) == 1 and target_style in current_styles:
            # SQL already uses the target style
            return sql

        # Convert placeholders from end to start to preserve positions
        result_sql = sql
        for param_info in reversed(param_info_list):
            start_pos = param_info.position
            end_pos = start_pos + len(param_info.placeholder_text)
            new_placeholder = self._get_placeholder_for_style(target_style, param_info)
            result_sql = result_sql[:start_pos] + new_placeholder + result_sql[end_pos:]

        return result_sql

    def _convert_date_time_parameters(self, parameters: Any) -> Any:
        """Convert string date/time parameters to proper Python objects for PostgreSQL.

        Args:
            parameters: Parameters in any format

        Returns:
            Parameters with date/time strings converted to Python objects
        """
        if parameters is None or self.dialect != "postgres":
            return parameters

        def convert_value(value: Any) -> Any:
            """Convert individual value if it's a date/time string."""
            if not isinstance(value, str):
                return value

            # Try to parse as date (YYYY-MM-DD)
            if len(value) == DATE_FORMAT_LENGTH and value[4] == "-" and value[7] == "-":
                try:
                    return datetime.date.fromisoformat(value)
                except ValueError:
                    pass

            # Try to parse as time (HH:MM:SS)
            if len(value) == TIME_FORMAT_LENGTH and value[2] == ":" and value[5] == ":":
                try:
                    return datetime.time.fromisoformat(value)
                except ValueError:
                    pass

            # Try to parse as datetime (YYYY-MM-DD HH:MM:SS)
            if len(value) >= DATETIME_FORMAT_MIN_LENGTH and value[4] == "-" and value[7] == "-" and value[10] == " ":
                try:
                    # Handle with or without timezone
                    if "+" in value or value.endswith("Z"):
                        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
                    return datetime.datetime.fromisoformat(value)
                except ValueError:
                    pass

            return value

        # Convert based on parameter type
        if isinstance(parameters, (list, tuple)):
            return type(parameters)(convert_value(v) for v in parameters)
        if isinstance(parameters, dict):
            return {k: convert_value(v) for k, v in parameters.items()}
        return convert_value(parameters)

    @staticmethod
    def _get_placeholder_for_style(target_style: ParameterStyle, param_info: "ParameterInfo") -> str:
        """Generate placeholder text for the target parameter style."""
        if target_style == ParameterStyle.QMARK:
            return "?"
        if target_style == ParameterStyle.NAMED_COLON:
            return f":{param_info.name}" if param_info.name else f":param_{param_info.ordinal}"
        if target_style == ParameterStyle.POSITIONAL_COLON:
            return f":{param_info.ordinal + 1}"
        if target_style == ParameterStyle.NAMED_DOLLAR:
            return f"${param_info.name}" if param_info.name else f"$param_{param_info.ordinal}"
        if target_style == ParameterStyle.NUMERIC:
            return f"${param_info.ordinal + 1}"  # PostgreSQL-style $1, $2, etc.
        if target_style == ParameterStyle.NAMED_AT:
            return f"@{param_info.name}" if param_info.name else f"@param_{param_info.ordinal}"
        if target_style == ParameterStyle.NAMED_PYFORMAT:
            return f"%({param_info.name})s" if param_info.name else f"%(param_{param_info.ordinal})s"
        if target_style == ParameterStyle.POSITIONAL_PYFORMAT:
            return "%s"
        return param_info.placeholder_text

    def _execute_statement(
        self, statement: SQL, connection: Optional["AdbcConnection"] = None, **kwargs: Any
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
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            # The SQL is already in the correct format from to_sql()
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)

            # The parameters are already in the correct format from get_parameters()
            # Only apply driver-specific type conversions (e.g., date/time for PostgreSQL)
            converted_params = self._convert_date_time_parameters(parameters)

            if self.instrumentation_config.log_parameters and converted_params:
                logger.debug("Query parameters: %s", converted_params)

            # Validate that parameters are provided if the SQL expects them
            # This provides a better error message than the database error
            if statement.expression and hasattr(statement, "parameter_info"):
                param_info = statement.parameter_info
                if param_info and not converted_params:
                    msg = (
                        f"SQL statement requires {len(param_info)} parameters, but none were provided. "
                        "Ensure parameters are correctly passed to the execute() method."
                    )
                    raise ValueError(msg)

            cursor.execute(sql, converted_params or [])

            # Check if this is a SELECT query (returns rows)
            is_select = self.returns_rows(statement.expression)
            # If expression is None (parsing disabled or failed), check SQL string
            if not is_select and statement.expression is None:
                sql_upper = sql.strip().upper()
                is_select = any(sql_upper.startswith(prefix) for prefix in ["SELECT", "WITH", "VALUES", "TABLE"])

            if is_select:
                fetched_data = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                result: SelectResultDict = {
                    "data": fetched_data,
                    "column_names": column_names,
                    "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                }
                return result

            # For non-SELECT statements, return DML result
            dml_result: DMLResultDict = {
                "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                "status_message": "OK",
            }
            return dml_result

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            # The SQL is already in the correct format from to_sql()
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)

            # Convert date/time strings to Python objects for PostgreSQL
            if param_list and self.dialect == "postgres":
                param_list = [self._convert_date_time_parameters(params) for params in param_list]

            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Query parameters (batch): %s", param_list)
            # ADBC expects list of parameter sets
            cursor.executemany(sql, param_list or [])
            # Return DML result dict
            result: DMLResultDict = {
                "rows_affected": cursor.rowcount if cursor.rowcount is not None else -1,
                "status_message": "OK",
            }
            return result

    def _execute_script(
        self, script: str, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        if self.instrumentation_config.log_queries:
            logger.debug("Executing SQL script: %s", script)

        # ADBC drivers don't support multiple statements in a single execute
        # Use the shared implementation to split the script
        statements = self._split_script_statements(script)

        with self._get_cursor(conn) as cursor:
            for statement in statements:
                if statement:
                    if self.instrumentation_config.log_queries:
                        logger.debug("Executing statement: %s", statement)
                    cursor.execute(statement)

        result: ScriptResultDict = {"statements_executed": len(statements), "status_message": "SCRIPT EXECUTED"}
        return result

    def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "adbc_wrap_select", "database"):
            # result must be a dict with keys: data, column_names, rows_affected
            fetched_data = result["data"]
            column_names = result["column_names"]
            rows_affected = result["rows_affected"]

            rows_as_dicts: list[dict[str, Any]] = []
            if fetched_data:
                rows_as_dicts = [dict(zip(column_names, row)) for row in fetched_data]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
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
                data=rows_as_dicts,
                column_names=column_names,
                rows_affected=rows_affected,
                operation_type="SELECT",
            )

    def _wrap_execute_result(
        self, statement: SQL, result: Union[DMLResultDict, ScriptResultDict], **kwargs: Any
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "adbc_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            # Handle TypedDict results
            if isinstance(result, dict):
                # Check if this is a ScriptResultDict
                if "statements_executed" in result:
                    return SQLResult[RowT](
                        statement=statement,
                        data=[],
                        rows_affected=0,
                        operation_type=operation_type or "SCRIPT",
                        metadata={"status_message": result["status_message"]},
                    )

                # Check if this is a DMLResultDict
                if "rows_affected" in result:
                    rows_affected = result["rows_affected"]
                    status_message = result["status_message"]

                    if self.instrumentation_config.log_results_count:
                        logger.debug("Execute operation affected %d rows", rows_affected)

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

    # ============================================================================
    # ADBC Native Arrow Support
    # ============================================================================

    def _fetch_arrow_table(self, sql_obj: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        """ADBC native Arrow table fetching.

        ADBC has excellent native Arrow support through cursor.fetch_arrow_table()
        This provides zero-copy data transfer for optimal performance.

        Args:
            sql_obj: Processed SQL object
            connection: Optional connection override
            **kwargs: Additional options (e.g., batch_size for streaming)

        Returns:
            ArrowResult with native Arrow table
        """
        from sqlspec.statement.result import ArrowResult

        conn = self._connection(connection)

        with wrap_exceptions(), self._get_cursor(conn) as cursor:
            # Execute the query
            cursor.execute(
                sql_obj.to_sql(placeholder_style=self.default_parameter_style),
                sql_obj.get_parameters(style=self.default_parameter_style) or [],
            )

            # Use ADBC's native Arrow fetch
            arrow_table = cursor.fetch_arrow_table()

            if self.instrumentation_config.log_results_count and arrow_table:
                logger.debug("Fetched Arrow table with %d rows", arrow_table.num_rows)

            return ArrowResult(statement=sql_obj, data=arrow_table)

    def _ingest_arrow_table(self, table: "Any", target_table: str, mode: str, **options: Any) -> int:
        """ADBC-optimized Arrow table ingestion using native bulk insert.

        ADBC drivers often support native Arrow table ingestion for high-performance
        bulk loading operations.

        Args:
            table: Arrow table to ingest
            target_table: Target database table name
            mode: Ingestion mode ('append', 'replace', 'create')
            **options: Additional ADBC-specific options

        Returns:
            Number of rows ingested
        """
        self._ensure_pyarrow_installed()

        with wrap_exceptions():
            conn = self._connection(None)

            # Handle different modes
            if mode == "replace":
                self.execute(SQL(f"TRUNCATE TABLE {target_table}"))
            elif mode == "create":
                # For create mode, we would need to infer schema and create table
                # This is complex, so for now just treat as append
                pass

            # Try ADBC native bulk insert if available
            if hasattr(conn, "adbc_ingest"):
                try:
                    # Use ADBC native ingestion
                    rows_inserted = conn.adbc_ingest(target_table, table, mode=mode, **options)
                except (AttributeError, NotImplementedError):
                    # Fall back to generic implementation if native not available
                    pass
                else:
                    if self.instrumentation_config.log_results_count:
                        logger.debug("ADBC ingested %d rows into %s", rows_inserted, target_table)

                    return rows_inserted

            # Generic fallback using batch INSERT
            return super()._ingest_arrow_table(table, target_table, mode, **options)
