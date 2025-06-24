import contextlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT, is_dict_with_field
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SyncDriverAdapterProtocol["AdbcConnection", RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
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

    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = False  # Not implemented yet
    supports_native_parquet_import: ClassVar[bool] = True
    __slots__ = ("default_parameter_style", "dialect", "supported_parameter_styles")

    def __init__(
        self,
        connection: "AdbcConnection",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)
        self.dialect: DialectType = self._get_dialect(connection)
        self.default_parameter_style = self._get_parameter_style_for_dialect(self.dialect)
        # Override supported parameter styles based on actual dialect capabilities
        self.supported_parameter_styles = self._get_supported_parameter_styles_for_dialect(self.dialect)

    def _coerce_boolean(self, value: Any) -> Any:
        """ADBC boolean handling varies by underlying driver."""
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """ADBC decimal handling varies by underlying driver."""
        if isinstance(value, str):
            return Decimal(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """ADBC JSON handling varies by underlying driver."""
        if self.dialect == "sqlite" and isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """ADBC array handling varies by underlying driver."""
        if self.dialect == "sqlite" and isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

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
    def _get_supported_parameter_styles_for_dialect(dialect: str) -> "tuple[ParameterStyle, ...]":
        """Get the supported parameter styles for a given dialect.

        Each ADBC driver supports different parameter styles based on the underlying database.
        """
        dialect_supported_styles_map = {
            "postgres": (ParameterStyle.NUMERIC,),  # PostgreSQL only supports $1, $2, $3
            "postgresql": (ParameterStyle.NUMERIC,),
            "bigquery": (ParameterStyle.NAMED_AT,),  # BigQuery only supports @param
            "sqlite": (ParameterStyle.QMARK,),  # ADBC SQLite only supports ? (not :param)
            "duckdb": (ParameterStyle.QMARK, ParameterStyle.NUMERIC),  # DuckDB supports ? and $1
            "mysql": (ParameterStyle.POSITIONAL_PYFORMAT,),  # MySQL only supports %s
            "snowflake": (ParameterStyle.QMARK, ParameterStyle.NUMERIC),  # Snowflake supports ? and :1
        }
        return dialect_supported_styles_map.get(dialect, (ParameterStyle.QMARK,))

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "AdbcConnection") -> Iterator["Cursor"]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _execute_statement(
        self, statement: SQL, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        # Determine if we need to convert parameter style
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

        sql, params = statement.compile(placeholder_style=target_style)
        params = self._process_parameters(params)
        if statement.is_many:
            return self._execute_many(sql, params, connection=connection, **kwargs)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            # ADBC expects parameters as a list for most drivers
            if parameters is not None and not isinstance(parameters, (list, tuple)):
                cursor_params = [parameters]
            else:
                cursor_params = parameters  # type: ignore[assignment]

            try:
                cursor.execute(sql, cursor_params or [])
            except Exception as e:
                # Rollback transaction on error for PostgreSQL to avoid "current transaction is aborted" errors
                if self.dialect == "postgres":
                    with contextlib.suppress(Exception):
                        cursor.execute("ROLLBACK")
                raise e from e

            if self.returns_rows(statement.expression):
                fetched_data = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                result: SelectResultDict = {
                    "data": fetched_data,
                    "column_names": column_names,
                    "rows_affected": len(fetched_data),
                }
                return result

            dml_result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return dml_result

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            try:
                cursor.executemany(sql, param_list or [])
            except Exception as e:
                if self.dialect == "postgres":
                    with contextlib.suppress(Exception):
                        cursor.execute("ROLLBACK")
                # Always re-raise the original exception
                raise e from e

            result: DMLResultDict = {"rows_affected": cursor.rowcount, "status_message": "OK"}
            return result

    def _execute_script(
        self, script: str, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        # ADBC drivers don't support multiple statements in a single execute
        # Use the shared implementation to split the script
        statements = self._split_script_statements(script)

        executed_count = 0
        with self._get_cursor(conn) as cursor:
            for statement in statements:
                executed_count += self._execute_single_script_statement(cursor, statement)

        result: ScriptResultDict = {"statements_executed": executed_count, "status_message": "SCRIPT EXECUTED"}
        return result

    def _execute_single_script_statement(self, cursor: "Cursor", statement: str) -> int:
        """Execute a single statement from a script and handle errors.

        Args:
            cursor: The database cursor
            statement: The SQL statement to execute

        Returns:
            1 if successful, 0 if failed
        """
        try:
            cursor.execute(statement)
        except Exception as e:
            # Rollback transaction on error for PostgreSQL
            if self.dialect == "postgres":
                with contextlib.suppress(Exception):
                    cursor.execute("ROLLBACK")
            raise e from e
        else:
            return 1

    def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        # result must be a dict with keys: data, column_names, rows_affected

        rows_as_dicts = [dict(zip(result["column_names"], row)) for row in result["data"]]

        if schema_type:
            return SQLResult[ModelDTOT](
                statement=statement,
                data=list(self.to_schema(data=rows_as_dicts, schema_type=schema_type)),
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
        operation_type = (
            str(statement.expression.key).upper()
            if statement.expression and hasattr(statement.expression, "key")
            else "UNKNOWN"
        )

        # Handle TypedDict results
        if is_dict_with_field(result, "statements_executed"):
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                total_statements=result["statements_executed"],
                operation_type="SCRIPT",  # Scripts always have operation_type SCRIPT
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
        msg = f"Unexpected result type: {type(result)}"
        raise ValueError(msg)

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        """ADBC native Arrow table fetching.

        ADBC has excellent native Arrow support through cursor.fetch_arrow_table()
        This provides zero-copy data transfer for optimal performance.

        Args:
            sql: Processed SQL object
            connection: Optional connection override
            **kwargs: Additional options (e.g., batch_size for streaming)

        Returns:
            ArrowResult with native Arrow table
        """
        self._ensure_pyarrow_installed()
        conn = self._connection(connection)

        with wrap_exceptions(), self._get_cursor(conn) as cursor:
            # Execute the query
            params = sql.get_parameters(style=self.default_parameter_style)
            # ADBC expects parameters as a list for most drivers
            cursor_params = [params] if params is not None and not isinstance(params, (list, tuple)) else params
            cursor.execute(sql.to_sql(placeholder_style=self.default_parameter_style), cursor_params or [])
            arrow_table = cursor.fetch_arrow_table()
            return ArrowResult(statement=sql, data=arrow_table)

    def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        """ADBC-optimized Arrow table ingestion using native bulk insert.

        ADBC drivers often support native Arrow table ingestion for high-performance
        bulk loading operations.

        Args:
            table: Arrow table to ingest
            table_name: Target database table name
            mode: Ingestion mode ('append', 'replace', 'create')
            **options: Additional ADBC-specific options

        Returns:
            Number of rows ingested
        """
        self._ensure_pyarrow_installed()

        conn = self._connection(None)
        with self._get_cursor(conn) as cursor:
            # Handle different modes
            if mode == "replace":
                cursor.execute(SQL(f"TRUNCATE TABLE {table_name}").to_sql(placeholder_style=ParameterStyle.STATIC))
            elif mode == "create":
                msg = "'create' mode is not supported for ADBC ingestion"
                raise NotImplementedError(msg)
            return cursor.adbc_ingest(table_name, table, mode=mode, **options)  # type: ignore[arg-type]
