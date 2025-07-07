import contextlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional, cast

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.connection import managed_transaction_sync
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncAdapterCacheMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.driver.parameters import convert_parameter_sequence
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, RowT
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


class AdbcDriver(
    SyncDriverAdapterProtocol["AdbcConnection", RowT],
    SyncAdapterCacheMixin,
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

    def __init__(
        self,
        connection: "AdbcConnection",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        dialect = self._get_dialect(connection)
        if config and not config.dialect:
            config = replace(config, dialect=dialect)
        elif not config:
            # Create config with dialect
            config = SQLConfig(dialect=dialect)

        super().__init__(connection=connection, config=config, default_row_type=default_row_type)
        self.dialect: DialectType = dialect
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
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
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

        sql, params = self._get_compiled_sql(statement, target_style)
        params = self._process_parameters(params)
        if statement.is_many:
            return self._execute_many(sql, params, connection=connection, **kwargs)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            converted_params = convert_parameter_sequence(parameters)
            if converted_params is not None and not isinstance(converted_params, (list, tuple)):
                cursor_params = [converted_params]
            else:
                cursor_params = converted_params

            with self._get_cursor(txn_conn) as cursor:
                try:
                    cursor.execute(sql, cursor_params or [])
                except Exception as e:
                    # Rollback transaction on error for PostgreSQL to avoid
                    # "current transaction is aborted" errors
                    if self.dialect == "postgres":
                        with contextlib.suppress(Exception):
                            cursor.execute("ROLLBACK")
                    raise e from e

                if self.returns_rows(statement.expression):
                    fetched_data = cursor.fetchall()
                    column_names = [col[0] for col in cursor.description or []]

                    if fetched_data and isinstance(fetched_data[0], tuple):
                        dict_data: list[dict[Any, Any]] = [dict(zip(column_names, row)) for row in fetched_data]
                    else:
                        dict_data = fetched_data  # type: ignore[assignment]

                    return SQLResult(
                        statement=statement,
                        data=cast("list[RowT]", dict_data),
                        column_names=column_names,
                        rows_affected=len(dict_data),
                        operation_type="SELECT",
                    )

                operation_type = self._determine_operation_type(statement)
                return SQLResult(
                    statement=statement,
                    data=cast("list[RowT]", []),
                    rows_affected=cursor.rowcount,
                    operation_type=operation_type,
                    metadata={"status_message": "OK"},
                )

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)

            with self._get_cursor(txn_conn) as cursor:
                try:
                    cursor.executemany(sql, converted_param_list or [])
                except Exception as e:
                    if self.dialect == "postgres":
                        with contextlib.suppress(Exception):
                            cursor.execute("ROLLBACK")
                    # Always re-raise the original exception
                    raise e from e

                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=cursor.rowcount,
                    operation_type="EXECUTE",
                    metadata={"status_message": "OK"},
                )

    def _execute_script(
        self, script: str, connection: Optional["AdbcConnection"] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # ADBC drivers don't support multiple statements in a single execute
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0
            with self._get_cursor(txn_conn) as cursor:
                for statement in statements:
                    if statement.strip():
                        # Validate each statement unless warnings suppressed
                        if not suppress_warnings:
                            # Run validation through pipeline
                            temp_sql = SQL(statement, config=self.config)
                            temp_sql._ensure_processed()
                            # Validation errors are logged as warnings by default

                        rows = self._execute_single_script_statement(cursor, statement)
                        executed_count += 1
                        total_rows += rows

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": "SCRIPT EXECUTED"},
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    def _execute_single_script_statement(self, cursor: "Cursor", statement: str) -> int:
        """Execute a single statement from a script and handle errors.

        Args:
            cursor: The database cursor
            statement: The SQL statement to execute

        Returns:
            Number of rows affected
        """
        try:
            cursor.execute(statement)
        except Exception as e:
            # Rollback transaction on error for PostgreSQL to avoid
            # "current transaction is aborted" errors
            if self.dialect == "postgres":
                with contextlib.suppress(Exception):
                    cursor.execute("ROLLBACK")
            raise e from e
        else:
            return cursor.rowcount or 0

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
            if mode == "replace":
                cursor.execute(
                    SQL(f"TRUNCATE TABLE {table_name}", _dialect=self.dialect).to_sql(
                        placeholder_style=ParameterStyle.STATIC
                    )
                )
            elif mode == "create":
                msg = "'create' mode is not supported for ADBC ingestion"
                raise NotImplementedError(msg)
            return cursor.adbc_ingest(table_name, table, mode=mode, **options)  # type: ignore[arg-type]

    def _connection(self, connection: Optional["AdbcConnection"] = None) -> "AdbcConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
