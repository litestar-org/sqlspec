import contextlib
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union

from adbc_driver_manager.dbapi import Connection, Cursor

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement.result import ArrowResult

__all__ = ("AdbcConnection", "AdbcDriver")

logger = logging.getLogger("sqlspec")

AdbcConnection = Connection


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

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style based on the detected ADBC dialect."""
        dialect_style_map = {
            "sqlite": ParameterStyle.QMARK,
            "duckdb": ParameterStyle.QMARK,
            "postgres": ParameterStyle.NUMERIC,
            "mysql": ParameterStyle.QMARK,
            "bigquery": ParameterStyle.NAMED_AT,
            "snowflake": ParameterStyle.QMARK,
        }
        return dialect_style_map.get(str(self.dialect), ParameterStyle.QMARK)

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
    @contextmanager
    def _get_cursor(connection: "AdbcConnection") -> Iterator["Cursor"]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()  # type: ignore[no-untyped-call]

    def _execute_statement(
        self,
        statement: SQL,
        connection: Optional["AdbcConnection"] = None,
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
            statement.get_parameters(style=self._get_placeholder_style()),
            statement,
            connection=connection,
            **kwargs,
        )

    def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional["AdbcConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            if self.instrumentation_config.log_parameters and parameters:
                logger.debug("Query parameters: %s", parameters)
            # ADBC accepts various parameter formats based on backend
            cursor.execute(sql, parameters or [])

            if self.returns_rows(statement.expression):
                fetched_data = cursor.fetchall()
                column_names = [col[0] for col in cursor.description or []]
                return {"data": fetched_data, "columns": column_names}

            # For non-SELECT statements, return rowcount
            return {"rowcount": cursor.rowcount}

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional["AdbcConnection"] = None,
        **kwargs: Any,
    ) -> Any:
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and param_list:
                logger.debug("Query parameters (batch): %s", param_list)
            # ADBC expects list of parameter sets
            cursor.executemany(sql, param_list or [])
            return cursor

    def _execute_script(
        self,
        script: str,
        connection: Optional["AdbcConnection"] = None,
        **kwargs: Any,
    ) -> str:
        conn = self._connection(connection)
        if self.instrumentation_config.log_queries:
            logger.debug("Executing SQL script: %s", script)
        with self._get_cursor(conn) as cursor:
            cursor.execute(script)
        return "SCRIPT EXECUTED"

    def _wrap_select_result(  # pyright ignore
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "adbc_wrap_select", "database"):
            fetched_data = result["data"]
            column_names = result["columns"]

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
                    operation_type="SELECT",
                )
            return SQLResult[RowT](
                statement=statement,
                data=rows_as_dicts,
                column_names=column_names,
                operation_type="SELECT",
            )

    def _wrap_execute_result(  # pyright ignore
        self,
        statement: SQL,
        result: Any,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "adbc_wrap_execute", "database"):
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

            rows_affected = result["rowcount"]
            returned_data: list[dict[str, Any]] = []
            column_names_for_returning: list[str] = []

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)
                if returned_data:
                    logger.debug("RETURNING clause potentially returned %d rows", len(returned_data))

            return SQLResult[RowT](
                statement=statement,
                data=returned_data,
                rows_affected=rows_affected,
                operation_type=operation_type,
                column_names=column_names_for_returning,
            )

    # ============================================================================
    # ADBC Native Arrow Support
    # ============================================================================

    def _fetch_arrow_table(
        self,
        sql_obj: SQL,
        connection: "Optional[Any]" = None,
        **kwargs: Any,
    ) -> "ArrowResult":
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
                sql_obj.to_sql(placeholder_style=self._get_placeholder_style()),
                sql_obj.get_parameters(style=self._get_placeholder_style()) or [],
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
                # Truncate table first
                from sqlspec.statement.sql import SQL

                self.execute(SQL(f"DELETE FROM {target_table}"))  # type: ignore[attr-defined]
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
