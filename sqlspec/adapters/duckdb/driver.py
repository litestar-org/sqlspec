# ruff: noqa: PLR6301
import contextlib
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from duckdb import DuckDBPyConnection

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ArrowTable, DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.config import InstrumentationConfig
    from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict
    from sqlspec.typing import ArrowTable

__all__ = ("DuckDBConnection", "DuckDBDriver")


DuckDBConnection = DuckDBPyConnection


logger = get_logger("adapters.duckdb")


class DuckDBDriver(
    SyncDriverAdapterProtocol["DuckDBConnection", RowT], SQLTranslatorMixin, SyncStorageMixin, ToSchemaMixin
):
    """DuckDB Sync Driver Adapter with modern architecture.

    DuckDB is a fast, in-process analytical database built for modern data analysis.
    This driver provides:

    - High-performance columnar query execution
    - Excellent Arrow integration for analytics workloads
    - Direct file querying (CSV, Parquet, JSON) without imports
    - Extension ecosystem for cloud storage and formats
    - Zero-copy operations where possible
    """

    dialect: "DialectType" = "duckdb"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NUMERIC)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True

    def __init__(
        self,
        connection: "DuckDBConnection",
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

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "DuckDBConnection") -> Generator["DuckDBConnection", None, None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _execute_statement(
        self, statement: SQL, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "Union[SelectResultDict, DMLResultDict, ScriptResultDict]":
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        if statement.is_many:
            sql, params = statement.compile(placeholder_style=self.default_parameter_style)
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=self.default_parameter_style)
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "Union[SelectResultDict, DMLResultDict]":
        conn = self._connection(connection)

        if self.instrumentation_config.log_queries:
            logger.debug("Executing SQL: %s", sql)

        if self.instrumentation_config.log_parameters and parameters:
            logger.debug("Query parameters: %s", parameters)

        # Use connection.execute() which returns a DuckDBPyRelation
        result = conn.execute(sql, parameters or [])

        if self.returns_rows(statement.expression):
            # For SELECT statements, fetch data and return with columns
            fetched_data = result.fetchall()
            column_names = [col[0] for col in result.description or []]
            return {"data": fetched_data, "column_names": column_names, "rows_affected": len(fetched_data)}

        # For DML statements, get affected rows from the result
        row = result.fetchone()
        rows_affected = row[0] if row else 0
        return {"rows_affected": rows_affected}

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "DMLResultDict":
        conn = self._connection(connection)
        if self.instrumentation_config.log_queries:
            logger.debug("Executing SQL (executemany): %s", sql)
        if self.instrumentation_config.log_parameters and param_list:
            logger.debug("Query parameters (batch): %s", param_list)

        with self._get_cursor(conn) as cursor:
            cursor.executemany(sql, param_list or [])
            return {"rows_affected": cursor.rowcount}

    def _execute_script(
        self, script: str, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "ScriptResultDict":
        conn = self._connection(connection)
        if self.instrumentation_config.log_queries:
            logger.debug("Executing SQL script: %s", script)

        # Use a cursor to execute the script
        with self._get_cursor(conn) as cursor:
            cursor.execute(script)

        # DuckDB does not provide detailed results from script execution,
        # so we return a success message.
        return {
            "statements_executed": -1,  # Not supported
            "status_message": "Script executed successfully.",
            "description": "The script was sent to the database.",
        }

    def _wrap_select_result(
        self, statement: SQL, result: "SelectResultDict", schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        with instrument_operation(self, "duckdb_wrap_select", "database"):
            fetched_tuples = result["data"]
            column_names = result["column_names"]
            rows_affected = result["rows_affected"]

            rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row)) for row in fetched_tuples]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=list(converted_data),
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
        self, statement: SQL, result: "Union[DMLResultDict, ScriptResultDict]", **kwargs: Any
    ) -> SQLResult[RowT]:
        with instrument_operation(self, "duckdb_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            if statement.expression:
                operation_type = str(statement.expression.key).upper()

            # Check if this is a ScriptResultDict
            if "statements_executed" in result:
                script_result = cast("ScriptResultDict", result)
                return SQLResult[RowT](
                    statement=statement,
                    data=[],
                    rows_affected=0,
                    operation_type=operation_type or "SCRIPT",
                    metadata={"status_message": script_result.get("status_message", "")},
                )

            # Otherwise, assume DMLResultDict
            dml_result = cast("DMLResultDict", result)
            rows_affected = dml_result.get("rows_affected", -1)
            status_message = dml_result.get("status_message", "")

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows", rows_affected)

            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )

    # ============================================================================
    # DuckDB Native Arrow Support
    # ============================================================================

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: Any) -> "ArrowResult":
        """Enhanced DuckDB native Arrow table fetching with streaming support.

        DuckDB has excellent native Arrow support through execute().arrow()
        This is much faster than fetching rows and converting to Arrow.

        Args:
            sql: Processed SQL object
            connection: Optional connection override
            **kwargs: Additional options (batch_size for streaming, etc.)

        Returns:
            ArrowResult with native Arrow table
        """
        conn = self._connection(connection)

        with wrap_exceptions():
            # Use DuckDB's native execute() with parameters for optimal performance
            sql_string, parameters = sql.compile(placeholder_style=self.default_parameter_style)

            if self.instrumentation_config.log_queries:
                logger.debug("Executing DuckDB Arrow query: %s", sql_string)
            if self.instrumentation_config.log_parameters and parameters:
                logger.debug("DuckDB Arrow query parameters: %s", parameters)

            # Execute with proper parameters
            result = conn.execute(sql_string, parameters or [])

            # Check for streaming batch_size option
            batch_size = kwargs.get("batch_size")
            if batch_size:
                # Streaming mode - use RecordBatchReader for large datasets
                arrow_reader = result.fetch_record_batch(batch_size)
                # Convert to table by reading all batches
                import pyarrow as pa

                batches = []
                while True:
                    try:
                        batch = arrow_reader.read_next_batch()
                        batches.append(batch)
                    except StopIteration:  # noqa: PERF203
                        break

                arrow_table = pa.Table.from_batches(batches) if batches else pa.table({})

                if self.instrumentation_config.log_results_count:
                    logger.debug(
                        "Fetched Arrow table (streaming) with %d rows in %d batches", arrow_table.num_rows, len(batches)
                    )
            else:
                # Direct Arrow table - zero-copy from DuckDB
                arrow_table = result.arrow()

                if self.instrumentation_config.log_results_count:
                    logger.debug("Fetched Arrow table (zero-copy) with %d rows", arrow_table.num_rows)

            return ArrowResult(statement=sql, data=arrow_table)

    # ============================================================================
    # DuckDB Native Storage Operations (Override base implementations)
    # ============================================================================

    def _has_native_capability(self, operation: str, uri: str = "", format: str = "") -> bool:
        """Check if DuckDB has native capability for the operation."""
        # DuckDB has excellent native support for many formats and operations
        if format:
            format_lower = format.lower()
            if operation == "export" and format_lower in {"parquet", "csv", "json"}:
                return True
            if operation == "import" and format_lower in {"parquet", "csv", "json"}:
                return True
            if operation == "read" and format_lower == "parquet":
                return True
        return False

    def _export_native(self, query: str, destination_uri: str, format: str, **options: Any) -> int:
        """Enhanced DuckDB native COPY TO with advanced options support."""
        with instrument_operation(self, "duckdb_export_native", "database"):
            connection = self._connection(None)

            # Build advanced COPY TO statement with options
            copy_options = []

            if format.lower() == "parquet":
                copy_options.append("FORMAT PARQUET")

                # Add Parquet-specific options
                if "compression" in options:
                    copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
                if "row_group_size" in options:
                    copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

                # Handle partitioning
                if "partition_by" in options:
                    partition_by = options["partition_by"]
                    partition_cols = [partition_by] if isinstance(partition_by, str) else partition_by
                    partition_str = ", ".join(partition_cols)
                    copy_options.append(f"PARTITION_BY ({partition_str})")

            elif format.lower() == "csv":
                copy_options.extend(("FORMAT CSV", "HEADER"))

                # Add CSV-specific options
                if "compression" in options:
                    copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
                if "delimiter" in options:
                    copy_options.append(f"DELIMITER '{options['delimiter']}'")
                if "quote" in options:
                    copy_options.append(f"QUOTE '{options['quote']}'")

            elif format.lower() == "json":
                copy_options.append("FORMAT JSON")

                # Add JSON-specific options
                if "compression" in options:
                    copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")

            else:
                msg = f"Unsupported format for DuckDB native export: {format}"
                raise ValueError(msg)

            # Build final COPY statement
            options_str = f"({', '.join(copy_options)})" if copy_options else ""
            copy_sql = f"COPY ({query}) TO '{destination_uri}' {options_str}"

            if self.instrumentation_config.log_queries:
                logger.debug("Executing DuckDB export: %s", copy_sql)

            # Execute the COPY TO statement
            result = connection.execute(copy_sql).fetchone()
            rows_exported = result[0] if result and isinstance(result, tuple) else 0

            if self.instrumentation_config.log_results_count:
                logger.debug("Exported %d rows to %s", rows_exported, destination_uri)

            return rows_exported if rows_exported is not None else 0

    def _import_native(self, source_uri: str, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Use DuckDB's native reading capabilities for efficient import."""
        with instrument_operation(self, "duckdb_import_native", "database"):
            connection = self._connection(None)
            # Determine read function based on format
            if format == "parquet":
                read_func = f"read_parquet('{source_uri}')"
            elif format == "csv":
                read_func = f"read_csv_auto('{source_uri}')"
            elif format == "json":
                read_func = f"read_json_auto('{source_uri}')"
            else:
                msg = f"Unsupported format for DuckDB native import: {format}"
                raise ValueError(msg)

            # Handle different import modes
            if mode == "create":
                sql = f"CREATE TABLE {table_name} AS SELECT * FROM {read_func}"
            elif mode == "replace":
                sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {read_func}"
            elif mode == "append":
                sql = f"INSERT INTO {table_name} SELECT * FROM {read_func}"
            else:
                msg = f"Unsupported import mode: {mode}"
                raise ValueError(msg)

            # Execute the import
            result = connection.execute(sql).fetchone()
            if result:
                rows_imported = int(result[0]) if isinstance(result, tuple) else 0
                logger.debug("Imported %d rows to table %s", rows_imported, table_name)
                return rows_imported

            # If no result, get count from the table
            count_result = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            return int(count_result[0]) if count_result else 0

    def _read_parquet_native(
        self, source_uri: str, columns: Optional[list[str]] = None, **options: Any
    ) -> "SQLResult[dict[str, Any]]":
        """Enhanced DuckDB native Parquet reader with multiple file and filter support."""

        with instrument_operation(self, "duckdb_read_parquet_native", "database"):
            connection = self._connection(None)

            # Handle different source types
            if isinstance(source_uri, list):
                # Multiple files - use DuckDB's list syntax
                file_list = "[" + ", ".join(f"'{f}'" for f in source_uri) + "]"
                read_func = f"read_parquet({file_list})"
            elif "*" in source_uri or "?" in source_uri:
                # Glob pattern - DuckDB handles this natively
                read_func = f"read_parquet('{source_uri}')"
            else:
                # Single file
                read_func = f"read_parquet('{source_uri}')"

            # Build column selection
            column_list = ", ".join(columns) if columns else "*"

            # Build query with potential filters
            query = f"SELECT {column_list} FROM {read_func}"

            # Add filters from options if provided
            filters = options.get("filters")
            if filters:
                where_clauses = []
                filter_spec_parts = 3
                for filter_spec in filters:
                    if len(filter_spec) == filter_spec_parts:
                        col, op, val = filter_spec
                        if isinstance(val, str):
                            where_clauses.append(f"'{col}' {op} '{val}'")
                        else:
                            where_clauses.append(f"'{col}' {op} {val}")
                if where_clauses:
                    query += " WHERE " + " AND ".join(where_clauses)

            if self.instrumentation_config.log_queries:
                logger.debug("Executing DuckDB Parquet query: %s", query)

            # DuckDB can read Parquet and return as Arrow table directly
            arrow_table = connection.execute(query).arrow()

            if self.instrumentation_config.log_results_count:
                logger.debug("Read %d rows from Parquet source: %s", arrow_table.num_rows, source_uri)

            # Convert Arrow table to dict rows for SQLResult compatibility
            arrow_dict = arrow_table.to_pydict()
            column_names = arrow_table.column_names
            num_rows = arrow_table.num_rows

            # Convert columnar dict to row-wise dicts
            rows = []
            for i in range(num_rows):
                row = {col: arrow_dict[col][i] for col in column_names}
                rows.append(row)

            return SQLResult[dict[str, Any]](
                statement=SQL(query),
                data=rows,
                column_names=column_names,
                rows_affected=num_rows,
                operation_type="SELECT",
            )

    def _write_parquet_native(self, data: Union[str, "ArrowTable"], destination_uri: str, **options: Any) -> None:
        """Enhanced DuckDB native Parquet writer with advanced options."""

        with instrument_operation(self, "duckdb_write_parquet_native", "database"):
            connection = self._connection(None)

            # Build COPY options
            copy_options = ["FORMAT PARQUET"]

            # Add Parquet-specific options
            if "compression" in options:
                copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
            if "row_group_size" in options:
                copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

            options_str = f"({', '.join(copy_options)})"

            if isinstance(data, str):
                # Direct query to Parquet
                copy_sql = f"COPY ({data}) TO '{destination_uri}' {options_str}"

                if self.instrumentation_config.log_queries:
                    logger.debug("Executing DuckDB Parquet write (query): %s", copy_sql)

                connection.execute(copy_sql)
            else:
                # Arrow table to Parquet
                temp_name = f"_arrow_data_{uuid.uuid4().hex[:8]}"
                connection.register(temp_name, data)

                try:
                    copy_sql = f"COPY {temp_name} TO '{destination_uri}' {options_str}"

                    if self.instrumentation_config.log_queries:
                        logger.debug("Executing DuckDB Parquet write (Arrow): %s", copy_sql)

                    connection.execute(copy_sql)

                    if self.instrumentation_config.log_results_count:
                        logger.debug("Wrote Arrow table with %d rows to %s", data.num_rows, destination_uri)

                finally:
                    with contextlib.suppress(Exception):
                        connection.unregister(temp_name)

    def _ingest_arrow_table(self, table: "ArrowTable", target_table: str, mode: str, **options: Any) -> int:
        """DuckDB-optimized Arrow table ingestion using native registration.

        DuckDB can directly register Arrow tables in memory and use them in SQL queries,
        providing zero-copy data transfer for optimal performance.

        Args:
            table: Arrow table to ingest
            target_table: Target database table name
            mode: Ingestion mode ('append', 'replace', 'create')
            **options: Additional options

        Returns:
            Number of rows ingested
        """
        self._ensure_pyarrow_installed()

        with wrap_exceptions():
            connection = self._connection(None)
            temp_name = f"_arrow_temp_{uuid.uuid4().hex[:8]}"

            try:
                # Register Arrow table with DuckDB
                connection.register(temp_name, table)

                if mode == "create":
                    # Use sqlglot to build safe SQL
                    from sqlglot import exp

                    create = exp.Create(
                        this=exp.Table(this=exp.Identifier(this=target_table)),
                        expression=exp.Select().from_(temp_name).select("*"),
                        kind="TABLE",
                    )
                    sql = create.sql()
                elif mode == "append":
                    # Use sqlglot to build safe SQL
                    from sqlglot import exp

                    insert = exp.Insert(
                        this=exp.Table(this=exp.Identifier(this=target_table)),
                        expression=exp.Select().from_(temp_name).select("*"),
                    )
                    sql = insert.sql()
                elif mode == "replace":
                    # Use sqlglot to build safe SQL - CREATE OR REPLACE
                    from sqlglot import exp

                    create = exp.Create(
                        this=exp.Table(this=exp.Identifier(this=target_table)),
                        expression=exp.Select().from_(temp_name).select("*"),
                        kind="TABLE",
                        replace=True,  # CREATE OR REPLACE TABLE
                    )
                    sql = create.sql()
                else:
                    msg = f"Unsupported mode: {mode}"
                    raise ValueError(msg)

                if self.instrumentation_config.log_queries:
                    logger.debug("DuckDB native Arrow ingest: %s", sql)

                from sqlspec.statement.sql import SQL

                result = self.execute(SQL(sql))

                if self.instrumentation_config.log_results_count:
                    logger.debug("Ingested %d rows into %s", table.num_rows, target_table)

                return result.rows_affected or table.num_rows

            finally:
                with contextlib.suppress(Exception):
                    connection.unregister(temp_name)

    def read_parquet_direct(
        self, source_uri: str, columns: "Optional[list[str]]" = None, **options: Any
    ) -> "SQLResult[dict[str, Any]]":
        """Read Parquet file directly using database's native capabilities."""
        if not self._has_native_capability("read", source_uri, "parquet"):
            msg = (
                f"{self.__class__.__name__} does not support direct Parquet reading. Use import_from_storage() instead."
            )
            raise NotImplementedError(msg)
        return self._read_parquet_native(source_uri, columns, **options)
