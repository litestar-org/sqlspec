# ruff: noqa: D104 RUF100 FA100 BLE001 UP037 PLR0913 ANN401 COM812 S608 A002 ARG002 SLF001
import contextlib
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from duckdb import DuckDBPyConnection
from sqlglot import exp

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver.connection import managed_transaction_sync
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncAdapterCacheMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any, ClassVar, Optional, Union

    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

    from sqlspec.typing import ArrowTable

__all__ = ("DuckDBConnection", "DuckDBDriver")

if TYPE_CHECKING:
    DuckDBConnection: TypeAlias = DuckDBPyConnection
else:
    DuckDBConnection = DuckDBPyConnection

logger = get_logger("adapters.duckdb")


class DuckDBDriver(
    SyncDriverAdapterBase,
    SyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    ToSchemaMixin,
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

    connection_type = DuckDBConnection

    dialect: "DialectType" = "duckdb"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NUMERIC)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK
    supports_native_arrow_export: "ClassVar[bool]" = True
    supports_native_arrow_import: "ClassVar[bool]" = True
    supports_native_parquet_export: "ClassVar[bool]" = True
    supports_native_parquet_import: "ClassVar[bool]" = True
    supports_native_csv_export: "ClassVar[bool]" = True
    supports_native_csv_import: "ClassVar[bool]" = True
    supports_native_json_export: "ClassVar[bool]" = True
    supports_native_json_import: "ClassVar[bool]" = True

    def __init__(self, connection: "DuckDBConnection", config: "Optional[SQLConfig]" = None) -> None:  # noqa: FA100
        super().__init__(connection=connection, config=config)

    def _connection(self, connection: "Optional[DuckDBConnection]" = None) -> "DuckDBConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "DuckDBConnection") -> Generator["DuckDBConnection", None, None]:
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()

    def _select_parameter_style(self, statement: "Union[SQL, exp.Expression]") -> "ParameterStyle":
        """Select the best parameter style based on detected styles.

        DuckDB-specific override: prefer QMARK style when available.
        """
        detected_style = super()._select_parameter_style(statement)

        # DuckDB performs better with QMARK style
        if detected_style in self.supported_parameter_styles:
            return detected_style

        # Fallback to QMARK if it's supported
        if ParameterStyle.QMARK in self.supported_parameter_styles:
            return ParameterStyle.QMARK

        return self.default_parameter_style

    def _execute_statement(  # type: ignore[override]
        self,
        statement: SQL,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: "Any",  # noqa: FA100
    ) -> SQLResult:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, **kwargs)

        parameter_style = self._select_parameter_style(statement)
        sql, params = self._get_compiled_sql(statement, parameter_style)
        params = self._process_parameters(params)

        if statement.is_many:
            return self._execute_many(sql, params, connection=connection, **kwargs)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _build_select_result(self, result: "Any", statement: "SQL") -> "SQLResult":
        """Build SQLResult for SELECT operations."""
        fetched_data = result.fetchall()
        column_names = [col[0] for col in result.description or []]

        if fetched_data and isinstance(fetched_data[0], tuple):
            dict_data = [dict(zip(column_names, row)) for row in fetched_data]
        else:
            dict_data = fetched_data

        return SQLResult(
            statement=statement,
            data=dict_data,
            column_names=column_names,
            rows_affected=len(dict_data),
            operation_type="SELECT",
        )

    def _execute(  # noqa: PLR0913
        self,
        sql: str,
        parameters: "Any",
        statement: SQL,
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: "Any",  # noqa: FA100, ANN401
    ) -> SQLResult:
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # TypeCoercionMixin handles parameter processing
            final_params = parameters or []

            if self.returns_rows(statement.expression):
                result = txn_conn.execute(sql, final_params)
                return self._build_select_result(result, statement)

            with self._get_cursor(txn_conn) as cursor:
                cursor.execute(sql, final_params)
                return self._build_modify_result(cursor, statement)

    def _execute_many(
        self,
        sql: str,
        param_list: "Any",
        connection: "Optional[DuckDBConnection]" = None,
        **kwargs: "Any",  # noqa: FA100, ANN401
    ) -> SQLResult:
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # TypeCoercionMixin handles parameter processing
            final_param_list = param_list or []

            # DuckDB throws an error if executemany is called with empty parameter list
            if not final_param_list:
                return SQLResult(  # pyright: ignore
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=0,
                    operation_type="EXECUTE",
                    metadata={"status_message": "OK"},
                )

            with self._get_cursor(txn_conn) as cursor:
                cursor.executemany(sql, final_param_list)
                # DuckDB returns -1 for rowcount on DML operations
                # For executemany, fetchone() only returns the count from the last operation,
                # so use parameter list length as the most accurate estimate
                rows_affected = cursor.rowcount if cursor.rowcount >= 0 else len(final_param_list)
                return SQLResult(  # pyright: ignore
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=rows_affected,
                    operation_type="EXECUTE",
                    metadata={"status_message": "OK"},
                )

    def _validate_statement(self, statement: str) -> None:
        """Validate a single statement from a script."""
        temp_sql = SQL(statement, config=self.config)
        temp_sql._ensure_processed()  # noqa: SLF001
        # Validation errors are logged as warnings by default

    def _execute_script(
        self, script: str, connection: "Optional[DuckDBConnection]" = None, **kwargs: "Any"
    ) -> SQLResult:  # noqa: FA100, ANN401
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # Split script into individual statements for validation
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0

            with self._get_cursor(txn_conn) as cursor:
                for statement in statements:
                    if statement.strip():
                        # Validate each statement unless warnings suppressed
                        if not suppress_warnings:
                            self._validate_statement(statement)

                        cursor.execute(statement)
                        executed_count += 1
                        total_rows += cursor.rowcount or 0

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={
                    "status_message": "Script executed successfully.",
                    "description": "The script was sent to the database.",
                },
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    # ============================================================================
    # DuckDB Native Arrow Support
    # ============================================================================

    def _fetch_arrow_table(self, sql: SQL, connection: "Optional[Any]" = None, **kwargs: "Any") -> "ArrowResult":  # noqa: FA100, ANN401
        """Enhanced DuckDB native Arrow table fetching with streaming support."""
        conn = self._connection(connection)
        # Use centralized SQL compilation from storage mixin
        sql_string, parameters = self._get_compiled_sql_for_arrow(sql)
        result = conn.execute(sql_string, parameters or [])

        batch_size = kwargs.get("batch_size")
        if batch_size:
            arrow_reader = result.fetch_record_batch(batch_size)
            import pyarrow as pa

            batches = list(arrow_reader)
            arrow_table = pa.Table.from_batches(batches) if batches else pa.table({})
            logger.debug("Fetched Arrow table (streaming) with %d rows", arrow_table.num_rows)
        else:
            arrow_table = result.arrow()
            logger.debug("Fetched Arrow table (zero-copy) with %d rows", arrow_table.num_rows)

        return ArrowResult(statement=sql, data=arrow_table)

    # ============================================================================
    # DuckDB Native Storage Operations (Override base implementations)
    # ============================================================================
    # Note: _has_native_capability is now inherited from StorageMixinBase

    def _export_native(self, query: str, destination_uri: "Union[str, Path]", format: str, **options: "Any") -> int:  # noqa: FA100, A002, ANN401
        conn = self._connection(None)
        copy_options: list[str] = []

        if format.lower() == "parquet":
            copy_options.append("FORMAT PARQUET")
            if "compression" in options:
                copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
            if "row_group_size" in options:
                copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")
            if "partition_by" in options:
                partition_cols = (
                    [options["partition_by"]] if isinstance(options["partition_by"], str) else options["partition_by"]
                )
                copy_options.append(f"PARTITION_BY ({', '.join(partition_cols)})")
        elif format.lower() == "csv":
            copy_options.extend(("FORMAT CSV", "HEADER"))
            if "compression" in options:
                copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
            if "delimiter" in options:
                copy_options.append(f"DELIMITER '{options['delimiter']}'")
            if "quote" in options:
                copy_options.append(f"QUOTE '{options['quote']}'")
        elif format.lower() == "json":
            copy_options.append("FORMAT JSON")
            if "compression" in options:
                copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
        else:
            msg = f"Unsupported format for DuckDB native export: {format}"
            raise ValueError(msg)

        options_str = f"({', '.join(copy_options)})" if copy_options else ""
        copy_sql = f"COPY ({query}) TO '{destination_uri!s}' {options_str}"  # noqa: S608
        result_rel = conn.execute(copy_sql)
        result = result_rel.fetchone() if result_rel else None
        return result[0] if result else 0

    def _import_native(  # noqa: PLR0913
        self,
        source_uri: "Union[str, Path]",
        table_name: str,
        format: str,
        mode: str,
        **options: "Any",  # noqa: FA100, A002, ANN401
    ) -> int:
        conn = self._connection(None)
        # Build the appropriate read function expression
        if format == "parquet":
            read_func_expr = exp.func("read_parquet", exp.Literal.string(str(source_uri)))
        elif format == "csv":
            read_func_expr = exp.func("read_csv_auto", exp.Literal.string(str(source_uri)))
        elif format == "json":
            read_func_expr = exp.func("read_json_auto", exp.Literal.string(str(source_uri)))
        else:
            msg = f"Unsupported format for DuckDB native import: {format}"
            raise ValueError(msg)

        # Build SQL expression using SQLglot
        if mode == "create":
            sql_expr = exp.Create(
                this=exp.to_table(table_name), expression=exp.Select().from_(read_func_expr).select("*"), kind="TABLE"
            )
        elif mode == "replace":
            sql_expr = exp.Create(
                this=exp.to_table(table_name),
                expression=exp.Select().from_(read_func_expr).select("*"),
                kind="TABLE",
                replace=True,
            )
        elif mode == "append":
            sql_expr = exp.Insert(
                this=exp.to_table(table_name), expression=exp.Select().from_(read_func_expr).select("*")
            )
        else:
            msg = f"Unsupported import mode: {mode}"
            raise ValueError(msg)

        # Execute the SQL
        sql_str = sql_expr.sql(dialect=self.dialect)
        result_rel = conn.execute(sql_str)
        result = result_rel.fetchone() if result_rel else None
        if result:
            return int(result[0])

        # Get row count if result doesn't contain it
        count_expr = exp.Select(exp.func("COUNT", exp.Star())).from_(table_name)
        count_sql = count_expr.sql(dialect=self.dialect)
        count_result_rel = conn.execute(count_sql)
        count_result = count_result_rel.fetchone() if count_result_rel else None
        return int(count_result[0]) if count_result else 0

    def _read_parquet_native(  # noqa: PLR0913
        # NOTE: This method uses direct SQL string construction for DuckDB's read_parquet function
        # rather than the builder API because:
        # 1. read_parquet() is a DuckDB-specific table function, not standard SQL
        # 2. The builder API doesn't have native support for table functions
        # 3. Direct construction is more straightforward for this specific use case
        # Future enhancement: Add table function support to the builder API
        self,
        source_uri: "Union[str, Path]",  # noqa: FA100
        columns: "Optional[list[str]]" = None,  # noqa: FA100, UP037
        **options: "Any",  # noqa: ANN401
    ) -> "SQLResult":
        conn = self._connection(None)
        if isinstance(source_uri, list):
            file_list = "[" + ", ".join(f"'{f}'" for f in source_uri) + "]"
            read_func = f"read_parquet({file_list})"
        elif "*" in str(source_uri) or "?" in str(source_uri):
            read_func = f"read_parquet('{source_uri!s}')"
        else:
            read_func = f"read_parquet('{source_uri!s}')"

        column_list = ", ".join(columns) if columns else "*"
        query = f"SELECT {column_list} FROM {read_func}"  # noqa: S608

        filters = options.get("filters")
        if filters:
            where_clauses = []
            for col, op, val in filters:
                where_clauses.append(f"'{col}' {op} '{val}'" if isinstance(val, str) else f"'{col}' {op} {val}")
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)

        arrow_table = conn.execute(query).arrow()
        arrow_dict = arrow_table.to_pydict()
        column_names = arrow_table.column_names
        num_rows = arrow_table.num_rows

        rows = [{col: arrow_dict[col][i] for col in column_names} for i in range(num_rows)]

        return SQLResult(
            statement=SQL(query, _dialect=self.dialect),
            data=rows,
            column_names=column_names,
            rows_affected=num_rows,
            operation_type="SELECT",
        )

    def _write_parquet_native(
        self,
        data: "Union[str, ArrowTable]",
        destination_uri: "Union[str, Path]",
        **options: "Any",  # noqa: FA100, ANN401
    ) -> None:
        conn = self._connection(None)
        copy_options: list[str] = ["FORMAT PARQUET"]
        if "compression" in options:
            copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
        if "row_group_size" in options:
            copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

        options_str = f"({', '.join(copy_options)})"

        if isinstance(data, str):
            copy_sql = f"COPY ({data}) TO '{destination_uri!s}' {options_str}"  # noqa: S608
            conn.execute(copy_sql)
        else:
            temp_name = f"_arrow_data_{uuid.uuid4().hex[:8]}"
            conn.register(temp_name, data)
            try:
                copy_sql = f"COPY {temp_name} TO '{destination_uri!s}' {options_str}"  # noqa: S608
                conn.execute(copy_sql)
            finally:
                with contextlib.suppress(Exception):
                    conn.unregister(temp_name)

    def _ingest_arrow_table(self, table: "ArrowTable", table_name: str, mode: str = "create", **options: "Any") -> int:  # noqa: ANN401
        """DuckDB-optimized Arrow table ingestion using native registration."""
        self._ensure_pyarrow_installed()
        # Validate mode parameter using centralized method
        mode = self._standardize_mode_parameter(mode)
        conn = self._connection(None)
        temp_name = f"_arrow_temp_{uuid.uuid4().hex[:8]}"

        try:
            conn.register(temp_name, table)

            if mode == "create":
                sql_expr = exp.Create(
                    this=exp.to_table(table_name), expression=exp.Select().from_(temp_name).select("*"), kind="TABLE"
                )
            elif mode == "append":
                sql_expr = exp.Insert(  # type: ignore[assignment]
                    this=exp.to_table(table_name), expression=exp.Select().from_(temp_name).select("*")
                )
            elif mode == "replace":
                sql_expr = exp.Create(
                    this=exp.to_table(table_name),
                    expression=exp.Select().from_(temp_name).select("*"),
                    kind="TABLE",
                    replace=True,
                )
            else:
                msg = f"Unsupported mode: {mode}"
                raise ValueError(msg)

            result = self.execute(SQL(sql_expr.sql(dialect=self.dialect), _dialect=self.dialect))
            return result.rows_affected or table.num_rows
        finally:
            with contextlib.suppress(Exception):  # noqa: BLE001
                conn.unregister(temp_name)
