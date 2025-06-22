import contextlib
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from duckdb import DuckDBPyConnection
from sqlglot import exp

from sqlspec.driver import SyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncPipelinedExecutionMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import ArrowTable, DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.typing import ArrowTable

__all__ = ("DuckDBConnection", "DuckDBDriver")

DuckDBConnection = DuckDBPyConnection

logger = get_logger("adapters.duckdb")


class DuckDBDriver(
    SyncDriverAdapterProtocol["DuckDBConnection", RowT],
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

    dialect: "DialectType" = "duckdb"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NUMERIC)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK
    supports_native_arrow_export: ClassVar[bool] = True
    supports_native_arrow_import: ClassVar[bool] = True
    supports_native_parquet_export: ClassVar[bool] = True
    supports_native_parquet_import: ClassVar[bool] = True
    __slots__ = ()

    def __init__(
        self,
        connection: "DuckDBConnection",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

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
            params = self._process_parameters(params)
            return self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=self.default_parameter_style)
        params = self._process_parameters(params)
        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "Union[SelectResultDict, DMLResultDict]":
        conn = self._connection(connection)

        if self.returns_rows(statement.expression):
            result = conn.execute(sql, parameters or [])
            fetched_data = result.fetchall()
            column_names = [col[0] for col in result.description or []]
            return {"data": fetched_data, "column_names": column_names, "rows_affected": len(fetched_data)}

        with self._get_cursor(conn) as cursor:
            cursor.execute(sql, parameters or [])
            # DuckDB returns -1 for rowcount on DML operations
            # However, fetchone() returns the actual affected row count as (count,)
            rows_affected = cursor.rowcount
            if rows_affected < 0:
                try:
                    # Get actual affected row count from fetchone()
                    fetch_result = cursor.fetchone()
                    if fetch_result and isinstance(fetch_result, (tuple, list)) and len(fetch_result) > 0:
                        rows_affected = fetch_result[0]
                    else:
                        rows_affected = 0
                except Exception:
                    # Fallback to 1 if fetchone fails
                    rows_affected = 1
            return {"rows_affected": rows_affected}

    def _execute_many(
        self, sql: str, param_list: Any, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "DMLResultDict":
        conn = self._connection(connection)
        param_list = param_list or []

        # DuckDB throws an error if executemany is called with empty parameter list
        if not param_list:
            return {"rows_affected": 0}
        with self._get_cursor(conn) as cursor:
            cursor.executemany(sql, param_list)
            # DuckDB returns -1 for rowcount on DML operations
            # For executemany, fetchone() only returns the count from the last operation,
            # so use parameter list length as the most accurate estimate
            rows_affected = cursor.rowcount if cursor.rowcount >= 0 else len(param_list)
            return {"rows_affected": rows_affected}

    def _execute_script(
        self, script: str, connection: Optional["DuckDBConnection"] = None, **kwargs: Any
    ) -> "ScriptResultDict":
        conn = self._connection(connection)
        with self._get_cursor(conn) as cursor:
            cursor.execute(script)

        return {
            "statements_executed": -1,
            "status_message": "Script executed successfully.",
            "description": "The script was sent to the database.",
        }

    def _wrap_select_result(
        self, statement: SQL, result: "SelectResultDict", schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        fetched_tuples = result["data"]
        column_names = result["column_names"]
        rows_affected = result["rows_affected"]

        rows_as_dicts: list[dict[str, Any]] = [dict(zip(column_names, row)) for row in fetched_tuples]

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
        operation_type = "UNKNOWN"
        if statement.expression:
            operation_type = str(statement.expression.key).upper()

        if "statements_executed" in result:
            script_result = cast("ScriptResultDict", result)
            return SQLResult[RowT](
                statement=statement,
                data=[],
                rows_affected=0,
                operation_type=operation_type or "SCRIPT",
                metadata={"status_message": script_result.get("status_message", "")},
            )

        dml_result = cast("DMLResultDict", result)
        rows_affected = dml_result.get("rows_affected", -1)
        status_message = dml_result.get("status_message", "")
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
        """Enhanced DuckDB native Arrow table fetching with streaming support."""
        conn = self._connection(connection)
        sql_string, parameters = sql.compile(placeholder_style=self.default_parameter_style)
        parameters = self._process_parameters(parameters)
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

    def _has_native_capability(self, operation: str, uri: str = "", format: str = "") -> bool:
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
        copy_sql = f"COPY ({query}) TO '{destination_uri}' {options_str}"
        result_rel = conn.execute(copy_sql)
        result = result_rel.fetchone() if result_rel else None
        return result[0] if result else 0

    def _import_native(self, source_uri: str, table_name: str, format: str, mode: str, **options: Any) -> int:
        conn = self._connection(None)
        if format == "parquet":
            read_func = f"read_parquet('{source_uri}')"
        elif format == "csv":
            read_func = f"read_csv_auto('{source_uri}')"
        elif format == "json":
            read_func = f"read_json_auto('{source_uri}')"
        else:
            msg = f"Unsupported format for DuckDB native import: {format}"
            raise ValueError(msg)

        if mode == "create":
            sql = f"CREATE TABLE {table_name} AS SELECT * FROM {read_func}"
        elif mode == "replace":
            sql = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {read_func}"
        elif mode == "append":
            sql = f"INSERT INTO {table_name} SELECT * FROM {read_func}"
        else:
            msg = f"Unsupported import mode: {mode}"
            raise ValueError(msg)

        result_rel = conn.execute(sql)
        result = result_rel.fetchone() if result_rel else None
        if result:
            return int(result[0])

        count_result_rel = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_result = count_result_rel.fetchone() if count_result_rel else None
        return int(count_result[0]) if count_result else 0

    def _read_parquet_native(
        self, source_uri: str, columns: Optional[list[str]] = None, **options: Any
    ) -> "SQLResult[dict[str, Any]]":
        conn = self._connection(None)
        if isinstance(source_uri, list):
            file_list = "[" + ", ".join(f"'{f}'" for f in source_uri) + "]"
            read_func = f"read_parquet({file_list})"
        elif "*" in source_uri or "?" in source_uri:
            read_func = f"read_parquet('{source_uri}')"
        else:
            read_func = f"read_parquet('{source_uri}')"

        column_list = ", ".join(columns) if columns else "*"
        query = f"SELECT {column_list} FROM {read_func}"

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

        return SQLResult[dict[str, Any]](
            statement=SQL(query), data=rows, column_names=column_names, rows_affected=num_rows, operation_type="SELECT"
        )

    def _write_parquet_native(self, data: Union[str, "ArrowTable"], destination_uri: str, **options: Any) -> None:
        conn = self._connection(None)
        copy_options: list[str] = ["FORMAT PARQUET"]
        if "compression" in options:
            copy_options.append(f"COMPRESSION '{options['compression'].upper()}'")
        if "row_group_size" in options:
            copy_options.append(f"ROW_GROUP_SIZE {options['row_group_size']}")

        options_str = f"({', '.join(copy_options)})"

        if isinstance(data, str):
            copy_sql = f"COPY ({data}) TO '{destination_uri}' {options_str}"
            conn.execute(copy_sql)
        else:
            temp_name = f"_arrow_data_{uuid.uuid4().hex[:8]}"
            conn.register(temp_name, data)
            try:
                copy_sql = f"COPY {temp_name} TO '{destination_uri}' {options_str}"
                conn.execute(copy_sql)
            finally:
                with contextlib.suppress(Exception):
                    conn.unregister(temp_name)

    def _ingest_arrow_table(self, table: "ArrowTable", table_name: str, mode: str = "create", **options: Any) -> int:
        """DuckDB-optimized Arrow table ingestion using native registration."""
        self._ensure_pyarrow_installed()
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

            result = self.execute(SQL(sql_expr.sql(dialect=self.dialect)))
            return result.rows_affected or table.num_rows
        finally:
            with contextlib.suppress(Exception):
                conn.unregister(temp_name)
