import contextlib
import csv
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union, cast

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlglot import exp
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

from sqlspec.driver import SyncDriverAdapterBase
from sqlspec.driver.connection import managed_transaction_sync
from sqlspec.driver.mixins import (
    SQLTranslatorMixin,
    SyncAdapterCacheMixin,
    SyncPipelinedExecutionMixin,
    SyncQueryMixin,
    SyncStorageMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

__all__ = ("SqliteConnection", "SqliteDriver")

logger = get_logger("adapters.sqlite")

if TYPE_CHECKING:
    SqliteConnection: TypeAlias = sqlite3.Connection
else:
    # Direct assignment for mypyc runtime
    SqliteConnection = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterBase,
    SyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    SyncStorageMixin,
    SyncPipelinedExecutionMixin,
    SyncQueryMixin,
    ToSchemaMixin,
):
    """SQLite Sync Driver Adapter with Arrow/Parquet export support.

    Refactored to align with the new enhanced driver architecture and
    instrumentation standards following the psycopg pattern.
    """

    connection_type = SqliteConnection
    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK

    def __init__(self, connection: "SqliteConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    def _connection(self, connection: "Optional[SqliteConnection]" = None) -> "SqliteConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection

    # SQLite-specific type coercion overrides
    def _coerce_boolean(self, value: "Any") -> "Any":
        """SQLite stores booleans as integers (0/1)."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: "Any") -> "Any":
        """SQLite stores decimals as strings to preserve precision."""
        if isinstance(value, str):
            return value
        from decimal import Decimal

        if isinstance(value, Decimal):
            return str(value)
        return value

    def _coerce_json(self, value: "Any") -> "Any":
        """SQLite stores JSON as strings (requires JSON1 extension)."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: "Any") -> "Any":
        """SQLite doesn't have native arrays - store as JSON strings."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

    @staticmethod
    @contextmanager
    def _get_cursor(connection: "SqliteConnection") -> "Iterator[sqlite3.Cursor]":
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            with contextlib.suppress(Exception):
                cursor.close()

    def _execute_statement(
        self, statement: "SQL", connection: "Optional[SqliteConnection]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        """Execute a SQL statement."""
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, statement=statement, **kwargs)

        # Determine target parameter style
        target_style = self._select_parameter_style(statement)

        if statement.is_many:
            sql, params = self._get_compiled_sql(statement, target_style)
            return self._execute_many(sql, params, connection=connection, statement=statement, **kwargs)

        sql, params = self._get_compiled_sql(statement, target_style)
        params = self._process_parameters(params)

        # SQLite expects tuples for positional parameters
        if target_style == ParameterStyle.QMARK and isinstance(params, list):
            params = tuple(params)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _select_parameter_style(self, statement: "Union[SQL, exp.Expression]") -> "ParameterStyle":
        """Select the best parameter style for SQLite based on detected styles.

        Extends base implementation to handle parameterized literals optimization.
        """
        detected_style = super()._select_parameter_style(statement)

        # Handle parameterized literals optimization specific to SQLite
        try:
            processing_context = statement._processing_context
            if (
                processing_context
                and processing_context.metadata.get("literals_parameterized")
                and detected_style == ParameterStyle.NAMED_COLON
            ):
                return self.default_parameter_style  # QMARK for better performance
        except AttributeError:
            pass

        return detected_style

    def _execute(
        self,
        sql: str,
        parameters: "Any",
        statement: "SQL",
        connection: "Optional[SqliteConnection]" = None,
        **kwargs: "Any",
    ) -> "SQLResult":
        """Execute a single statement with parameters."""
        conn = self._connection(connection)
        with managed_transaction_sync(conn, auto_commit=True) as txn_conn, self._get_cursor(txn_conn) as cursor:
            cursor.execute(sql, parameters)

            if self.returns_rows(statement.expression):
                return self._build_select_result(cursor, statement)

            return self._build_modify_result(cursor, statement)

    def _build_select_result(self, cursor: "sqlite3.Cursor", statement: "SQL") -> "SQLResult":
        """Build result for SELECT operations."""
        fetched_data: "list[sqlite3.Row]" = cursor.fetchall()  # noqa: UP037
        return SQLResult(
            statement=statement,
            data=cast("list[dict[str, Any]]", fetched_data),
            column_names=[col[0] for col in cursor.description or []],
            rows_affected=len(fetched_data),
            operation_type="SELECT",
        )

    def _execute_many(
        self,
        sql: str,
        param_list: "Any",
        connection: "Optional[SqliteConnection]" = None,
        statement: "Optional[SQL]" = None,
        **kwargs: "Any",
    ) -> "SQLResult":
        """Execute a statement many times with a list of parameter tuples."""
        conn = self._connection(connection)
        with managed_transaction_sync(conn, auto_commit=True) as txn_conn, self._get_cursor(txn_conn) as cursor:
            cursor.executemany(sql, param_list)

            if statement is None:
                statement = SQL(sql, _dialect=self.dialect)

            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=cursor.rowcount,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    def _execute_script(
        self,
        script: str,
        connection: "Optional[SqliteConnection]" = None,
        statement: "Optional[SQL]" = None,
        **kwargs: "Any",
    ) -> "SQLResult":
        """Execute script using splitter for per-statement validation."""
        from sqlspec.statement.splitter import split_sql_script

        conn = self._connection(connection)
        statements = split_sql_script(script, dialect="sqlite")
        suppress_warnings = kwargs.get("_suppress_warnings", False)
        continue_on_error = kwargs.get("continue_on_error", False)

        total_rows = 0
        successful = 0

        with self._get_cursor(conn) as cursor:
            for stmt in statements:
                try:
                    if not suppress_warnings and statement:
                        self._validate_statement(stmt, statement._config)

                    cursor.execute(stmt)
                    successful += 1
                    total_rows += cursor.rowcount or 0
                except Exception as e:  # noqa: PERF203
                    if not continue_on_error:
                        raise
                    logger.warning("Script statement failed: %s", e)

        conn.commit()

        if statement is None:
            statement = SQL(script, _dialect=self.dialect).as_script()

        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=total_rows,
            operation_type="SCRIPT",
            total_statements=len(statements),
            successful_statements=successful,
            metadata={"status_message": "SCRIPT EXECUTED"},
        )

    def _validate_statement(self, stmt: str, config: "SQLConfig") -> None:
        """Validate a statement through the pipeline."""
        temp_sql = SQL(stmt, config=config)
        temp_sql._ensure_processed()

    def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "create", **options: "Any") -> int:
        """SQLite-specific Arrow table ingestion using CSV conversion.

        Since SQLite only supports CSV bulk loading, we convert the Arrow table
        to CSV format first using the storage backend for efficient operations.
        """
        import io
        import tempfile

        import pyarrow.csv as pa_csv

        # Validate mode parameter using centralized method
        mode = self._standardize_mode_parameter(mode)

        csv_buffer = io.BytesIO()
        pa_csv.write_csv(table, csv_buffer)
        csv_content = csv_buffer.getvalue()

        temp_filename = f"sqlspec_temp_{table_name}_{id(self)}.csv"
        temp_path = Path(tempfile.gettempdir()) / temp_filename

        # Use storage backend to write the CSV content
        backend = self._get_storage_backend(temp_path)
        backend.write_bytes(str(temp_path), csv_content)

        try:
            # Use SQLite's CSV bulk load
            return self._bulk_load_file(temp_path, table_name, "csv", mode, **options)
        finally:
            # Clean up using storage backend
            with contextlib.suppress(Exception):
                # Best effort cleanup
                backend.delete(str(temp_path))

    def _bulk_load_file(self, file_path: "Path", table_name: str, format: str, mode: str, **options: "Any") -> int:
        """Database-specific bulk load implementation using storage backend."""
        if format != "csv":
            msg = f"SQLite driver only supports CSV for bulk loading, not {format}."
            raise NotImplementedError(msg)

        conn = self._connection(None)
        with self._get_cursor(conn) as cursor:
            if mode == "replace":
                cursor.execute(f"DELETE FROM {table_name}")

            # Use storage backend to read the file
            backend = self._get_storage_backend(file_path)
            content = backend.read_text(str(file_path), encoding="utf-8")

            # Parse CSV content
            import io

            csv_file = io.StringIO(content)
            reader = csv.reader(csv_file, **options)
            header = next(reader)  # Skip header
            placeholders = ", ".join("?" for _ in header)
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # executemany is efficient for bulk inserts
            data_iter = list(reader)  # Read all data into memory
            cursor.executemany(sql, data_iter)
            return cursor.rowcount
