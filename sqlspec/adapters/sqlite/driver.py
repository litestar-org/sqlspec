import contextlib
import csv
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, cast

from typing_extensions import TypeAlias

from sqlspec.driver import SyncDriverAdapterProtocol
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
from sqlspec.driver.parameters import convert_parameter_sequence
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("SqliteConnection", "SqliteDriver")

logger = get_logger("adapters.sqlite")

SqliteConnection: TypeAlias = sqlite3.Connection


class SqliteDriver(
    SyncDriverAdapterProtocol[SqliteConnection, RowT],
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

    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK

    def __init__(
        self,
        connection: "SqliteConnection",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = dict[str, Any],
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    # SQLite-specific type coercion overrides
    def _coerce_boolean(self, value: Any) -> Any:
        """SQLite stores booleans as integers (0/1)."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """SQLite stores decimals as strings to preserve precision."""
        if isinstance(value, str):
            return value  # Already a string
        from decimal import Decimal

        if isinstance(value, Decimal):
            return str(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """SQLite stores JSON as strings (requires JSON1 extension)."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """SQLite doesn't have native arrays - store as JSON strings."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

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
        self, statement: SQL, connection: Optional[SqliteConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return self._execute_script(sql, connection=connection, statement=statement, **kwargs)

        detected_styles = set()
        sql_str = statement.to_sql(placeholder_style=None)  # Get raw SQL
        validator = self.config.parameter_validator if self.config else ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)
        if param_infos:
            detected_styles = {p.style for p in param_infos}

        target_style = self.default_parameter_style

        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            target_style = self.default_parameter_style
        elif len(detected_styles) > 1:
            # Mixed styles detected - use default style for consistency
            target_style = self.default_parameter_style
        elif detected_styles:
            # Single style detected - use it if supported
            detected_style = next(iter(detected_styles))
            if detected_style.value in self.supported_parameter_styles:
                target_style = detected_style
            else:
                target_style = self.default_parameter_style

        if statement.is_many:
            sql, params = self._get_compiled_sql(statement, target_style)
            return self._execute_many(sql, params, connection=connection, statement=statement, **kwargs)

        sql, params = self._get_compiled_sql(statement, target_style)

        params = self._process_parameters(params)

        # SQLite expects tuples for positional parameters
        if isinstance(params, list):
            params = tuple(params)

        return self._execute(sql, params, statement, connection=connection, **kwargs)

    def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[SqliteConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        """Execute a single statement with parameters."""
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)
        with managed_transaction_sync(conn, auto_commit=True) as txn_conn, self._get_cursor(txn_conn) as cursor:
            # Convert parameters using consolidated utility
            converted_params_list = convert_parameter_sequence(parameters)
            params_for_execute: Any
            if converted_params_list and len(converted_params_list) == 1:
                # Single parameter should be tuple for SQLite
                if not isinstance(converted_params_list[0], (tuple, list, dict)):
                    params_for_execute = (converted_params_list[0],)
                else:
                    params_for_execute = converted_params_list[0]
            else:
                # Multiple parameters
                params_for_execute = tuple(converted_params_list) if converted_params_list else ()

            cursor.execute(sql, params_for_execute)
            if self.returns_rows(statement.expression):
                fetched_data: list[sqlite3.Row] = cursor.fetchall()
                return SQLResult(
                    statement=statement,
                    data=cast("list[RowT]", fetched_data),
                    column_names=[col[0] for col in cursor.description or []],
                    rows_affected=len(fetched_data),
                    operation_type="SELECT",
                )
            operation_type = self._determine_operation_type(statement)

            return SQLResult(
                statement=statement,
                data=[],
                rows_affected=cursor.rowcount,
                operation_type=operation_type,
                metadata={"status_message": "OK"},
            )

    def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[SqliteConnection] = None,
        statement: Optional[SQL] = None,
        **kwargs: Any,
    ) -> SQLResult[RowT]:
        """Execute a statement many times with a list of parameter tuples."""
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)
        with managed_transaction_sync(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)
            formatted_params: list[tuple[Any, ...]] = []
            if converted_param_list:
                for param_set in converted_param_list:
                    if isinstance(param_set, (list, tuple)):
                        formatted_params.append(tuple(param_set))
                    elif param_set is None:
                        formatted_params.append(())
                    else:
                        formatted_params.append((param_set,))

            with self._get_cursor(txn_conn) as cursor:
                cursor.executemany(sql, formatted_params)

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
        self, script: str, connection: Optional[SqliteConnection] = None, statement: Optional[SQL] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        """Execute script using splitter for per-statement validation."""
        from sqlspec.statement.splitter import split_sql_script

        conn = connection if connection is not None else self._connection(None)
        statements = split_sql_script(script, dialect="sqlite")

        total_rows = 0
        successful = 0
        suppress_warnings = kwargs.get("_suppress_warnings", False)

        with self._get_cursor(conn) as cursor:
            for stmt in statements:
                try:
                    # Validate each statement unless warnings suppressed
                    if not suppress_warnings and statement:
                        # Run validation through pipeline
                        temp_sql = SQL(stmt, config=statement._config)
                        temp_sql._ensure_processed()
                        # Validation errors are logged as warnings by default

                    cursor.execute(stmt)
                    successful += 1
                    total_rows += cursor.rowcount or 0
                except Exception as e:  # noqa: PERF203
                    if not kwargs.get("continue_on_error", False):
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

    def _ingest_arrow_table(self, table: Any, table_name: str, mode: str = "create", **options: Any) -> int:
        """SQLite-specific Arrow table ingestion using CSV conversion.

        Since SQLite only supports CSV bulk loading, we convert the Arrow table
        to CSV format first using the storage backend for efficient operations.
        """
        import io
        import tempfile

        import pyarrow.csv as pa_csv

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

    def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
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
