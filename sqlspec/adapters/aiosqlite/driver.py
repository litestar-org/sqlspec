import csv
import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

import aiosqlite

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.connection import managed_transaction_async
from sqlspec.driver.mixins import (
    AsyncAdapterCacheMixin,
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.driver.parameters import convert_parameter_sequence
from sqlspec.statement.parameters import ParameterStyle, ParameterValidator
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, RowT
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    AsyncDriverAdapterProtocol[AiosqliteConnection, RowT],
    AsyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Aiosqlite SQLite Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)
    default_parameter_style: ParameterStyle = ParameterStyle.QMARK

    def __init__(
        self,
        connection: AiosqliteConnection,
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    # AIOSQLite-specific type coercion overrides (same as SQLite)
    def _coerce_boolean(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores booleans as integers (0/1)."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores decimals as strings to preserve precision."""
        if isinstance(value, str):
            return value  # Already a string
        from decimal import Decimal

        if isinstance(value, Decimal):
            return str(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores JSON as strings (requires JSON1 extension)."""
        if isinstance(value, (dict, list)):
            return to_json(value)
        return value

    def _coerce_array(self, value: Any) -> Any:
        """AIOSQLite/SQLite doesn't have native arrays - store as JSON strings."""
        if isinstance(value, (list, tuple)):
            return to_json(list(value))
        return value

    @asynccontextmanager
    async def _get_cursor(
        self, connection: Optional[AiosqliteConnection] = None
    ) -> AsyncGenerator[aiosqlite.Cursor, None]:
        conn_to_use = connection or self.connection
        conn_to_use.row_factory = aiosqlite.Row
        cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _execute_statement(
        self, statement: SQL, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

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
        elif detected_styles:
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            sql, params = self._get_compiled_sql(statement, target_style)

            params = self._process_parameters(params)

            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = self._get_compiled_sql(statement, target_style)

        params = self._process_parameters(params)

        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        conn = self._connection(connection)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            converted_params = convert_parameter_sequence(parameters)

            # Extract the actual parameters from the converted list
            actual_params = converted_params[0] if converted_params and len(converted_params) == 1 else converted_params

            # AIOSQLite expects tuple or dict - handle parameter conversion
            if ":param_" in sql or (isinstance(actual_params, dict)):
                # SQL has named placeholders, ensure params are dict
                converted_params = self._convert_parameters_to_driver_format(
                    sql, actual_params, target_style=ParameterStyle.NAMED_COLON
                )
            else:
                # SQL has positional placeholders, ensure params are list/tuple
                converted_params = self._convert_parameters_to_driver_format(
                    sql, actual_params, target_style=ParameterStyle.QMARK
                )

            async with self._get_cursor(txn_conn) as cursor:
                # Aiosqlite handles both dict and tuple parameters
                await cursor.execute(sql, converted_params or ())
                if self.returns_rows(statement.expression):
                    fetched_data = await cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description or []]
                    data_list: list[Any] = list(fetched_data) if fetched_data else []
                    return SQLResult(
                        statement=statement,
                        data=data_list,
                        column_names=column_names,
                        rows_affected=len(data_list),
                        operation_type="SELECT",
                    )

                return SQLResult(
                    statement=statement,
                    data=[],
                    rows_affected=cursor.rowcount,
                    operation_type=self._determine_operation_type(statement),
                    metadata={"status_message": "OK"},
                )

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)

            params_list: list[tuple[Any, ...]] = []
            if converted_param_list and isinstance(converted_param_list, Sequence):
                for param_set in converted_param_list:
                    if isinstance(param_set, (list, tuple)):
                        params_list.append(tuple(param_set))
                    elif param_set is None:
                        params_list.append(())

            async with self._get_cursor(txn_conn) as cursor:
                await cursor.executemany(sql, params_list)
                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=cursor.rowcount,
                    operation_type="EXECUTE",
                    metadata={"status_message": "OK"},
                )

    async def _execute_script(
        self, script: str, connection: Optional[AiosqliteConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Split script into individual statements for validation
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0

            # Execute each statement individually for better control and validation
            async with self._get_cursor(txn_conn) as cursor:
                for statement in statements:
                    if statement.strip():
                        # Validate each statement unless warnings suppressed
                        if not suppress_warnings:
                            # Run validation through pipeline
                            temp_sql = SQL(statement, config=self.config)
                            temp_sql._ensure_processed()
                            # Validation errors are logged as warnings by default

                        await cursor.execute(statement)
                        executed_count += 1
                        total_rows += cursor.rowcount or 0

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": "SCRIPT EXECUTED"},
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    async def _bulk_load_file(self, file_path: Path, table_name: str, format: str, mode: str, **options: Any) -> int:
        """Database-specific bulk load implementation using storage backend."""
        if format != "csv":
            msg = f"aiosqlite driver only supports CSV for bulk loading, not {format}."
            raise NotImplementedError(msg)

        conn = await self._create_connection()  # type: ignore[attr-defined]
        try:
            async with self._get_cursor(conn) as cursor:
                if mode == "replace":
                    await cursor.execute(f"DELETE FROM {table_name}")

                # Use async storage backend to read the file
                file_path_str = str(file_path)
                backend = self._get_storage_backend(file_path_str)
                content = await backend.read_text_async(file_path_str, encoding="utf-8")
                # Parse CSV content
                import io

                csv_file = io.StringIO(content)
                reader = csv.reader(csv_file, **options)
                header = next(reader)  # Skip header
                placeholders = ", ".join("?" for _ in header)
                sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                data_iter = list(reader)
                await cursor.executemany(sql, data_iter)
                rowcount = cursor.rowcount
                await conn.commit()
                return rowcount
        finally:
            await conn.close()

    def _connection(self, connection: Optional[AiosqliteConnection] = None) -> AiosqliteConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
