import logging
import re
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any, ClassVar, Optional

import aiosqlite

# TypeAlias removed for mypyc compatibility
from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.driver.connection import managed_transaction_async
from sqlspec.driver.mixins import (
    AsyncAdapterCacheMixin,
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.utils.serializers import to_json

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType
    from typing_extensions import TypeAlias

__all__ = ("AiosqliteConnection", "AiosqliteDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AiosqliteConnection: TypeAlias = aiosqlite.Connection
else:
    # Direct assignment for mypyc runtime
    AiosqliteConnection = aiosqlite.Connection


class AiosqliteDriver(
    AsyncDriverAdapterBase,
    AsyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Aiosqlite SQLite Driver Adapter. Modern protocol implementation."""

    # Type specification for mypyc
    connection_type: "ClassVar[type[AiosqliteConnection]]" = AiosqliteConnection

    dialect: "DialectType" = "sqlite"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.QMARK, ParameterStyle.NAMED_COLON)
    default_parameter_style: "ParameterStyle" = ParameterStyle.QMARK

    def __init__(self, connection: "AiosqliteConnection", config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    # AIOSQLite-specific type coercion overrides (same as SQLite)
    def _coerce_boolean(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores booleans as integers (0/1)."""
        if isinstance(value, bool):
            return 1 if value else 0
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """AIOSQLite/SQLite stores decimals as strings to preserve precision."""
        if isinstance(value, str):
            return value
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
        self, connection: "Optional[AiosqliteConnection]" = None
    ) -> "AsyncGenerator[aiosqlite.Cursor, None]":
        conn_to_use = connection or self.connection
        conn_to_use.row_factory = aiosqlite.Row
        cursor = await conn_to_use.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _execute_statement(  # type: ignore[override]
        self, statement: "SQL", connection: "Optional[AiosqliteConnection]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        target_style = self._select_parameter_style(statement)

        if statement.is_many:
            sql, params = self._get_compiled_sql(statement, target_style)

            params = self._process_parameters(params)

            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = self._get_compiled_sql(statement, target_style)

        params = self._process_parameters(params)

        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self,
        sql: str,
        parameters: "Any",
        statement: "SQL",
        connection: "Optional[AiosqliteConnection]" = None,
        **kwargs: "Any",
    ) -> "SQLResult":
        conn = self._connection(connection)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            actual_params = parameters

            # AIOSQLite supports both QMARK (?) and NAMED_COLON (:name) styles
            # Detect style from SQL: if it contains ':' followed by alphanumeric, it's named style
            # This is more reliable than checking for specific patterns
            has_named_params = bool(re.search(r":\w+", sql))

            if has_named_params:
                # SQL has named placeholders, ensure params are dict
                converted_params = self._convert_parameters_to_driver_format(
                    sql, actual_params, target_style=ParameterStyle.NAMED_COLON
                )
            else:
                # SQL has positional placeholders (? style), ensure params are list/tuple
                converted_params = self._convert_parameters_to_driver_format(
                    sql, actual_params, target_style=ParameterStyle.QMARK
                )

            async with self._get_cursor(txn_conn) as cursor:
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
        self, sql: str, param_list: "Any", connection: "Optional[AiosqliteConnection]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # TypeCoercionMixin handles parameter processing
            converted_param_list = param_list

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
        self, script: str, connection: "Optional[AiosqliteConnection]" = None, **kwargs: "Any"
    ) -> "SQLResult":
        async with managed_transaction_async(self._connection(connection), auto_commit=True) as txn_conn:
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0
            async with self._get_cursor(txn_conn) as cursor:
                for statement in statements:
                    if statement.strip():
                        if not suppress_warnings:
                            temp_sql = SQL(statement, config=self.config)
                            temp_sql._ensure_processed()

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

    def _connection(self, connection: "Optional[AiosqliteConnection]" = None) -> "AiosqliteConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
