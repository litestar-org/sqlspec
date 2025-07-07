import logging
from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional, Union

from asyncmy import Connection
from typing_extensions import TypeAlias

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

if TYPE_CHECKING:
    from asyncmy.cursors import Cursor, DictCursor
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AsyncmyConnection", "AsyncmyDriver")

logger = logging.getLogger("sqlspec")

AsyncmyConnection: TypeAlias = Connection


class AsyncmyDriver(
    AsyncDriverAdapterProtocol[AsyncmyConnection, RowT],
    AsyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
):
    """Asyncmy MySQL/MariaDB Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "mysql"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.POSITIONAL_PYFORMAT,)
    default_parameter_style: ParameterStyle = ParameterStyle.POSITIONAL_PYFORMAT

    def __init__(
        self,
        connection: AsyncmyConnection,
        config: Optional[SQLConfig] = None,
        default_row_type: type[DictRow] = DictRow,
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    @asynccontextmanager
    async def _get_cursor(
        self, connection: "Optional[AsyncmyConnection]" = None
    ) -> "AsyncGenerator[Union[Cursor, DictCursor], None]":
        conn = self._connection(connection)
        cursor = conn.cursor()
        try:
            yield cursor
        finally:
            await cursor.close()

    async def _execute_statement(
        self, statement: SQL, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Detect parameter styles in the SQL
        detected_styles = set()
        sql_str = statement.to_sql(placeholder_style=None)  # Get raw SQL
        validator = self.config.parameter_validator if self.config else ParameterValidator()
        param_infos = validator.extract_parameters(sql_str)
        if param_infos:
            detected_styles = {p.style for p in param_infos}

        # Determine target style based on what's in the SQL
        target_style = self.default_parameter_style

        # Check if there are unsupported styles
        unsupported_styles = detected_styles - set(self.supported_parameter_styles)
        if unsupported_styles:
            # Force conversion to default style
            target_style = self.default_parameter_style
        elif detected_styles:
            # Prefer the first supported style found
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        # Compile with the determined style
        sql, params = self._get_compiled_sql(statement, target_style)

        if statement.is_many:
            params = self._process_parameters(params)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        params = self._process_parameters(params)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Convert parameters using consolidated utility
            converted_params = convert_parameter_sequence(parameters)
            # AsyncMy doesn't like empty lists/tuples, convert to None
            final_params = converted_params[0] if converted_params and len(converted_params) == 1 else converted_params
            if not final_params:
                final_params = None

            async with self._get_cursor(txn_conn) as cursor:
                # AsyncMy expects list/tuple parameters or dict for named params
                await cursor.execute(sql, final_params)

                if self.returns_rows(statement.expression):
                    # For SELECT queries, fetch data and return SQLResult
                    data = await cursor.fetchall()
                    column_names = [desc[0] for desc in cursor.description or []]
                    return SQLResult(
                        statement=statement,
                        data=data,
                        column_names=column_names,
                        rows_affected=len(data),
                        operation_type="SELECT",
                    )

                # For DML/DDL queries
                return SQLResult(
                    statement=statement,
                    data=[],
                    rows_affected=cursor.rowcount if cursor.rowcount is not None else -1,
                    operation_type=self._determine_operation_type(statement),
                    metadata={"status_message": "OK"},
                )

    async def _execute_many(
        self, sql: str, param_list: Any, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)

            params_list: list[Union[list[Any], tuple[Any, ...]]] = []
            if converted_param_list and isinstance(converted_param_list, Sequence):
                for param_set in converted_param_list:
                    if isinstance(param_set, (list, tuple)):
                        params_list.append(param_set)
                    elif param_set is None:
                        params_list.append([])
                    else:
                        params_list.append([param_set])

            async with self._get_cursor(txn_conn) as cursor:
                await cursor.executemany(sql, params_list)
                return SQLResult(
                    statement=SQL(sql, _dialect=self.dialect),
                    data=[],
                    rows_affected=cursor.rowcount if cursor.rowcount != -1 else len(params_list),
                    operation_type="EXECUTE",
                    metadata={"status_message": "OK"},
                )

    async def _execute_script(
        self, script: str, connection: "Optional[AsyncmyConnection]" = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # AsyncMy may not support multi-statement scripts without CLIENT_MULTI_STATEMENTS flag
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)
            statements_executed = 0
            total_rows = 0

            async with self._get_cursor(txn_conn) as cursor:
                for statement_str in statements:
                    if statement_str:
                        # Validate each statement unless warnings suppressed
                        if not suppress_warnings:
                            # Run validation through pipeline
                            temp_sql = SQL(statement_str, config=self.config)
                            temp_sql._ensure_processed()
                            # Validation errors are logged as warnings by default

                        await cursor.execute(statement_str)
                        statements_executed += 1
                        total_rows += cursor.rowcount if cursor.rowcount is not None else 0

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": "SCRIPT EXECUTED"},
                total_statements=statements_executed,
                successful_statements=statements_executed,
            )

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        self._ensure_pyarrow_installed()
        conn = self._connection(None)
        async with managed_transaction_async(conn, auto_commit=True) as txn_conn, self._get_cursor(txn_conn) as cursor:
            if mode == "replace":
                await cursor.execute(f"TRUNCATE TABLE {table_name}")
            elif mode == "create":
                msg = "'create' mode is not supported for asyncmy ingestion."
                raise NotImplementedError(msg)

            data_for_ingest = table.to_pylist()
            if not data_for_ingest:
                return 0

            # Generate column placeholders: %s, %s, etc.
            num_columns = len(data_for_ingest[0])
            placeholders = ", ".join("%s" for _ in range(num_columns))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
            await cursor.executemany(sql, data_for_ingest)
            return cursor.rowcount if cursor.rowcount is not None else -1

    def _connection(self, connection: Optional["AsyncmyConnection"] = None) -> "AsyncmyConnection":
        """Get the connection to use for the operation."""
        return connection or self.connection
