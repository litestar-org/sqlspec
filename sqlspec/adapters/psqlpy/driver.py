"""Psqlpy Driver Implementation."""
# pyright: reportCallIssue=false, reportAttributeAccessIssue=false, reportArgumentType=false

import io
import logging
from typing import TYPE_CHECKING, Any, Optional, cast

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from sqlspec.typing import ConnectionT

from psqlpy import Connection

from sqlspec.driver import AsyncDriverAdapterBase
from sqlspec.driver.mixins import (
    AsyncAdapterCacheMixin,
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.driver.mixins._query_tools import AsyncQueryMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig

if TYPE_CHECKING:
    from sqlglot.dialects.dialect import DialectType

__all__ = ("PsqlpyConnection", "PsqlpyDriver")

if TYPE_CHECKING:
    PsqlpyConnection: TypeAlias = Connection
else:
    # Direct assignment for mypyc runtime
    PsqlpyConnection = Connection
logger = logging.getLogger("sqlspec")


class PsqlpyDriver(
    AsyncDriverAdapterBase,
    AsyncAdapterCacheMixin,
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    ToSchemaMixin,
    AsyncQueryMixin,
):
    """Psqlpy Driver Adapter.

    Modern, high-performance driver for PostgreSQL.
    """

    connection_type = PsqlpyConnection

    dialect: "DialectType" = "postgres"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.NUMERIC,)
    default_parameter_style: ParameterStyle = ParameterStyle.NUMERIC

    def __init__(self, connection: PsqlpyConnection, config: "Optional[SQLConfig]" = None) -> None:
        super().__init__(connection=connection, config=config)

    def _connection(self, connection: "Optional[ConnectionT]" = None) -> "PsqlpyConnection":
        """Get the connection to use for the operation."""
        return cast("PsqlpyConnection", connection or self.connection)

    def _coerce_boolean(self, value: Any) -> Any:
        """PostgreSQL has native boolean support, return as-is."""
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """PostgreSQL has native decimal support."""
        if isinstance(value, str):
            from decimal import Decimal

            return Decimal(value)
        return value

    def _coerce_json(self, value: Any) -> Any:
        """PostgreSQL has native JSON/JSONB support, return as-is."""
        return value

    def _coerce_array(self, value: Any) -> Any:
        """PostgreSQL has native array support, return as-is."""
        return value

    def _wrap_execute_result(self, sql: Any, driver_result: Any) -> SQLResult:
        """Wrap driver result into SQLResult for PsqlPy."""
        # For DML operations (INSERT/UPDATE/DELETE), we don't have row data
        return SQLResult(
            statement=sql,
            data=[],
            rows_affected=-1,  # PsqlPy doesn't provide row count
            operation_type="EXECUTE"
        )
    
    def _wrap_select_result(self, sql: Any, driver_result: Any) -> SQLResult:
        """Wrap driver result into SQLResult for PsqlPy SELECT operations."""
        # Extract data from driver result
        data = []
        column_names = []
        
        if hasattr(driver_result, 'result'):
            # PsqlPy QueryResult has a result() method
            data = driver_result.result()
            if data:
                column_names = list(data[0].keys())
        elif isinstance(driver_result, dict) and 'data' in driver_result:
            data = driver_result['data']
            column_names = driver_result.get('column_names', [])
        
        return SQLResult(
            statement=sql,
            data=data,
            column_names=column_names,
            rows_affected=len(data),
            operation_type="SELECT"
        )

    async def begin(self) -> None:
        """Begin a transaction. PsqlPy starts transactions automatically."""
        # PsqlPy transactions start automatically with first SQL statement
        pass

    async def commit(self) -> None:
        """Commit the current transaction."""
        await self.connection.execute("COMMIT")

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        await self.connection.execute("ROLLBACK")

    async def _execute_statement(
        self, statement: SQL, connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> SQLResult:
        if statement.is_script:
            sql, _ = self._get_compiled_sql(statement, ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        # Determine target style based on what's in the SQL
        target_style = self._select_parameter_style(statement)

        # Compile with the determined style
        sql, params = self._get_compiled_sql(statement, target_style)
        params = self._process_parameters(params)

        if statement.is_many:
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> SQLResult:
        _ = kwargs  # Unused but required for protocol compatibility
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        # PSQLPy expects parameters as a list (for $1, $2, etc.) or dict
        # Ensure we always pass a sequence or mapping, never a scalar
        final_params: Any
        if isinstance(parameters, (list, tuple)):
            final_params = list(parameters)
        elif isinstance(parameters, dict):
            # PSQLPy doesn't like empty dicts, convert to empty list
            final_params = parameters or []
        elif parameters is None:
            final_params = []
        else:
            # Single parameter - wrap in list for NUMERIC style ($1)
            final_params = [parameters]

        if self.returns_rows(statement.expression):
            # Only pass parameters if there are any
            if final_params:
                query_result = await conn.fetch(sql, parameters=final_params)
            else:
                query_result = await conn.fetch(sql)
            dict_rows: list[dict[str, Any]] = []
            if query_result:
                # psqlpy QueryResult has a result() method that returns list of dicts
                dict_rows = query_result.result()
            column_names = list(dict_rows[0].keys()) if dict_rows else []
            return SQLResult(
                statement=statement,
                data=dict_rows,
                column_names=column_names,
                rows_affected=len(dict_rows),
                operation_type="SELECT",
            )

        # Only pass parameters if there are any
        if final_params:
            query_result = await conn.execute(sql, parameters=final_params)
        else:
            query_result = await conn.execute(sql)
        # Note: psqlpy doesn't provide rows_affected for DML operations
        # The QueryResult object only has result(), as_class(), and row_factory() methods
        affected_count = -1  # Unknown, as psqlpy doesn't provide this info
        return SQLResult(
            statement=statement,
            data=[],
            rows_affected=affected_count,
            operation_type=self._determine_operation_type(statement),
            metadata={"status_message": "OK"},
        )

    async def _execute_many(
        self, sql: str, param_list: Any, connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> SQLResult:
        _ = kwargs  # Unused but required for protocol compatibility
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        # PSQLPy expects a list of parameter lists/tuples for execute_many
        if param_list is None:
            final_param_list = []
        elif isinstance(param_list, (list, tuple)):
            # Ensure each parameter set is a list/tuple
            final_param_list = [
                list(params) if isinstance(params, (list, tuple)) else [params] for params in param_list
            ]
        else:
            # Single parameter set - wrap it
            final_param_list = [list(param_list) if isinstance(param_list, (list, tuple)) else [param_list]]

        await conn.execute_many(sql, final_param_list)
        # execute_many doesn't return a value with rows_affected
        affected_count = -1
        return SQLResult(
            statement=SQL(sql, _dialect=self.dialect),
            data=[],
            rows_affected=affected_count,
            operation_type="EXECUTE",
            metadata={"status_message": "OK"},
        )

    async def _execute_script(
        self, script: str, connection: "Optional[ConnectionT]" = None, **kwargs: Any
    ) -> SQLResult:
        # Use provided connection or driver's default connection
        conn = self._connection(connection)

        # Split script into individual statements for validation
        statements = self._split_script_statements(script)
        suppress_warnings = kwargs.get("_suppress_warnings", False)

        executed_count = 0
        total_rows = 0

        # Execute each statement individually for better control and validation
        for statement in statements:
            if statement.strip():
                # Validate each statement unless warnings suppressed
                if not suppress_warnings:
                    # Run validation through pipeline
                    temp_sql = SQL(statement, config=self.config)
                    temp_sql._ensure_processed()
                    # Validation errors are logged as warnings by default

                await conn.execute(statement)
                executed_count += 1
                # psqlpy doesn't provide row count from execute()

        return SQLResult(
            statement=SQL(script, _dialect=self.dialect).as_script(),
            data=[],
            rows_affected=total_rows,
            operation_type="SCRIPT",
            metadata={"status_message": "SCRIPT EXECUTED"},
            total_statements=executed_count,
            successful_statements=executed_count,
        )

    async def _ingest_arrow_table(self, table: "Any", table_name: str, mode: str = "append", **options: Any) -> int:
        _ = options  # Unused but may be used in future implementations
        self._ensure_pyarrow_installed()
        import pyarrow.csv as pacsv

        conn = self._connection(None)
        if mode == "replace":
            from sqlspec import sql

            truncate_stmt = sql.truncate(table_name).build()
            await conn.execute(truncate_stmt.sql)
        elif mode == "create":
            msg = "'create' mode is not supported for psqlpy ingestion."
            raise NotImplementedError(msg)

        buffer = io.BytesIO()
        pacsv.write_csv(table, buffer)
        buffer.seek(0)

        # Use copy_from_raw or copy_from depending on what's available
        # The method name might have changed in newer versions
        copy_method = getattr(conn, "copy_from_raw", getattr(conn, "copy_from_query", None))
        if copy_method:
            await copy_method(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)", data=buffer.read())
            return int(table.num_rows)
        msg = "Connection does not support COPY operations"
        raise NotImplementedError(msg)
