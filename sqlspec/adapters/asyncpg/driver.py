import re
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
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
from sqlspec.utils.logging import get_logger

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy
    from sqlglot.dialects.dialect import DialectType

__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = get_logger("adapters.asyncpg")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection, Any]

# Compiled regex to parse asyncpg status messages like "INSERT 0 1" or "UPDATE 1"
# Group 1: Command Tag (e.g., INSERT, UPDATE)
# Group 2: (Optional) OID count for INSERT (we ignore this)
# Group 3: Rows affected
ASYNC_PG_STATUS_REGEX = re.compile(r"^([A-Z]+)(?:\s+(\d+))?\s+(\d+)$", re.IGNORECASE)

# Expected number of groups in the regex match for row count extraction
EXPECTED_REGEX_GROUPS = 3


class AsyncpgDriver(
    AsyncDriverAdapterProtocol[AsyncpgConnection, RowT],
    SQLTranslatorMixin,
    TypeCoercionMixin,
    AsyncStorageMixin,
    AsyncPipelinedExecutionMixin,
    AsyncAdapterCacheMixin,
    ToSchemaMixin,
):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "postgres"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.NUMERIC,)
    default_parameter_style: ParameterStyle = ParameterStyle.NUMERIC

    def __init__(
        self,
        connection: "AsyncpgConnection",
        config: "Optional[SQLConfig]" = None,
        default_row_type: "type[DictRow]" = dict[str, Any],
    ) -> None:
        super().__init__(connection=connection, config=config, default_row_type=default_row_type)

    # AsyncPG-specific type coercion overrides (PostgreSQL has rich native types)
    def _coerce_boolean(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native boolean support."""
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native decimal/numeric support."""
        return value

    def _coerce_json(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native JSON/JSONB support."""
        # AsyncPG can handle dict/list directly for JSON columns
        return value

    def _coerce_array(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native array support."""
        if isinstance(value, tuple):
            return list(value)
        return value

    async def _execute_statement(
        self, statement: SQL, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
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
            for style in detected_styles:
                if style in self.supported_parameter_styles:
                    target_style = style
                    break

        if statement.is_many:
            sql, params = self._get_compiled_sql(statement, target_style)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = self._get_compiled_sql(statement, target_style)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        if statement.is_many:
            # This should have gone to _execute_many, redirect it
            return await self._execute_many(sql, parameters, connection=connection, **kwargs)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Convert parameters using consolidated utility
            converted_params = convert_parameter_sequence(parameters)
            # AsyncPG expects parameters as *args, not a single list
            args_for_driver: list[Any] = []
            if converted_params:
                # converted_params is already a list, just use it directly
                args_for_driver = converted_params

            if self.returns_rows(statement.expression):
                records = await txn_conn.fetch(sql, *args_for_driver)
                data = [dict(record) for record in records]
                column_names = list(records[0].keys()) if records else []
                return SQLResult(
                    statement=statement,
                    data=cast("list[RowT]", data),
                    column_names=column_names,
                    rows_affected=len(records),
                    operation_type="SELECT",
                )

            status = await txn_conn.execute(sql, *args_for_driver)
            # Parse row count from status string
            rows_affected = 0
            if status and isinstance(status, str):
                match = ASYNC_PG_STATUS_REGEX.match(status)
                if match and len(match.groups()) >= EXPECTED_REGEX_GROUPS:
                    rows_affected = int(match.group(3))

            operation_type = self._determine_operation_type(statement)
            return SQLResult(
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status or "OK"},
            )

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Normalize parameter list using consolidated utility
            converted_param_list = convert_parameter_sequence(param_list)

            params_list: list[tuple[Any, ...]] = []
            rows_affected = 0
            if converted_param_list:
                for param_set in converted_param_list:
                    if isinstance(param_set, (list, tuple)):
                        params_list.append(tuple(param_set))
                    elif param_set is None:
                        params_list.append(())
                    else:
                        params_list.append((param_set,))

                await txn_conn.executemany(sql, params_list)
                # AsyncPG's executemany returns None, not a status string
                # We need to use the number of parameter sets as the row count
                rows_affected = len(params_list)

            return SQLResult(
                statement=SQL(sql, _dialect=self.dialect),
                data=[],
                rows_affected=rows_affected,
                operation_type="EXECUTE",
                metadata={"status_message": "OK"},
            )

    async def _execute_script(
        self, script: str, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> SQLResult[RowT]:
        # Use provided connection or driver's default connection
        conn = connection if connection is not None else self._connection(None)

        async with managed_transaction_async(conn, auto_commit=True) as txn_conn:
            # Split script into individual statements for validation
            statements = self._split_script_statements(script)
            suppress_warnings = kwargs.get("_suppress_warnings", False)

            executed_count = 0
            total_rows = 0
            last_status = None

            # Execute each statement individually for better control and validation
            for statement in statements:
                if statement.strip():
                    # Validate each statement unless warnings suppressed
                    if not suppress_warnings:
                        # Run validation through pipeline
                        temp_sql = SQL(statement, config=self.config)
                        temp_sql._ensure_processed()
                        # Validation errors are logged as warnings by default

                    status = await txn_conn.execute(statement)
                    executed_count += 1
                    last_status = status
                    # AsyncPG doesn't provide row count from execute()

            return SQLResult(
                statement=SQL(script, _dialect=self.dialect).as_script(),
                data=[],
                rows_affected=total_rows,
                operation_type="SCRIPT",
                metadata={"status_message": last_status or "SCRIPT EXECUTED"},
                total_statements=executed_count,
                successful_statements=executed_count,
            )

    def _connection(self, connection: Optional[AsyncpgConnection] = None) -> AsyncpgConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection

    async def _execute_pipeline_native(self, operations: "list[Any]", **options: Any) -> "list[SQLResult[RowT]]":
        """Native pipeline execution using AsyncPG's efficient batch handling.

        Note: AsyncPG doesn't have explicit pipeline support like Psycopg, but we can
        achieve similar performance benefits through careful batching and transaction
        management.

        Args:
            operations: List of PipelineOperation objects
            **options: Pipeline configuration options

        Returns:
            List of SQLResult objects from all operations
        """

        results: list[Any] = []
        connection = self._connection()

        # Use a single transaction for all operations
        async with connection.transaction():
            for i, op in enumerate(operations):
                await self._execute_pipeline_operation(connection, i, op, options, results)

        return results

    async def _execute_pipeline_operation(
        self, connection: Any, i: int, op: Any, options: dict[str, Any], results: list[Any]
    ) -> None:
        """Execute a single pipeline operation with error handling."""
        from sqlspec.exceptions import PipelineExecutionError

        try:
            sql_str = op.sql.to_sql(placeholder_style=ParameterStyle.NUMERIC)
            params = self._convert_to_positional_params(op.sql.parameters)

            filtered_sql = self._apply_operation_filters(op.sql, op.filters)
            if filtered_sql != op.sql:
                sql_str = filtered_sql.to_sql(placeholder_style=ParameterStyle.NUMERIC)
                params = self._convert_to_positional_params(filtered_sql.parameters)

            # Execute based on operation type
            if op.operation_type == "execute_many":
                # AsyncPG has native executemany support
                status = await connection.executemany(sql_str, params)
                # Parse row count from status (e.g., "INSERT 0 5")
                rows_affected = self._parse_asyncpg_status(status)
                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", []),
                    rows_affected=rows_affected,
                    operation_type="EXECUTE",
                    metadata={"status_message": status},
                )
            elif op.operation_type == "select":
                # Use fetch for SELECT statements
                rows = await connection.fetch(sql_str, *params)
                data = [dict(record) for record in rows] if rows else []
                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", data),
                    rows_affected=len(data),
                    operation_type="SELECT",
                    metadata={"column_names": list(rows[0].keys()) if rows else []},
                )
            elif op.operation_type == "execute_script":
                # For scripts, split and execute each statement
                script_statements = self._split_script_statements(op.sql.to_sql())
                total_affected = 0
                last_status = ""

                for stmt in script_statements:
                    if stmt.strip():
                        status = await connection.execute(stmt)
                        total_affected += self._parse_asyncpg_status(status)
                        last_status = status

                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", []),
                    rows_affected=total_affected,
                    operation_type="SCRIPT",
                    metadata={"status_message": last_status, "statements_executed": len(script_statements)},
                )
            else:
                status = await connection.execute(sql_str, *params)
                rows_affected = self._parse_asyncpg_status(status)
                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", []),
                    rows_affected=rows_affected,
                    operation_type="EXECUTE",
                    metadata={"status_message": status},
                )

            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if options.get("continue_on_error"):
                error_result = SQLResult[RowT](
                    statement=op.sql, error=e, operation_index=i, parameters=op.original_params, data=[]
                )
                results.append(error_result)
            else:
                # Transaction will be rolled back automatically
                msg = f"AsyncPG pipeline failed at operation {i}: {e}"
                raise PipelineExecutionError(
                    msg, operation_index=i, partial_results=results, failed_operation=op
                ) from e

    def _convert_to_positional_params(self, params: Any) -> "tuple[Any, ...]":
        """Convert parameters to positional format for AsyncPG.

        AsyncPG requires parameters as positional arguments for $1, $2, etc.

        Args:
            params: Parameters in various formats

        Returns:
            Tuple of positional parameters
        """
        if params is None:
            return ()
        if isinstance(params, dict):
            if not params:
                return ()
            # This assumes the SQL was compiled with NUMERIC style
            max_param = 0
            for key in params:
                if isinstance(key, str) and key.startswith("param_"):
                    try:
                        param_num = int(key[6:])  # Extract number from "param_N"
                        max_param = max(max_param, param_num)
                    except ValueError:
                        continue

            if max_param > 0:
                # Rebuild positional args from param_0, param_1, etc.
                positional = []
                for i in range(max_param + 1):
                    param_key = f"param_{i}"
                    if param_key in params:
                        positional.append(params[param_key])
                return tuple(positional)
            # Fall back to dict values in arbitrary order
            return tuple(params.values())
        if isinstance(params, (list, tuple)):
            return tuple(params)
        return (params,)

    def _apply_operation_filters(self, sql: "SQL", filters: "list[Any]") -> "SQL":
        """Apply filters to a SQL object for pipeline operations."""
        if not filters:
            return sql

        result_sql = sql
        for filter_obj in filters:
            if hasattr(filter_obj, "apply"):
                result_sql = filter_obj.apply(result_sql)

        return result_sql

    def _split_script_statements(self, script: str, strip_trailing_semicolon: bool = False) -> "list[str]":
        """Split a SQL script into individual statements."""
        # Simple splitting on semicolon - could be enhanced with proper SQL parsing
        statements = [stmt.strip() for stmt in script.split(";")]
        return [stmt for stmt in statements if stmt]

    @staticmethod
    def _parse_asyncpg_status(status: str) -> int:
        """Parse AsyncPG status string to extract row count.

        Args:
            status: Status string like "INSERT 0 1", "UPDATE 3", "DELETE 2"

        Returns:
            Number of affected rows, or 0 if cannot parse
        """
        if not status:
            return 0

        match = ASYNC_PG_STATUS_REGEX.match(status.strip())
        if match:
            # For INSERT: "INSERT 0 5" -> groups: (INSERT, 0, 5)
            # For UPDATE/DELETE: "UPDATE 3" -> groups: (UPDATE, None, 3)
            groups = match.groups()
            if len(groups) >= EXPECTED_REGEX_GROUPS:
                try:
                    # The last group is always the row count
                    return int(groups[-1])
                except (ValueError, IndexError):
                    pass

        return 0
