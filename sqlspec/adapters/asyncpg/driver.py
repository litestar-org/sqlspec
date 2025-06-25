import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
from typing_extensions import TypeAlias

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import (
    AsyncPipelinedExecutionMixin,
    AsyncStorageMixin,
    SQLTranslatorMixin,
    ToSchemaMixin,
    TypeCoercionMixin,
)
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import DMLResultDict, ScriptResultDict, SelectResultDict, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
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
    ToSchemaMixin,
):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "postgres"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.NUMERIC,)
    default_parameter_style: ParameterStyle = ParameterStyle.NUMERIC
    __slots__ = ()

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
        # Keep booleans as-is, AsyncPG handles them natively
        return value

    def _coerce_decimal(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native decimal/numeric support."""
        # Keep decimals as-is, AsyncPG handles them natively
        return value

    def _coerce_json(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native JSON/JSONB support."""
        # AsyncPG can handle dict/list directly for JSON columns
        return value

    def _coerce_array(self, value: Any) -> Any:
        """AsyncPG/PostgreSQL has native array support."""
        # Convert tuples to lists for consistency
        if isinstance(value, tuple):
            return list(value)
        # Keep other arrays as-is, AsyncPG handles them natively
        return value

    async def _execute_statement(
        self, statement: SQL, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict, ScriptResultDict]:
        if statement.is_script:
            sql, _ = statement.compile(placeholder_style=ParameterStyle.STATIC)
            return await self._execute_script(sql, connection=connection, **kwargs)

        detected_styles = {p.style for p in statement.parameter_info}
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
            sql, params = statement.compile(placeholder_style=target_style)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        # Process parameters to handle TypedParameter objects
        parameters = self._process_parameters(parameters)

        # Check if this is actually a many operation that was misrouted
        if statement.is_many:
            # This should have gone to _execute_many, redirect it
            return await self._execute_many(sql, parameters, connection=connection, **kwargs)

        # AsyncPG expects parameters as *args, not a single list
        args_for_driver: list[Any] = []

        if parameters is not None:
            if isinstance(parameters, (list, tuple)):
                args_for_driver.extend(parameters)
            else:
                args_for_driver.append(parameters)

        if self.returns_rows(statement.expression):
            records = await conn.fetch(sql, *args_for_driver)
            # Convert asyncpg Records to dicts
            data = [dict(record) for record in records]
            # Get column names from first record or empty list
            column_names = list(records[0].keys()) if records else []
            result: SelectResultDict = {"data": data, "column_names": column_names, "rows_affected": len(records)}
            return result

        status = await conn.execute(sql, *args_for_driver)
        # Parse row count from status string
        rows_affected = 0
        if status and isinstance(status, str):
            match = ASYNC_PG_STATUS_REGEX.match(status)
            if match and len(match.groups()) >= EXPECTED_REGEX_GROUPS:
                rows_affected = int(match.group(3))

        dml_result: DMLResultDict = {"rows_affected": rows_affected, "status_message": status or "OK"}
        return dml_result

    async def _execute_many(
        self, sql: str, param_list: Any, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> DMLResultDict:
        conn = self._connection(connection)
        # Process parameters to handle TypedParameter objects
        param_list = self._process_parameters(param_list)

        params_list: list[tuple[Any, ...]] = []
        rows_affected = 0
        if param_list and isinstance(param_list, Sequence):
            for param_set in param_list:
                if isinstance(param_set, (list, tuple)):
                    params_list.append(tuple(param_set))
                elif param_set is None:
                    params_list.append(())
                else:
                    params_list.append((param_set,))

            await conn.executemany(sql, params_list)
            # AsyncPG's executemany returns None, not a status string
            # We need to use the number of parameter sets as the row count
            rows_affected = len(params_list)

        dml_result: DMLResultDict = {"rows_affected": rows_affected, "status_message": "OK"}
        return dml_result

    async def _execute_script(
        self, script: str, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> ScriptResultDict:
        conn = self._connection(connection)
        status = await conn.execute(script)

        result: ScriptResultDict = {
            "statements_executed": -1,  # AsyncPG doesn't provide statement count
            "status_message": status or "SCRIPT EXECUTED",
        }
        return result

    async def _wrap_select_result(
        self, statement: SQL, result: SelectResultDict, schema_type: Optional[type[ModelDTOT]] = None, **kwargs: Any
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        records = cast("list[Record]", result["data"])
        column_names = result["column_names"]
        rows_affected = result["rows_affected"]

        rows_as_dicts: list[dict[str, Any]] = [dict(record) for record in records]

        if schema_type:
            converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
            converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
            return SQLResult[ModelDTOT](
                statement=statement,
                data=converted_data_list,
                column_names=column_names,
                rows_affected=rows_affected,
                operation_type="SELECT",
            )

        return SQLResult[RowT](
            statement=statement,
            data=cast("list[RowT]", rows_as_dicts),
            column_names=column_names,
            rows_affected=rows_affected,
            operation_type="SELECT",
        )

    async def _wrap_execute_result(
        self, statement: SQL, result: Union[DMLResultDict, ScriptResultDict], **kwargs: Any
    ) -> SQLResult[RowT]:
        operation_type = "UNKNOWN"
        if statement.expression:
            operation_type = str(statement.expression.key).upper()

        # Handle script results
        if "statements_executed" in result:
            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=0,
                operation_type="SCRIPT",
                metadata={
                    "status_message": result.get("status_message", ""),
                    "statements_executed": result.get("statements_executed", -1),
                },
            )

        # Handle DML results
        rows_affected = cast("int", result.get("rows_affected", -1))
        status_message = result.get("status_message", "")

        return SQLResult[RowT](
            statement=statement,
            data=cast("list[RowT]", []),
            rows_affected=rows_affected,
            operation_type=operation_type,
            metadata={"status_message": status_message},
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
            # Convert parameters to positional for AsyncPG (requires $1, $2, etc.)
            sql_str = op.sql.to_sql(placeholder_style=ParameterStyle.NUMERIC)
            params = self._convert_to_positional_params(op.sql.parameters)

            # Apply operation-specific filters
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
                    operation_type="execute_many",
                    metadata={"status_message": status},
                )
            elif op.operation_type == "select":
                # Use fetch for SELECT statements
                rows = await connection.fetch(sql_str, *params)
                # Convert AsyncPG Records to dictionaries
                data = [dict(record) for record in rows] if rows else []
                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", data),
                    rows_affected=len(data),
                    operation_type="select",
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
                    operation_type="execute_script",
                    metadata={"status_message": last_status, "statements_executed": len(script_statements)},
                )
            else:
                status = await connection.execute(sql_str, *params)
                rows_affected = self._parse_asyncpg_status(status)
                result = SQLResult[RowT](
                    statement=op.sql,
                    data=cast("list[RowT]", []),
                    rows_affected=rows_affected,
                    operation_type="execute",
                    metadata={"status_message": status},
                )

            # Add operation context
            result.operation_index = i
            result.pipeline_sql = op.sql
            results.append(result)

        except Exception as e:
            if options.get("continue_on_error"):
                # Create error result
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
            # Convert dict to positional based on $1, $2, etc. order
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
