import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
from typing_extensions import TypeAlias

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, ToSchemaMixin, TypeCoercionMixin
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
    ToSchemaMixin,
):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "postgres"
    supported_parameter_styles: "tuple[ParameterStyle, ...]" = (ParameterStyle.NUMERIC,)
    default_parameter_style: ParameterStyle = ParameterStyle.NUMERIC
    __slots__ = ("config", "connection", "default_row_type")

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
        # Keep arrays as-is, AsyncPG handles them natively
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
            params = self._process_parameters(params)
            return await self._execute_many(sql, params, connection=connection, **kwargs)

        sql, params = statement.compile(placeholder_style=target_style)
        params = self._process_parameters(params)
        return await self._execute(sql, params, statement, connection=connection, **kwargs)

    async def _execute(
        self, sql: str, parameters: Any, statement: SQL, connection: Optional[AsyncpgConnection] = None, **kwargs: Any
    ) -> Union[SelectResultDict, DMLResultDict]:
        conn = self._connection(connection)
        # Parameters are already in the correct format from compile()
        # AsyncPG expects parameters as *args, not a single list
        args_for_driver: list[Any] = []
        if parameters is not None:
            if isinstance(parameters, (list, tuple)):
                args_for_driver.extend(parameters)
            else:
                args_for_driver.append(parameters)

        if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
            records = await conn.fetch(sql, *args_for_driver)
            # Get column names from first record or empty list
            column_names = list(records[0].keys()) if records else []
            result: SelectResultDict = {"data": records, "column_names": column_names, "rows_affected": len(records)}
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

            result = await conn.executemany(sql, params_list)

            if result and isinstance(result, str):
                # Parse PostgreSQL status like "INSERT 0 5"
                match = ASYNC_PG_STATUS_REGEX.match(result)
                if match and len(match.groups()) >= EXPECTED_REGEX_GROUPS:
                    rows_affected = int(match.group(3))

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
