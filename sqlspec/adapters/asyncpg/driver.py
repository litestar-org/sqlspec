# ruff: noqa: PLR6301
import contextlib
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
from typing_extensions import TypeAlias

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.driver.mixins import AsyncStorageMixin, SQLTranslatorMixin, ToSchemaMixin
from sqlspec.exceptions import wrap_exceptions
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.logging import get_logger
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy
    from sqlglot.dialects.dialect import DialectType

    from sqlspec.config import InstrumentationConfig


__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = get_logger("adapters.asyncpg")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[
        AsyncpgNativeConnection[Record],
        PoolConnectionProxy[Record],
    ]
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
    AsyncDriverAdapterProtocol[AsyncpgConnection, RowT], SQLTranslatorMixin, AsyncStorageMixin, ToSchemaMixin
):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: "DialectType" = "postgres"
    __supports_arrow__: ClassVar[bool] = True
    __supports_parquet__: ClassVar[bool] = False

    def __init__(
        self,
        connection: "AsyncpgConnection",
        config: "Optional[SQLConfig]" = None,
        instrumentation_config: "Optional[InstrumentationConfig]" = None,
        default_row_type: "type[DictRow]" = DictRow,
    ) -> None:
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=default_row_type,
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        return ParameterStyle.NUMERIC

    async def _execute_statement(
        self,
        statement: SQL,
        connection: Optional[AsyncpgConnection] = None,
        **kwargs: Any,
    ) -> Any:
        if statement.is_script:
            return await self._execute_script(
                statement.to_sql(placeholder_style=ParameterStyle.STATIC),
                connection=connection,
                **kwargs,
            )
        if statement.is_many:
            return await self._execute_many(
                statement.to_sql(placeholder_style=self._get_placeholder_style()),
                statement.parameters,
                connection=connection,
                **kwargs,
            )

        # AsyncPG expects positional parameters with numeric placeholders ($1, $2, etc.)
        sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
        params = statement.get_parameters(style=ParameterStyle.NUMERIC)

        return await self._execute(
            sql,
            params,
            statement,
            connection=connection,
            **kwargs,
        )

    async def _execute(
        self,
        sql: str,
        parameters: Any,
        statement: SQL,
        connection: Optional[AsyncpgConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "asyncpg_execute", "database"):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", sql)
            args_for_driver: list[Any] = []
            if parameters is not None:
                if isinstance(parameters, (list, tuple)):
                    args_for_driver.extend(parameters)
                elif isinstance(parameters, dict):
                    # Don't add empty dicts as parameters
                    if parameters:
                        # AsyncPG expects positional params, not a dict
                        # This shouldn't happen after our conversion in _execute_statement
                        logger.warning(
                            "Unexpected dict parameters in AsyncPG execute: %s",
                            parameters,
                        )
                        args_for_driver.append(parameters)
                else:
                    args_for_driver.append(parameters)
            if self.instrumentation_config.log_parameters and args_for_driver:
                logger.debug("Query parameters: %s", args_for_driver)
            if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                return await conn.fetch(sql, *args_for_driver)
            return await conn.execute(sql, *args_for_driver)

    async def _execute_many(
        self,
        sql: str,
        param_list: Any,
        connection: Optional[AsyncpgConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(self, "asyncpg_execute_many", "database"):
            conn = self._connection(connection)
            params_list: list[tuple[Any, ...]] = []
            if param_list and isinstance(param_list, Sequence):
                for param_set in param_list:
                    if isinstance(param_set, (list, tuple)):
                        params_list.append(tuple(param_set))
                    elif param_set is None:
                        params_list.append(())
                    else:
                        params_list.append((param_set,))
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL (executemany): %s", sql)
            if self.instrumentation_config.log_parameters and params_list:
                logger.debug("Query parameters (batch): %s", params_list)

            result = await conn.executemany(sql, params_list)

            # AsyncPG's executemany returns None, not a status string
            # We need to return information that _wrap_execute_result can use
            # Return a synthetic status message with the batch count
            if result is None and params_list:
                # For executemany, assume each parameter set affects 1 row
                # This is the typical case for INSERT/UPDATE/DELETE operations
                batch_size = len(params_list)
                # Create a synthetic status message that our parser can understand
                return f"INSERT 0 {batch_size}"  # Standard PostgreSQL format

            return result

    async def _execute_script(
        self,
        script: str,
        connection: Optional[AsyncpgConnection] = None,
        **kwargs: Any,
    ) -> Any:
        async with instrument_operation_async(
            self,
            "asyncpg_execute_script",
            "database",
        ):
            conn = self._connection(connection)
            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL script: %s", script)
            return await conn.execute(script)

    async def _wrap_select_result(
        self,
        statement: SQL,
        result: Any,
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SQLResult[ModelDTOT], SQLResult[RowT]]:
        async with instrument_operation_async(self, "asyncpg_wrap_select", "database"):
            records = cast("list[Record]", result)

            if not records:
                # TODO: use the parsed `SQL`
                # AsyncPG limitation: cannot get column names from empty result sets
                # This is a known limitation where schema information is not available
                return SQLResult[RowT](
                    statement=statement,
                    data=cast("list[RowT]", []),
                    column_names=[],
                    operation_type="SELECT",
                )

            column_names = list(records[0].keys())  # asyncpg.Record acts like a dict
            rows_as_dicts: list[dict[str, Any]] = [dict(record) for record in records]

            if self.instrumentation_config.log_results_count:
                logger.debug("Query returned %d rows", len(rows_as_dicts))

            if schema_type:
                converted_data_seq = self.to_schema(
                    data=rows_as_dicts,
                    schema_type=schema_type,
                )
                converted_data_list = list(converted_data_seq) if converted_data_seq is not None else []
                return SQLResult[ModelDTOT](
                    statement=statement,
                    data=converted_data_list,
                    column_names=column_names,
                    operation_type="SELECT",
                )

            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", rows_as_dicts),
                column_names=column_names,
                operation_type="SELECT",
            )

    async def _wrap_execute_result(
        self,
        statement: SQL,
        result: Any,  # This is a status string from conn.execute()
        **kwargs: Any,
    ) -> SQLResult[RowT]:  # Changed return type to match base class
        async with instrument_operation_async(self, "asyncpg_wrap_execute", "database"):
            operation_type = "UNKNOWN"
            with wrap_exceptions(wrap_exceptions=False, suppress=AttributeError):
                if statement.expression:
                    operation_type = str(statement.expression.key).upper()

            rows_affected = 0  # Default

            # Handle None result case gracefully
            status_message = "UNKNOWN" if result is None else str(result)

            match = ASYNC_PG_STATUS_REGEX.match(status_message)
            if match:
                command_tag = match.group(1).upper()
                if command_tag in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
                    try:
                        rows_affected = int(match.group(3))  # group(3) is the row count
                    except (IndexError, ValueError):
                        logger.warning(
                            "Could not parse row count from asyncpg status: %s",
                            status_message,
                        )
                elif (
                    command_tag == "SELECT" and len(match.groups()) >= EXPECTED_REGEX_GROUPS
                ):  # SELECT count (from SELECT INTO?)
                    with contextlib.suppress(IndexError, ValueError):
                        rows_affected = int(match.group(3))
            elif "SCRIPT" in operation_type.upper():
                pass
            elif status_message != "UNKNOWN":  # Don't warn for None results we converted to UNKNOWN
                logger.warning(
                    "Could not parse asyncpg status message: %s",
                    status_message,
                )

            if self.instrumentation_config.log_results_count:
                logger.debug(
                    "Execute operation affected %d rows. Raw status: %s",
                    rows_affected,
                    status_message,
                )

            # Data is empty as conn.execute() for DML returns status string, not records.
            # last_inserted_id and inserted_ids are not available from status string.
            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )

    def _connection(
        self,
        connection: Optional[AsyncpgConnection] = None,
    ) -> AsyncpgConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
