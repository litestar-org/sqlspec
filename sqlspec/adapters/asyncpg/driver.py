# ruff: noqa: PLR6301
import contextlib
import logging
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
from typing_extensions import TypeAlias

from sqlspec.driver import AsyncDriverAdapterProtocol
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ArrowResult, SQLResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, RowT
from sqlspec.utils.telemetry import instrument_operation_async

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    from sqlspec.config import InstrumentationConfig


__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection, Any]

# Compiled regex to parse asyncpg status messages like "INSERT 0 1" or "UPDATE 1"
# Group 1: Command Tag (e.g., INSERT, UPDATE)
# Group 2: (Optional) OID count for INSERT (we ignore this)
# Group 3: Rows affected
ASYNC_PG_STATUS_REGEX = re.compile(r"^([A-Z]+)(?:\s+\d+)?\s+(\d+)$", re.IGNORECASE)

# Expected number of groups in the regex match for row count extraction
EXPECTED_REGEX_GROUPS = 3


class AsyncpgDriver(
    AsyncDriverAdapterProtocol[AsyncpgConnection, RowT],
    AsyncArrowMixin[AsyncpgConnection],
    SQLTranslatorMixin[AsyncpgConnection],
    ResultConverter,
):
    """AsyncPG PostgreSQL Driver Adapter. Modern protocol implementation."""

    dialect: str = "postgres"
    __supports_arrow__: ClassVar[bool] = True

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
        async with instrument_operation_async(self, "asyncpg_execute", "database"):
            conn = self._connection(connection)

            if statement.is_script:
                final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
                if self.instrumentation_config.log_queries:
                    logger.debug("Executing SQL script: %s", final_sql)
                # asyncpg's execute method can handle multi-statement strings.
                # Parameters are not typically passed separately for scripts.
                return await conn.execute(final_sql)

            final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())
            params_to_execute = statement.parameters  # These are the merged and processed params

            if self.instrumentation_config.log_queries:
                logger.debug("Executing SQL: %s", final_sql)

            args_for_driver: list[Any] = []
            if statement.is_many:
                # asyncpg executemany expects a list of tuples/lists.
                # statement.parameters should already be a sequence of sequences/dicts.
                # We need to ensure each inner sequence is a tuple for asyncpg if it's not a dict.
                params_list: list[tuple[Any, ...]] = []
                if params_to_execute and isinstance(params_to_execute, Sequence):
                    for param_set in params_to_execute:
                        if isinstance(param_set, (list, tuple)):
                            params_list.append(tuple(param_set))
                        elif param_set is None:  # Should not happen with valid is_many
                            params_list.append(())
                        else:  # Should be a dict or a single value that needs to be tuple-ized
                            # This path might indicate an issue if not a dict, as asyncpg expects sequences.
                            # For now, assume parameters are correctly list of lists/tuples or list of dicts.
                            # If it's a list of dicts, asyncpg handles it. If list of lists/tuples, also fine.
                            # The guide focuses on simplification, assuming SQL object prepares params well.
                            params_list.append(
                                tuple(param_set) if isinstance(param_set, list) else (param_set,)
                            )  # Ensure tuple for single items if not dict

                if self.instrumentation_config.log_parameters and params_list:
                    logger.debug("Query parameters (batch): %s", params_list)

                # The type for params_list in executemany is list[tuple[Any, ...]]
                # or can be list[dict[str, Any]] if using named placeholders (not for numeric)
                return await conn.executemany(final_sql, params_list)

            # Single execution
            # asyncpg execute expects parameters as separate arguments (*args)
            # statement.parameters should be a single sequence (list/tuple) or dict
            if params_to_execute is not None:
                if isinstance(params_to_execute, (list, tuple)):
                    args_for_driver.extend(params_to_execute)
                elif isinstance(params_to_execute, dict):  # asyncpg can handle dict for named params
                    args_for_driver.append(params_to_execute)
                else:  # Single non-sequence parameter
                    args_for_driver.append(params_to_execute)

            if self.instrumentation_config.log_parameters and args_for_driver:
                logger.debug("Query parameters: %s", args_for_driver)

            if AsyncDriverAdapterProtocol.returns_rows(statement.expression):
                return await conn.fetch(final_sql, *args_for_driver)
            return await conn.execute(final_sql, *args_for_driver)

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
                # Even if no records, try to get column names if possible (e.g. from statement, though not standard here)
                # For now, assume empty column_names if no records.
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
                converted_data_seq = self.to_schema(data=rows_as_dicts, schema_type=schema_type)
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
            if statement.expression and hasattr(statement.expression, "key"):
                operation_type = str(statement.expression.key).upper()

            rows_affected = 0  # Default
            status_message = str(result)  # Ensure it's a string

            match = ASYNC_PG_STATUS_REGEX.match(status_message)
            if match:
                command_tag = match.group(1).upper()
                if command_tag in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
                    try:
                        rows_affected = int(match.group(3))  # group(3) is the row count
                    except (IndexError, ValueError):
                        logger.warning("Could not parse row count from asyncpg status: %s", status_message)
                elif (
                    command_tag == "SELECT" and len(match.groups()) >= EXPECTED_REGEX_GROUPS
                ):  # SELECT count (from SELECT INTO?)
                    with contextlib.suppress(IndexError, ValueError):
                        rows_affected = int(match.group(3))
            elif "SCRIPT" in operation_type.upper():
                pass
            else:
                logger.warning("Could not parse asyncpg status message: %s", status_message)

            if self.instrumentation_config.log_results_count:
                logger.debug("Execute operation affected %d rows. Raw status: %s", rows_affected, status_message)

            # Data is empty as conn.execute() for DML returns status string, not records.
            # last_inserted_id and inserted_ids are not available from status string.
            return SQLResult[RowT](
                statement=statement,
                data=cast("list[RowT]", []),
                rows_affected=rows_affected,
                operation_type=operation_type,
                metadata={"status_message": status_message},
            )

    async def _select_to_arrow_impl(
        self,
        stmt_obj: "SQL",  # Changed from statement, parameters, filters, config
        connection: "AsyncpgConnection",  # Explicitly typed connection
        **kwargs: Any,
    ) -> "ArrowResult":
        # SQL object (stmt_obj) is already built and validated by the mixin.
        # returns_rows check is done by the mixin.
        # Instrumentation is handled by the mixin.
        import pyarrow as pa  # Ensure pyarrow is imported

        final_sql = stmt_obj.to_sql(placeholder_style=self._get_placeholder_style())

        # Parameters for asyncpg's fetch are passed as *args.
        # stmt_obj.parameters should hold the sequence of parameters.
        params_for_driver: list[Any] = []
        if stmt_obj.parameters is not None:
            if isinstance(stmt_obj.parameters, (list, tuple)):
                params_for_driver.extend(stmt_obj.parameters)
            # Asyncpg also accepts a single dict for named parameters, but we use numeric ($1, $2).
            # So, expecting a list/tuple here.
            else:  # Single parameter
                params_for_driver.append(stmt_obj.parameters)

        if self.instrumentation_config.log_queries:  # Keep specific logging if desired
            logger.debug("Executing asyncpg Arrow query: %s", final_sql)
        if self.instrumentation_config.log_parameters and params_for_driver:
            logger.debug("Query parameters for asyncpg Arrow: %s", params_for_driver)

        # conn is passed as 'connection' argument from the mixin
        records = await connection.fetch(final_sql, *params_for_driver)

        if not records:
            # Attempt to get column names from the statement or prepared statement if possible,
            # though asyncpg.fetch returning empty list means no data, schema might still be inferable
            # from a prepared statement's attributes if one was used. Here, simple case.
            # For now, if records is empty, we can't get keys from records[0].
            # We need a way to get column names even for empty results. This might require
            # conn.prepare() then getting attributes, or relying on stmt_obj if it carries them.
            # As a fallback, use empty names list.
            return ArrowResult(statement=stmt_obj, data=pa.Table.from_arrays([], names=[]))

        # Convert asyncpg records to Arrow table
        column_names = list(records[0].keys())  # Records are dict-like

        # Efficiently convert list of dict-like records to list of lists (columnar)
        columns_data = []
        for col_name in column_names:
            column_values = [record[col_name] for record in records]
            columns_data.append(column_values)

        arrow_table = pa.Table.from_arrays(columns_data, names=column_names)
        return ArrowResult(statement=stmt_obj, data=arrow_table)

    def _connection(self, connection: Optional[AsyncpgConnection] = None) -> AsyncpgConnection:
        """Get the connection to use for the operation."""
        return connection or self.connection
