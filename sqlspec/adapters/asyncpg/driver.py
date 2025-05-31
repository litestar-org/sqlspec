import logging
import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Union, cast

from asyncpg import Connection as AsyncpgNativeConnection
from asyncpg import Record
from typing_extensions import TypeAlias

from sqlspec.config import InstrumentationConfig
from sqlspec.driver import AsyncDriverAdapterProtocol, CommonDriverAttributes
from sqlspec.statement.mixins import AsyncArrowMixin, ResultConverter, SQLTranslatorMixin
from sqlspec.statement.parameters import ParameterStyle
from sqlspec.statement.result import ExecuteResult, SelectResult
from sqlspec.statement.sql import SQL, SQLConfig
from sqlspec.typing import DictRow, ModelDTOT, SQLParameterType
from sqlspec.utils.telemetry import instrument_async

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy


__all__ = ("AsyncpgConnection", "AsyncpgDriver")

logger = logging.getLogger("sqlspec")

if TYPE_CHECKING:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection[Record], PoolConnectionProxy[Record]]
else:
    AsyncpgConnection: TypeAlias = Union[AsyncpgNativeConnection, Any]

# Compile the row count regex once for efficiency
ROWCOUNT_REGEX = re.compile(r"^(?:INSERT|UPDATE|DELETE|MERGE) \d+ (\d+)$", re.IGNORECASE)


class AsyncpgDriver(
    AsyncDriverAdapterProtocol[AsyncpgConnection, DictRow],
    SQLTranslatorMixin[AsyncpgConnection],
    AsyncArrowMixin[AsyncpgConnection],
    ResultConverter,
):
    """AsyncPG Postgres Driver Adapter.

    This driver implements the new unified SQLSpec protocol with the core 3 methods:
    - execute() - Universal method for all SQL operations
    - execute_many() - Batch operations
    - execute_script() - Multi-statement scripts

    Enhanced Features:
    - Full SQLStatement integration with filters and validation
    - PostgreSQL-specific parameter style handling ($1, $2, etc.)
    - Comprehensive parameter validation and security checks
    - Support for filter composition (pagination, search, etc.)
    """

    __supports_arrow__: ClassVar[bool] = False  # asyncpg doesn't support Arrow natively
    dialect: str = "postgres"
    config: SQLConfig

    def __init__(
        self,
        connection: AsyncpgConnection,
        config: Optional[SQLConfig] = None,
        instrumentation_config: Optional[InstrumentationConfig] = None,
    ) -> None:
        """Initialize the AsyncPG driver adapter."""
        super().__init__(
            connection=connection,
            config=config,
            instrumentation_config=instrumentation_config,
            default_row_type=DictRow,
        )

    def _get_placeholder_style(self) -> ParameterStyle:
        """Return the placeholder style for PostgreSQL ($1, $2, etc.)."""
        return ParameterStyle.NUMERIC

    @instrument_async(operation_type="database")
    async def _execute_impl(
        self,
        statement: SQL,
        parameters: Optional[SQLParameterType] = None,
        connection: Optional[AsyncpgConnection] = None,
        config: Optional[SQLConfig] = None,
        is_many: bool = False,
        is_script: bool = False,
        **kwargs: Any,
    ) -> Any:
        conn = self._connection(connection)

        final_sql = statement.to_sql(placeholder_style=self._get_placeholder_style())

        # Parameters for asyncpg need to be passed as separate arguments *args
        # or as a list/tuple for executemany.
        args_for_driver: list[Any] = []  # For single execute or script
        args_for_driver_many: list[Sequence[Any]] = []  # For executemany

        if is_script:
            # asyncpg's execute for scripts doesn't take parameters in the same way as query methods.
            # Parameters should be rendered into the script if needed, using STATIC style.
            final_sql = statement.to_sql(placeholder_style=ParameterStyle.STATIC)
            # No separate args for conn.execute(script_sql_string)
        elif is_many:
            if parameters is not None and isinstance(parameters, Sequence):
                for param_set in parameters:
                    # Each param_set should be a sequence (list/tuple) for one row
                    if isinstance(param_set, (list, tuple)):
                        args_for_driver_many.append(param_set)
                    elif param_set is None:  # Should not happen for a row's params
                        args_for_driver_many.append(())
                    else:  # Single param for a row
                        args_for_driver_many.append((param_set,))
        else:  # Single execute
            # Get parameters as a flat list/tuple for *args unpacking
            # The `parameters` arg to _execute_impl is the SQLParameterType for the single call
            ordered_params = statement.get_parameters(style=self._get_placeholder_style())
            if isinstance(ordered_params, (list, tuple)):
                args_for_driver.extend(ordered_params)
            elif ordered_params is not None:
                args_for_driver.append(ordered_params)
            # If ordered_params is None, args_for_driver remains empty []

        if is_script:
            # For scripts, conn.execute() is used and it returns the status string of the last command.
            status_str: str = await conn.execute(final_sql)
            return status_str

        if is_many:
            # executemany doesn't return records directly; status indicates completion.
            # It's primarily for batch DML. asyncpg doesn't have a rowcount for executemany.
            await conn.executemany(final_sql, args_for_driver_many)
            # We can return the number of parameter sets processed as an indicator of operations.
            return len(args_for_driver_many)  # Or a status string if asyncpg provided one.

        # Single execute: can be SELECT (fetch) or DML (execute)
        # The protocol execute method will decide based on statement.returns_rows
        # whether to call _wrap_select_result or _wrap_execute_result.
        # This _execute_impl should provide the raw data needed by those wrappers.
        if CommonDriverAttributes.returns_rows(statement.expression):  # Use the helper from base
            records: list[Record] = await conn.fetch(final_sql, *args_for_driver)
            return records  # List of asyncpg.Record objects
        status_str: str = await conn.execute(final_sql, *args_for_driver)
        return status_str  # Status string like 'INSERT 0 1'

    @instrument_async(operation_type="database")
    async def _wrap_select_result(
        self,
        statement: SQL,
        raw_driver_result: Any,  # This will be list[Record]
        schema_type: Optional[type[ModelDTOT]] = None,
        **kwargs: Any,
    ) -> Union[SelectResult[ModelDTOT], SelectResult[DictRow]]:
        records = cast("list[Record]", raw_driver_result)

        if not records:
            return SelectResult(rows=[], column_names=[], raw_result=records)

        # Assuming Record objects are dict-like (they are mapping-like)
        column_names = list(records[0].keys())
        rows_as_dicts: list[dict[str, Any]] = [dict(record) for record in records]

        if schema_type:
            converted_rows = self.to_schema(rows_as_dicts, schema_type=schema_type)
            return SelectResult(
                rows=converted_rows,  # type: ignore[arg-type]
                column_names=column_names,
                raw_result=records,
            )

        return SelectResult(
            rows=rows_as_dicts,
            column_names=column_names,
            raw_result=records,
        )

    @instrument_async(operation_type="database")
    async def _wrap_execute_result(
        self,
        statement: SQL,
        raw_driver_result: Any,  # For DML, this is status string or row count for executemany
        **kwargs: Any,
    ) -> ExecuteResult[Any]:
        rows_affected = 0

        if isinstance(raw_driver_result, str):  # Status string from conn.execute()
            match = ROWCOUNT_REGEX.match(raw_driver_result)
            if match:
                rows_affected = int(match.group(1))
        elif isinstance(raw_driver_result, int):  # Count from executemany
            rows_affected = raw_driver_result

        operation_type = "UNKNOWN"
        if statement.expression and hasattr(statement.expression, "key"):
            operation_type = str(statement.expression.key).upper()

        return ExecuteResult(
            raw_result=raw_driver_result,  # Store the original status string or count
            rows_affected=rows_affected,
            operation_type=operation_type,
            # last_inserted_id is not directly available in asyncpg status string
            # For RETURNING clauses, data would come via _wrap_select_result
        )

    def _connection(self, connection: "Optional[AsyncpgConnection]" = None) -> "AsyncpgConnection":
        """Return the connection to use. If None, use the default connection."""
        return connection if connection is not None else self.connection
