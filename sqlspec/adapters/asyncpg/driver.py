"""AsyncPG PostgreSQL driver implementation for async PostgreSQL operations."""

from collections import OrderedDict
from io import BytesIO
from typing import TYPE_CHECKING, Any, cast

import asyncpg

from sqlspec.adapters.asyncpg.core import (
    PREPARED_STATEMENT_CACHE_SIZE,
    NormalizedStackOperation,
    collect_rows,
    create_mapped_exception,
    default_statement_config,
    driver_profile,
    invoke_prepared_statement,
    parse_status,
)
from sqlspec.adapters.asyncpg.data_dictionary import AsyncpgDataDictionary
from sqlspec.core import (
    SQL,
    StackResult,
    StatementStack,
    create_sql_result,
    get_cache_config,
    is_copy_from_operation,
    is_copy_operation,
    register_driver_profile,
)
from sqlspec.driver import AsyncDriverAdapterBase, StackExecutionObserver, describe_stack_statement
from sqlspec.exceptions import SQLSpecError, StackExecutionError
from sqlspec.utils.logging import get_logger
from sqlspec.utils.type_guards import has_sqlstate

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlspec.adapters.asyncpg._typing import AsyncpgConnection, AsyncpgPreparedStatement
    from sqlspec.core import ArrowResult, SQLResult, StatementConfig
    from sqlspec.driver import ExecutionResult
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry

from sqlspec.adapters.asyncpg._typing import AsyncpgSessionContext

__all__ = ("AsyncpgCursor", "AsyncpgDriver", "AsyncpgExceptionHandler", "AsyncpgSessionContext")

logger = get_logger("sqlspec.adapters.asyncpg")


class AsyncpgCursor:
    """Context manager for AsyncPG cursor management."""

    __slots__ = ("connection",)

    def __init__(self, connection: "AsyncpgConnection") -> None:
        self.connection = connection

    async def __aenter__(self) -> "AsyncpgConnection":
        return self.connection

    async def __aexit__(self, *_: Any) -> None: ...


class AsyncpgExceptionHandler:
    """Async context manager for handling AsyncPG database exceptions.

    Maps PostgreSQL SQLSTATE error codes to specific SQLSpec exceptions
    for better error handling in application code.

    Uses deferred exception pattern for mypyc compatibility: exceptions
    are stored in pending_exception rather than raised from __aexit__
    to avoid ABI boundary violations with compiled code.
    """

    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "AsyncpgExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_val is None:
            return False
        if isinstance(exc_val, asyncpg.PostgresError) or has_sqlstate(exc_val):
            self.pending_exception = create_mapped_exception(exc_val)
            return True
        return False


class AsyncpgDriver(AsyncDriverAdapterBase):
    """AsyncPG PostgreSQL driver for async database operations.

    Supports COPY operations, numeric parameter style handling, PostgreSQL
    exception handling, transaction management, SQL statement compilation
    and caching, and parameter processing with type coercion.
    """

    __slots__ = ("_data_dictionary", "_prepared_statements")
    dialect = "postgres"

    def __init__(
        self,
        connection: "AsyncpgConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        if statement_config is None:
            statement_config = default_statement_config.replace(
                enable_caching=get_cache_config().compiled_cache_enabled
            )

        super().__init__(connection=connection, statement_config=statement_config, driver_features=driver_features)
        self._data_dictionary: AsyncpgDataDictionary | None = None
        self._prepared_statements: OrderedDict[str, AsyncpgPreparedStatement] = OrderedDict()

    # ─────────────────────────────────────────────────────────────────────────────
    # CORE DISPATCH METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def dispatch_execute(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute single SQL statement.

        Handles both SELECT queries and non-SELECT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to execute

        Returns:
            ExecutionResult with statement execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        params: tuple[Any, ...] = cast("tuple[Any, ...]", prepared_parameters) if prepared_parameters else ()

        if statement.returns_rows():
            records = await cursor.fetch(sql, *params) if params else await cursor.fetch(sql)
            data, column_names = collect_rows(records)

            return self.create_execution_result(
                cursor, selected_data=data, column_names=column_names, data_row_count=len(data), is_select_result=True
            )

        result = await cursor.execute(sql, *params) if params else await cursor.execute(sql)

        affected_rows = parse_status(result)

        return self.create_execution_result(cursor, rowcount_override=affected_rows)

    async def dispatch_execute_many(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL with multiple parameter sets using AsyncPG's executemany.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with multiple parameter sets

        Returns:
            ExecutionResult with batch execution details
        """
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)

        if prepared_parameters:
            parameter_sets = cast("list[Sequence[object]]", prepared_parameters)
            await cursor.executemany(sql, parameter_sets)

            affected_rows = len(parameter_sets)
        else:
            affected_rows = 0

        return self.create_execution_result(cursor, rowcount_override=affected_rows, is_many_result=True)

    async def dispatch_execute_script(self, cursor: "AsyncpgConnection", statement: "SQL") -> "ExecutionResult":
        """Execute SQL script with statement splitting and parameter handling.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement containing multiple statements

        Returns:
            ExecutionResult with script execution details
        """
        sql, _ = self._get_compiled_sql(statement, self.statement_config)
        statements = self.split_script_statements(sql, statement.statement_config, strip_trailing_semicolon=True)

        successful_count = 0
        last_result = None

        for stmt in statements:
            result = await cursor.execute(stmt)
            last_result = result
            successful_count += 1

        return self.create_execution_result(
            last_result, statement_count=len(statements), successful_statements=successful_count, is_script_result=True
        )

    async def dispatch_special_handling(self, cursor: "AsyncpgConnection", statement: "SQL") -> "SQLResult | None":
        """Handle PostgreSQL COPY operations and other special cases.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement to analyze

        Returns:
            SQLResult if special operation was handled, None for standard execution
        """
        if is_copy_operation(statement.operation_type):
            await self._handle_copy_operation(cursor, statement)
            return self.build_statement_result(statement, self.create_execution_result(cursor))

        return None

    # ─────────────────────────────────────────────────────────────────────────────
    # TRANSACTION MANAGEMENT
    # ─────────────────────────────────────────────────────────────────────────────

    async def begin(self) -> None:
        """Begin a database transaction."""
        try:
            await self.connection.execute("BEGIN")
        except asyncpg.PostgresError as e:
            msg = f"Failed to begin async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def commit(self) -> None:
        """Commit the current transaction."""
        try:
            await self.connection.execute("COMMIT")
        except asyncpg.PostgresError as e:
            msg = f"Failed to commit async transaction: {e}"
            raise SQLSpecError(msg) from e

    async def rollback(self) -> None:
        """Rollback the current transaction."""
        try:
            await self.connection.execute("ROLLBACK")
        except asyncpg.PostgresError as e:
            msg = f"Failed to rollback async transaction: {e}"
            raise SQLSpecError(msg) from e

    def with_cursor(self, connection: "AsyncpgConnection") -> "AsyncpgCursor":
        """Create context manager for AsyncPG cursor."""
        return AsyncpgCursor(connection)

    def handle_database_exceptions(self) -> "AsyncpgExceptionHandler":
        """Handle database exceptions with PostgreSQL error codes."""
        return AsyncpgExceptionHandler()

    # ─────────────────────────────────────────────────────────────────────────────
    # STACK EXECUTION METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def execute_stack(
        self, stack: "StatementStack", *, continue_on_error: bool = False
    ) -> "tuple[StackResult, ...]":
        """Execute a StatementStack using asyncpg's rapid batching."""

        if not isinstance(stack, StatementStack) or not stack or self.stack_native_disabled:
            return await super().execute_stack(stack, continue_on_error=continue_on_error)

        return await self._execute_stack_native(stack, continue_on_error=continue_on_error)

    async def _execute_stack_native(
        self, stack: "StatementStack", *, continue_on_error: bool
    ) -> "tuple[StackResult, ...]":
        results: list[StackResult] = []

        transaction_cm = None
        if not continue_on_error and not self._connection_in_transaction():
            transaction_cm = self.connection.transaction()

        with StackExecutionObserver(self, stack, continue_on_error, native_pipeline=True) as observer:
            if transaction_cm is not None:
                async with transaction_cm:
                    await self._run_stack_operations(stack, continue_on_error, observer, results)
            else:
                await self._run_stack_operations(stack, continue_on_error, observer, results)

        return tuple(results)

    async def _run_stack_operations(
        self,
        stack: "StatementStack",
        continue_on_error: bool,
        observer: "StackExecutionObserver",
        results: "list[StackResult]",
    ) -> None:
        """Run operations for statement stack execution.

        Extracted from _execute_stack_native to avoid closure compilation issues.
        """
        for index, operation in enumerate(stack.operations):
            try:
                normalized: NormalizedStackOperation | None = None
                if operation.method == "execute":
                    kwargs = dict(operation.keyword_arguments) if operation.keyword_arguments else {}
                    statement_config = kwargs.pop("statement_config", None)
                    config = statement_config or self.statement_config

                    sql_statement = self.prepare_statement(
                        operation.statement, operation.arguments, statement_config=config, kwargs=kwargs
                    )
                    if not sql_statement.is_script and not sql_statement.is_many:
                        sql_text, prepared_parameters = self._get_compiled_sql(sql_statement, config)
                        prepared_parameters = cast("tuple[Any, ...] | dict[str, Any] | None", prepared_parameters)
                        normalized = NormalizedStackOperation(
                            operation=operation, statement=sql_statement, sql=sql_text, parameters=prepared_parameters
                        )

                if normalized is not None:
                    stack_result = await self._execute_stack_operation_prepared(normalized)
                else:
                    result = await self._execute_stack_operation(operation)
                    stack_result = StackResult(result=result)
            except Exception as exc:
                stack_error = StackExecutionError(
                    index,
                    describe_stack_statement(operation.statement),
                    exc,
                    adapter=type(self).__name__,
                    mode="continue-on-error" if continue_on_error else "fail-fast",
                )
                if continue_on_error:
                    observer.record_operation_error(stack_error)
                    results.append(StackResult.from_error(stack_error))
                    continue
                raise stack_error from exc

            results.append(stack_result)

    async def _execute_stack_operation_prepared(self, normalized: "NormalizedStackOperation") -> StackResult:
        prepared = await self._get_prepared_statement(normalized.sql)
        metadata = {"prepared_statement": True}

        if normalized.statement.returns_rows():
            rows = await invoke_prepared_statement(prepared, normalized.parameters, fetch=True)
            data, _ = collect_rows(rows)
            sql_result = create_sql_result(normalized.statement, data=data, rows_affected=len(data), metadata=metadata)
            return StackResult.from_sql_result(sql_result)

        status = await invoke_prepared_statement(prepared, normalized.parameters, fetch=False)
        rowcount = parse_status(status)
        sql_result = create_sql_result(normalized.statement, rows_affected=rowcount, metadata=metadata)
        return StackResult.from_sql_result(sql_result)

    # ─────────────────────────────────────────────────────────────────────────────
    # STORAGE API METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    async def select_to_storage(
        self,
        statement: "SQL | str",
        destination: "StorageDestination",
        /,
        *parameters: Any,
        statement_config: "StatementConfig | None" = None,
        partitioner: "dict[str, object] | None" = None,
        format_hint: "StorageFormat | None" = None,
        telemetry: "StorageTelemetry | None" = None,
        **kwargs: Any,
    ) -> "StorageBridgeJob":
        """Execute a query and persist results to storage once native COPY is available."""

        self._require_capability("arrow_export_enabled")
        arrow_result = await self.select_to_arrow(statement, *parameters, statement_config=statement_config, **kwargs)
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_result_to_storage_async(
            arrow_result, destination, format_hint=format_hint, pipeline=async_pipeline
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_arrow(
        self,
        table: str,
        source: "ArrowResult | Any",
        *,
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
        telemetry: "StorageTelemetry | None" = None,
    ) -> "StorageBridgeJob":
        """Load Arrow data into a PostgreSQL table via COPY."""

        self._require_capability("arrow_import_enabled")
        arrow_table = self._coerce_arrow_table(source)
        if overwrite:
            try:
                await self.connection.execute(f"TRUNCATE TABLE {table}")
            except asyncpg.PostgresError as exc:
                msg = f"Failed to truncate table '{table}': {exc}"
                raise SQLSpecError(msg) from exc
        columns, records = self._arrow_table_to_rows(arrow_table)
        if records:
            await self.connection.copy_records_to_table(table, records=records, columns=columns)
        telemetry_payload = self._build_ingest_telemetry(arrow_table)
        telemetry_payload["destination"] = table
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._create_storage_job(telemetry_payload, telemetry)

    async def load_from_storage(
        self,
        table: str,
        source: "StorageDestination",
        *,
        file_format: "StorageFormat",
        partitioner: "dict[str, object] | None" = None,
        overwrite: bool = False,
    ) -> "StorageBridgeJob":
        """Read an artifact from storage and ingest it via COPY."""

        arrow_table, inbound = await self._read_arrow_from_storage_async(source, file_format=file_format)
        return await self.load_from_arrow(
            table, arrow_table, partitioner=partitioner, overwrite=overwrite, telemetry=inbound
        )

    # ─────────────────────────────────────────────────────────────────────────────
    # UTILITY METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    @property
    def data_dictionary(self) -> "AsyncpgDataDictionary":
        """Get the data dictionary for this driver.

        Returns:
            Data dictionary instance for metadata queries
        """
        if self._data_dictionary is None:
            self._data_dictionary = AsyncpgDataDictionary()
        return self._data_dictionary

    # ─────────────────────────────────────────────────────────────────────────────
    # PRIVATE/INTERNAL METHODS
    # ─────────────────────────────────────────────────────────────────────────────

    def _connection_in_transaction(self) -> bool:
        """Check if connection is in transaction."""
        return bool(self.connection.is_in_transaction())

    async def _get_prepared_statement(self, sql: str) -> "AsyncpgPreparedStatement":
        cached = self._prepared_statements.get(sql)
        if cached is not None:
            self._prepared_statements.move_to_end(sql)
            return cached

        prepared = cast("AsyncpgPreparedStatement", await self.connection.prepare(sql))
        self._prepared_statements[sql] = prepared
        if len(self._prepared_statements) > PREPARED_STATEMENT_CACHE_SIZE:
            self._prepared_statements.popitem(last=False)
        return prepared

    async def _handle_copy_operation(self, cursor: "AsyncpgConnection", statement: "SQL") -> None:
        """Handle PostgreSQL COPY operations.

        Supports both COPY FROM STDIN and COPY TO STDOUT operations.

        Args:
            cursor: AsyncPG connection object
            statement: SQL statement with COPY operation
        """

        execution_args = statement.statement_config.execution_args
        metadata: dict[str, Any] = dict(execution_args) if execution_args else {}
        sql_text, _ = self._get_compiled_sql(statement, statement.statement_config)
        sql_upper = sql_text.upper()
        copy_data = metadata.get("postgres_copy_data")

        if copy_data and is_copy_from_operation(statement.operation_type) and "FROM STDIN" in sql_upper:
            if isinstance(copy_data, dict):
                data_str = (
                    str(next(iter(copy_data.values())))
                    if len(copy_data) == 1
                    else "\n".join(str(value) for value in copy_data.values())
                )
            elif isinstance(copy_data, (list, tuple)):
                data_str = str(copy_data[0]) if len(copy_data) == 1 else "\n".join(str(value) for value in copy_data)
            else:
                data_str = str(copy_data)

            data_io = BytesIO(data_str.encode("utf-8"))
            await cursor.copy_from_query(sql_text, output=data_io)
            return

        await cursor.execute(sql_text)


register_driver_profile("asyncpg", driver_profile)
